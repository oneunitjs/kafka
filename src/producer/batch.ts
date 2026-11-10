/**
 * Buffered batching producer (§8.5).
 *
 * Accumulates publish requests in memory and flushes them to Kafka based on
 * configurable size, count, or time triggers. This is an opt-in feature;
 * the default `publish()` path sends immediately.
 *
 * Documented behavior:
 *   - Maximum buffered records and bytes.
 *   - Flush interval.
 *   - Behavior when the buffer is full (reject with backpressure error).
 *   - Whether calls wait, reject, or apply backpressure (reject).
 *   - What happens during close (flush remaining, then disconnect).
 *   - How failures are reported to callers (reject the send promise).
 */

import { createLogger, type Logger } from "../transport/kafkajs/logger.js";
import { KafkaConnectionError } from "../errors/index.js";
import { resolveCodec, type Codec } from "../adapters/codec.js";
import { encodeValue } from "../utils/encoding.js";
import { Kafka, type Producer } from "kafkajs";

export interface BufferedBatchProducerOptions {
    /** KafkaJS instance used to construct the underlying producer. */
    kafka: Kafka;
    /** Underlying KafkaJS producer options (e.g. idempotent, transactionalId). */
    producerOptions?: Record<string, unknown>;
    /** Logger for diagnostics. */
    logger?: Logger;
    /** Codec for encoding values/keys. Defaults to JSON. */
    codec?: Codec | string;

    // Batch triggers (§8.5)
    /** Max records buffered per topic before flushing that topic. */
    maxRecordsPerTopic?: number;
    /** Max total bytes buffered across all topics before flushing everything. */
    maxBufferedBytes?: number;
    /** Max total records buffered across all topics before flushing everything. */
    maxBufferedRecords?: number;
    /** Maximum time (ms) a record may sit buffered before being flushed. */
    flushIntervalMs?: number;

    // Backpressure (§8.5)
    /** Reject `send()` when buffered records exceed this limit. */
    maxQueuedRecords?: number;
    /** Reject `send()` when buffered bytes exceed this limit. */
    maxQueuedBytes?: number;
}

interface BufferedRecord {
    topic: string;
    value: string | Buffer | null | undefined;
    key?: string | Buffer | null | undefined;
    headers?: Record<string, string | Buffer | undefined>;
    send?: Record<string, unknown>;
    resolve: (metadata: unknown[]) => void;
    reject: (error: unknown) => void;
    enqueuedAt: number;
}

export class BufferedBatchProducer {
    private readonly kafka: Kafka;
    private readonly logger: Logger;
    private readonly codec: Codec;
    private readonly producerOptions: Record<string, unknown>;
    private readonly maxRecordsPerTopic: number;
    private readonly maxBufferedBytes: number;
    private readonly maxBufferedRecords: number;
    private readonly flushIntervalMs: number;
    private readonly maxQueuedRecords: number;
    private readonly maxQueuedBytes: number;

    private producer: Producer | null = null;
    private connected = false;
    private closed = false;

    // Per-topic buffers.
    private readonly buffers = new Map<string, BufferedRecord[]>();
    private bufferedBytes = 0;
    private bufferedRecordCount = 0;

    private flushTimer: ReturnType<typeof setTimeout> | null = null;

    constructor(options: BufferedBatchProducerOptions) {
        this.kafka = options.kafka;
        this.logger = createLogger(options.logger);
        this.codec = resolveCodec(options.codec);
        this.producerOptions = options.producerOptions ?? {};
        this.maxRecordsPerTopic = options.maxRecordsPerTopic ?? 1000;
        this.maxBufferedBytes = options.maxBufferedBytes ?? 1_000_000; // 1 MB
        this.maxBufferedRecords = options.maxBufferedRecords ?? 10_000;
        this.flushIntervalMs = options.flushIntervalMs ?? 5_000;
        this.maxQueuedRecords = options.maxQueuedRecords ?? 50_000;
        this.maxQueuedBytes = options.maxQueuedBytes ?? 50_000_000; // 50 MB
    }

    /** Number of records currently buffered across all topics. */
    get size(): number {
        return this.bufferedRecordCount;
    }

    /** Total bytes currently buffered across all topics. */
    get bytesBuffered(): number {
        return this.bufferedBytes;
    }

    /** Whether the buffer is at or above backpressure limits. */
    get isFull(): boolean {
        return (
            this.bufferedRecordCount >= this.maxQueuedRecords ||
            this.bufferedBytes >= this.maxQueuedBytes
        );
    }

    /** Whether the underlying producer is connected. */
    get isConnected(): boolean {
        return this.connected;
    }

    private async ensureConnected(allowDuringClose = false): Promise<Producer> {
        if (this.closed && !allowDuringClose) {
            throw new KafkaConnectionError("Buffered batch producer is closed", {
                code: "KAFKA_CONNECTION_ERROR",
                retryable: false,
            });
        }
        if (!this.producer) {
            this.producer = this.kafka.producer({
                allowAutoTopicCreation: false,
                ...this.producerOptions,
            });
            await this.producer.connect();
            this.connected = true;
            this.logger.info("Buffered batch producer connected");
        }
        return this.producer;
    }

    /**
     * Buffer a record for batch sending. Returns a promise that resolves with
     * the send metadata once the record is flushed, or rejects if the buffer
     * is full (§8.5 backpressure) or the send fails.
     */
    async send(
        topic: string,
        value: unknown,
        options: {
            key?: unknown;
            headers?: Record<string, string | Buffer | undefined>;
            send?: Record<string, unknown>;
        } = {},
    ): Promise<unknown[]> {
        if (this.closed) {
            throw new KafkaConnectionError("Buffered batch producer is closed", {
                code: "KAFKA_CONNECTION_ERROR",
                retryable: false,
            });
        }

        // Backpressure: reject when the buffer is full (§8.5).
        if (this.isFull) {
            throw new KafkaConnectionError(
                "Producer buffer is full; apply backpressure",
                { code: "KAFKA_CONNECTION_ERROR", topic, retryable: true },
            );
        }

        const encodedValue = this.encode(value);
        const encodedKey =
            options.key === undefined || options.key === null
                ? undefined
                : this.encode(options.key);

        const recordSize = this.recordSizeValue(encodedValue, encodedKey);

        const promise = new Promise<unknown[]>((resolve, reject) => {
            const record: BufferedRecord = {
                topic,
                value: encodedValue,
                key: encodedKey,
                headers: options.headers,
                send: options.send,
                resolve,
                reject,
                enqueuedAt: Date.now(),
            };

            let topicBuffer = this.buffers.get(topic);
            if (!topicBuffer) {
                topicBuffer = [];
                this.buffers.set(topic, topicBuffer);
            }
            topicBuffer.push(record);
            this.bufferedRecordCount += 1;
            this.bufferedBytes += recordSize;

            // Check count trigger per topic.
            if (topicBuffer.length >= this.maxRecordsPerTopic) {
                this.flushTopic(topic).catch((error) => {
                    this.logger.error(`Failed to flush topic "${topic}"`, error);
                });
            } else if (
                this.bufferedRecordCount >= this.maxBufferedRecords ||
                this.bufferedBytes >= this.maxBufferedBytes
            ) {
                this.flushAll().catch((error) => {
                    this.logger.error("Failed to flush buffered batch", error);
                });
            } else {
                this.scheduleFlush();
            }
        });

        return promise;
    }

    /**
     * Force a flush of all buffered records immediately (§8.5).
     */
    async flush(): Promise<void> {
        if (this.closed) {
            return;
        }
        await this.flushAll();
    }

    /**
     * Flush all buffered records and disconnect the underlying producer.
     * Any in-flight sends are resolved or rejected based on the flush outcome.
     */
    async close(): Promise<void> {
        if (this.closed) {
            return;
        }
        this.closed = true;
        this.cancelFlushTimer();

        const errors: unknown[] = [];
        try {
            await this.flushAll();
        } catch (error) {
            errors.push(error);
        }

        if (this.producer) {
            try {
                await this.producer.disconnect();
            } catch (error) {
                errors.push(error);
            }
            this.producer = null;
        }
        this.connected = false;

        // Reject any records still buffered after close.
        for (const [topic, buffer] of this.buffers) {
            for (const record of buffer) {
                record.reject(
                    new KafkaConnectionError(
                        "Producer closed before buffered record was flushed",
                        { code: "KAFKA_CONNECTION_ERROR", topic, retryable: false },
                    ),
                );
            }
        }
        this.buffers.clear();
        this.bufferedRecordCount = 0;
        this.bufferedBytes = 0;

        if (errors.length > 0) {
            throw errors[0];
        }
    }

    private encode(value: unknown): Buffer | string | null | undefined {
        const encoded = encodeValue(value, this.codec);
        return encoded === undefined ? null : encoded;
    }

    private scheduleFlush(): void {
        this.cancelFlushTimer();
        this.flushTimer = setTimeout(() => {
            this.flushTimer = null;
            this.flushAll().catch((error) => {
                this.logger.error("Scheduled flush failed", error);
            });
        }, this.flushIntervalMs);
        // Allow the process to exit even if the timer is pending.
        if (this.flushTimer.unref) {
            this.flushTimer.unref();
        }
    }

    private cancelFlushTimer(): void {
        if (this.flushTimer) {
            clearTimeout(this.flushTimer);
            this.flushTimer = null;
        }
    }

    private async flushTopic(topic: string): Promise<void> {
        const buffer = this.buffers.get(topic);
        if (!buffer || buffer.length === 0) {
            return;
        }
        this.buffers.set(topic, []);
        this.bufferedRecordCount -= buffer.length;
        this.bufferedBytes -= buffer.reduce(
            (sum, r) => sum + this.recordSize(r),
            0,
        );

        const producer = await this.ensureConnected(true);
        const messages = buffer.map((record) => ({
            key: record.key as Buffer | string | null | undefined,
            value: (record.value as Buffer | string | null) ?? null,
            headers: record.headers,
        }));

        try {
            const metadata = await producer.send({
                topic,
                messages,
                ...(buffer[0]?.send ?? {}),
            });
            for (const record of buffer) {
                record.resolve(metadata);
            }
        } catch (error) {
            for (const record of buffer) {
                record.reject(error);
            }
            throw error;
        }
    }

    private async flushAll(): Promise<void> {
        const topics = [...this.buffers.keys()];
        if (topics.length === 0) {
            return;
        }
        await Promise.all(topics.map((topic) => this.flushTopic(topic)));
    }

    private recordSize(record: BufferedRecord): number {
        return this.recordSizeValue(record.value, record.key);
    }

    private recordSizeValue(
        value: Buffer | string | null | undefined,
        key: Buffer | string | null | undefined,
    ): number {
        return (
            Buffer.byteLength(JSON.stringify(value)) +
            (key ? Buffer.byteLength(JSON.stringify(key)) : 0) +
            256
        );
    }
}
