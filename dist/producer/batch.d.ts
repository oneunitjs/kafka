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
import { type Logger } from "../transport/kafkajs/logger.js";
import { type Codec } from "../adapters/codec.js";
import { Kafka } from "kafkajs";
export interface BufferedBatchProducerOptions {
    /** KafkaJS instance used to construct the underlying producer. */
    kafka: Kafka;
    /** Underlying KafkaJS producer options (e.g. idempotent, transactionalId). */
    producerOptions?: Record<string, unknown>;
    /** Logger for diagnostics. */
    logger?: Logger;
    /** Codec for encoding values/keys. Defaults to JSON. */
    codec?: Codec | string;
    /** Max records buffered per topic before flushing that topic. */
    maxRecordsPerTopic?: number;
    /** Max total bytes buffered across all topics before flushing everything. */
    maxBufferedBytes?: number;
    /** Max total records buffered across all topics before flushing everything. */
    maxBufferedRecords?: number;
    /** Maximum time (ms) a record may sit buffered before being flushed. */
    flushIntervalMs?: number;
    /** Reject `send()` when buffered records exceed this limit. */
    maxQueuedRecords?: number;
    /** Reject `send()` when buffered bytes exceed this limit. */
    maxQueuedBytes?: number;
}
export declare class BufferedBatchProducer {
    private readonly kafka;
    private readonly logger;
    private readonly codec;
    private readonly producerOptions;
    private readonly maxRecordsPerTopic;
    private readonly maxBufferedBytes;
    private readonly maxBufferedRecords;
    private readonly flushIntervalMs;
    private readonly maxQueuedRecords;
    private readonly maxQueuedBytes;
    private producer;
    private connected;
    private closed;
    private readonly buffers;
    private bufferedBytes;
    private bufferedRecordCount;
    private flushTimer;
    constructor(options: BufferedBatchProducerOptions);
    /** Number of records currently buffered across all topics. */
    get size(): number;
    /** Total bytes currently buffered across all topics. */
    get bytesBuffered(): number;
    /** Whether the buffer is at or above backpressure limits. */
    get isFull(): boolean;
    /** Whether the underlying producer is connected. */
    get isConnected(): boolean;
    private ensureConnected;
    /**
     * Buffer a record for batch sending. Returns a promise that resolves with
     * the send metadata once the record is flushed, or rejects if the buffer
     * is full (§8.5 backpressure) or the send fails.
     */
    send(topic: string, value: unknown, options?: {
        key?: unknown;
        headers?: Record<string, string | Buffer | undefined>;
        send?: Record<string, unknown>;
    }): Promise<unknown[]>;
    /**
     * Force a flush of all buffered records immediately (§8.5).
     */
    flush(): Promise<void>;
    /**
     * Flush all buffered records and disconnect the underlying producer.
     * Any in-flight sends are resolved or rejected based on the flush outcome.
     */
    close(): Promise<void>;
    private encode;
    private scheduleFlush;
    private cancelFlushTimer;
    private flushTopic;
    private flushAll;
    private recordSize;
    private recordSizeValue;
}
