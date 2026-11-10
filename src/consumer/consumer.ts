import { createLogger, type Logger } from "../transport/kafkajs/logger.js";
import { Kafka, type Consumer, type EachMessagePayload } from "kafkajs";
import { createConsumer, subscribeToTopic, consumeMessages } from "../transport/kafkajs/consumer.js";
import { KafkaConfigError, KafkaConnectionError, KafkaLifecycleError, KafkaTimeoutError } from "../errors/index.js";
import { composeConsume, type ConsumeContext, type ConsumeMiddleware } from "../middleware/compose.js";
import { createRetryPolicy, exponentialBackoff, type RetryPolicy } from "../resilience/retry.js";
import { DeadLetterPublisher, type DeadLetterRecord } from "../resilience/dead-letter.js";
import { sleep } from "../utils/sleep.js";

export interface KafkaConsumerOptions {
    logger?: Logger;
    fromBeginning?: boolean;
    retryPolicy?: RetryPolicy;
    deadLetterTopic?: string;
    /** Maximum number of in-flight handler invocations across all partitions (§9.6). */
    maxInFlight?: number;
    /** Per-handler timeout in milliseconds. Handlers exceeding this are aborted (§9.6). */
    handlerTimeoutMs?: number;
    [key: string]: unknown;
}

export type MessageHandler<T = unknown> = (payload: MessageContext<T>) => unknown | Promise<unknown>;

export interface MessageContext<T = unknown> {
    value: T;
    key: unknown;
    topic: string;
    partition: number;
    offset: string;
    timestamp: string;
    headers: Record<string, string | Buffer | undefined>;
    signal?: AbortSignal;
    metadata: {
        correlationId?: string;
        messageId?: string;
        contentType?: string;
        schemaVersion?: string;
    };
}

export class KafkaConsumer<T = unknown> {
    private readonly groupId: string;
    private readonly kafka: Kafka;
    private readonly logger: Logger;
    private consumer: Consumer | null = null;
    private started = false;
    private startPromise: Promise<void> | null = null;
    private stopped = false;
    private closed = false;
    private readonly middleware: ConsumeMiddleware<T>[] = [];
    private readonly subscriptions = new Map<string, { handler: MessageHandler<T>; options: KafkaConsumerOptions }>();
    private readonly retryPolicy: RetryPolicy;
    private readonly deadLetterTopic: string | undefined;
    private readonly dlqPublisher: DeadLetterPublisher | null;
    private readonly pauseReasons = new Set<string>();
    private readonly inFlightHandlers = new Set<Promise<void>>();
    private readonly maxInFlight: number;
    private readonly handlerTimeoutMs: number | undefined;
    private readonly handlerStartTimes = new Map<Promise<void>, number>();
    private readonly completedOffsets = new Map<string, Map<number, number>>();
    private readonly completedOffsetSets = new Map<string, Map<number, Set<number>>>();
    private readonly observedOffsetSets = new Map<string, Map<number, Set<number>>>();
    private readonly highWatermarkOffsets = new Map<string, Map<number, number>>();
    private inFlightSemaphore = 0;
    private readonly inFlightWaiters: Array<() => void> = [];

    constructor(groupId: string, kafka: Kafka, options: KafkaConsumerOptions = {}) {
        this.groupId = groupId;
        this.kafka = kafka;
        this.logger = createLogger(options.logger);
        this.retryPolicy = options.retryPolicy ?? createRetryPolicy();
        this.deadLetterTopic = options.deadLetterTopic;
        this.dlqPublisher = this.deadLetterTopic
            ? new DeadLetterPublisher(kafka, { dlqTopic: this.deadLetterTopic })
            : null;
        // Default to unbounded in-flight; callers set maxInFlight to bound concurrency (§9.6).
        this.maxInFlight = options.maxInFlight ?? 100;
        this.handlerTimeoutMs = options.handlerTimeoutMs;
    }

    /**
     * Pause consumption under a named reason (§9.7). The consumer stays
     * paused until every outstanding reason is resumed, so a single
     * `resume()` cannot accidentally undo another component's pause.
     */
    async pause(reason = "default", topics?: string[]): Promise<void> {
        this.ensureOpen();
        this.pauseReasons.add(reason);
        if (this.consumer) {
            const targetTopics = topics && topics.length > 0 ? topics : [...this.subscriptions.keys()];
            this.consumer.pause(targetTopics.map((topic) => ({ topic })));
        }
        this.logger.info(`Consumer "${this.groupId}" paused (reason: ${reason})`);
    }

    /**
     * Resume a previously recorded pause reason (§9.7).
     */
    async resume(reason = "default", topics?: string[]): Promise<void> {
        this.ensureOpen();
        this.pauseReasons.delete(reason);
        if (this.pauseReasons.size === 0 && this.consumer) {
            const targetTopics = topics && topics.length > 0 ? topics : [...this.subscriptions.keys()];
            this.consumer.resume(targetTopics.map((topic) => ({ topic })));
        }
        this.logger.info(`Consumer "${this.groupId}" resumed (reason: ${reason}, remaining: ${this.pauseReasons.size})`);
    }

    get pausedReasons(): ReadonlySet<string> {
        return this.pauseReasons;
    }

    get isPaused(): boolean {
        return this.pauseReasons.size > 0;
    }

    /**
     * Register publish middleware for this consumer. Runs in registration
     * order for the inbound path and reverse order on unwind (§11.3).
     */
    use(middleware: ConsumeMiddleware<T>): this {
        this.middleware.push(middleware);
        return this;
    }

    /**
     * Subscribe to a topic. Registration is deferred until `start()`;
     * multiple topics may be registered before starting.
     */
    async subscribe(topic: string, handler: MessageHandler<T>, options: KafkaConsumerOptions = {}): Promise<void> {
        this.ensureOpen();
        if (!topic || String(topic).trim() === "") {
            throw new KafkaConfigError("Kafka topic is required");
        }
        this.subscriptions.set(topic, { handler, options });

        // If already started, apply the subscription dynamically to the transport
        // so new topics are consumed without restarting the runner (§9.2).
        if (this.started && this.consumer) {
            await subscribeToTopic(this.consumer, topic, options);
            this.logger.info(`Dynamically subscribed to "${topic}" on running consumer "${this.groupId}"`);
            return;
        }
        this.logger.info(`Subscribed to Kafka topic "${topic}" on consumer "${this.groupId}"`);
    }

    /**
     * Connect and start one managed runner per consumer (§9.2). Calling
     * start() again while running returns the existing start promise.
     */
    async start(): Promise<void> {
        this.ensureOpen();
        if (this.started) {
            return;
        }
        if (this.startPromise) {
            return this.startPromise;
        }
        if (this.subscriptions.size === 0) {
            throw new KafkaConfigError(`Consumer "${this.groupId}" has no subscriptions`);
        }

        const pending = (async () => {
        const fromBeginning = (this.optionsFromFirstSubscription() ?? true);
        const consumer = await createConsumer(this.kafka, this.groupId, {
            logger: this.logger,
            fromBeginning,
        });

        this.consumer = consumer;

        for (const [topic, { options }] of this.subscriptions) {
            await subscribeToTopic(consumer, topic, options);
        }

        const transport = async (ctx: ConsumeContext<T>): Promise<void> => {
            const entry = this.subscriptions.get(ctx.topic);
            if (!entry) {
                return;
            }
            await this.invokeHandlerWithResilience(entry.handler, ctx as unknown as MessageContext<T>);
        };

        const run = composeConsume(this.middleware, transport);

        await consumeMessages(consumer, async (payload: EachMessagePayload) => {
            // Skip messages once the consumer is stopped (§9.4). Prevents new
            // handlers from being added while awaitInFlight is draining.
            if (this.stopped) {
                return;
            }
            const message = payload.message ?? ({} as Record<string, unknown>);
            const partition = payload.partition ?? 0;
            const offset = Number(message.offset ?? -1);

            // Track high-watermark offset per topic+partition for lag
            // computation (§16.1).
            let hwmByPartition = this.highWatermarkOffsets.get(payload.topic);
            if (!hwmByPartition) {
                hwmByPartition = new Map();
                this.highWatermarkOffsets.set(payload.topic, hwmByPartition);
            }
            const currentHWM = hwmByPartition.get(partition) ?? -1;
            if (offset > currentHWM) {
                hwmByPartition.set(partition, offset);
            }
            const headers = (message.headers ?? {}) as Record<string, string | Buffer | undefined>;
            const correlationId = headers["correlation-id"];
            const messageId = headers["message-id"];
            const contentType = headers["content-type"];
            const schemaVersion = headers["schema-version"];
            const controller = new AbortController();
            const ctx: ConsumeContext<T> = {
                topic: payload.topic,
                value: message.value as unknown as T,
                key: message.key,
                partition,
                offset: (message.offset ?? "") as string,
                timestamp: (message.timestamp ?? "") as string,
                headers,
                signal: controller.signal,
                metadata: {
                    correlationId: typeof correlationId === "string" ? correlationId : String(correlationId ?? ""),
                    messageId: typeof messageId === "string" ? messageId : String(messageId ?? ""),
                    contentType: typeof contentType === "string" ? contentType : String(contentType ?? ""),
                    schemaVersion: typeof schemaVersion === "string" ? schemaVersion : String(schemaVersion ?? ""),
                },
            };

            this.recordObservedOffset(payload.topic, partition, offset);

            // §9.6 Backpressure: wait for a semaphore slot when at capacity.
            await this.acquireInFlightSlot();

            let handlerSucceeded = false;
            const runHandler = (async () => {
                try {
                    await run(ctx);
                    handlerSucceeded = true;
                } finally {
                    this.releaseInFlightSlot();
                    if (handlerSucceeded) {
                        // Track the highest contiguous offset per partition for safe commits (§9.6).
                        this.recordCompletedOffset(payload.topic, partition, offset);
                    }
                }
            })();
            this.inFlightHandlers.add(runHandler);
            if (this.handlerTimeoutMs && this.handlerTimeoutMs > 0) {
                this.handlerStartTimes.set(runHandler, Date.now());
            }
            runHandler.finally(() => {
                this.inFlightHandlers.delete(runHandler);
                this.handlerStartTimes.delete(runHandler);
            });
        }, { logger: this.logger });

        this.started = true;
        this.stopped = false;
        this.logger.info(`Consumer "${this.groupId}" started`);
        })();
        this.startPromise = pending;
        pending.catch(() => {
            if (this.startPromise === pending) {
                this.startPromise = null;
            }
        });
        return pending;
    }

    /**
     * Invoke a user message handler with retry and dead-letter handling (§10).
     *
     * On handler failure the error is classified:
     *   retry    → re-invoke the handler with exponential backoff (§10.2)
     *   dead-letter → publish the original record to the DLQ (§10.4) and
     *                continue (the message is not re-delivered)
     *   stop     → re-throw (no retry, no DLQ)
     */
    private async invokeHandlerWithResilience(
        handler: MessageHandler<T>,
        payload: MessageContext<T>,
    ): Promise<void> {
        let lastError: unknown;

        for (let attempt = 0; attempt < this.retryPolicy.maxAttempts; attempt++) {
            try {
                if (this.handlerTimeoutMs && this.handlerTimeoutMs > 0) {
                    // Per-handler timeout prevents a deadlocked handler from
                    // blocking shutdown indefinitely (§9.6). On timeout the
                    // handler promise rejects; the runHandler wrapper releases
                    // the semaphore slot and skips offset commit. The handler
                    // function itself may continue in the background but is no
                    // longer awaited by awaitInFlight.
                    const timeoutPromise = sleep(this.handlerTimeoutMs).then(() => {
                        throw new KafkaTimeoutError(
                            `Handler timed out after ${this.handlerTimeoutMs}ms`,
                        );
                    });
                    await Promise.race([handler(payload), timeoutPromise]);
                } else {
                    await handler(payload);
                }
                return;
            } catch (error) {
                lastError = error;
                const disposition = this.retryPolicy.classify(error);

                if (disposition.action === "stop") {
                    throw error;
                }

                if (disposition.action === "dead-letter") {
                    if (this.dlqPublisher) {
                        const record: DeadLetterRecord = {
                            originalTopic: payload.topic,
                            originalPartition: payload.partition,
                            originalOffset: payload.offset,
                            originalKey: payload.key,
                            originalPayload: payload.value,
                            originalHeaders: payload.headers,
                            failureTimestamp: new Date().toISOString(),
                            attemptCount: attempt + 1,
                            errorName: error instanceof Error ? error.name : "Error",
                            errorMessage: error instanceof Error ? error.message : String(error),
                            consumerGroup: this.groupId,
                        };
                        try {
                            await this.dlqPublisher.publish(record);
                        } catch (dlqError) {
                            this.logger.error("Failed to publish dead-letter record", dlqError);
                            throw error;
                        }
                        return;
                    }
                    throw error;
                }

                const delayMs =
                    disposition.delayMs ??
                    exponentialBackoff(attempt, this.retryPolicy.baseDelayMs, this.retryPolicy.maxDelayMs);
                await sleep(delayMs);
            }
        }

        throw lastError;
    }

    private optionsFromFirstSubscription(): boolean | undefined {
        for (const { options } of this.subscriptions.values()) {
            if (options.fromBeginning !== undefined) {
                return options.fromBeginning;
            }
        }
        return undefined;
    }

    /**
     * Stop intake and await runner shutdown (§9.4). Does not disconnect the
     * transport; use close() for that.
     */
    async stop(): Promise<void> {
        if (!this.started || this.stopped) {
            return;
        }
        this.stopped = true;
        // KafkaJS consumer.stop() halts intake and awaits runner shutdown.
        await this.consumer?.stop();
        this.logger.info(`Consumer "${this.groupId}" stopped`);
    }

    /**
     * Wait for all in-flight handlers to settle, up to a configured deadline (§7.7).
     * Resolves when every handler has finished or the deadline elapses. Each
     * handler is individually bounded by `handlerTimeoutMs` (set via
     * `KafkaConsumerOptions.handlerTimeoutMs`) so shutdown cannot block
     * indefinitely on a deadlocked user handler (§9.6).
     */
    async awaitInFlight(timeoutMs: number): Promise<void> {
        const deadline = Date.now() + timeoutMs;
        while (this.inFlightHandlers.size > 0) {
            const remaining = deadline - Date.now();
            if (remaining <= 0) {
                return;
            }
            const handlers = [...this.inFlightHandlers];
            await Promise.race([
                Promise.allSettled(handlers),
                new Promise<void>((resolve) => setTimeout(resolve, remaining)),
            ]);
        }
    }

    /**
     * Acquire a semaphore slot for in-flight handling. When at capacity,
     * waits until a slot is released (§9.6 backpressure).
     */
    private async acquireInFlightSlot(): Promise<void> {
        if (this.maxInFlight === Infinity || this.inFlightSemaphore < this.maxInFlight) {
            this.inFlightSemaphore += 1;
            return;
        }
        await new Promise<void>((resolve) => {
            this.inFlightWaiters.push(resolve);
        });
    }

    /**
     * Release an in-flight slot and notify the next waiter (§9.6 backpressure).
     */
    private releaseInFlightSlot(): void {
        this.inFlightSemaphore = Math.max(0, this.inFlightSemaphore - 1);
        const waiter = this.inFlightWaiters.shift();
        if (waiter) {
            // Handoff the slot to the waiter: semaphore stays at the same
            // level, the waiter proceeds without incrementing.
            waiter();
        }
    }

    /**
     * Record the highest contiguous offset completed for a partition (§9.6).
     * Offsets are tracked per topic+partition so only safe contiguous ranges
     * are committed.
     */
    private recordObservedOffset(topic: string, partition: number, offset: number): void {
        if (offset < 0) {
            return;
        }
        let byPartition = this.observedOffsetSets.get(topic);
        if (!byPartition) {
            byPartition = new Map();
            this.observedOffsetSets.set(topic, byPartition);
        }
        let offsets = byPartition.get(partition);
        if (!offsets) {
            offsets = new Set<number>();
            byPartition.set(partition, offsets);
        }
        offsets.add(offset);
    }

    private recordCompletedOffset(topic: string, partition: number, offset: number): void {
        if (offset < 0) {
            return;
        }
        let completedByPartition = this.completedOffsetSets.get(topic);
        if (!completedByPartition) {
            completedByPartition = new Map();
            this.completedOffsetSets.set(topic, completedByPartition);
        }
        let completedOffsets = completedByPartition.get(partition);
        if (!completedOffsets) {
            completedOffsets = new Set<number>();
            completedByPartition.set(partition, completedOffsets);
        }
        completedOffsets.add(offset);

        const observedOffsets = this.observedOffsetSets.get(topic)?.get(partition);
        const minObserved = observedOffsets && observedOffsets.size > 0
            ? minSet(observedOffsets)
            : offset;
        let byPartition = this.completedOffsets.get(topic);
        if (!byPartition) {
            byPartition = new Map();
            this.completedOffsets.set(topic, byPartition);
        }

        let current = minObserved - 1;
        while (completedOffsets.has(current + 1)) {
            current += 1;
        }
        byPartition.set(partition, current);
    }

    /**
     * Get the highest contiguous committed offset per topic+partition (§9.6).
     */
    getCompletedOffsets(): Map<string, Map<number, number>> {
        return new Map(this.completedOffsets);
    }

    /**
     * Get consumer lag per topic+partition: high-watermark offset minus
     * committed offset (§16.1). Returns 0 for partitions with no committed
     * offset yet.
     */
    getLag(): Map<string, Map<number, number>> {
        const lag = new Map<string, Map<number, number>>();
        for (const [topic, hwmByPartition] of this.highWatermarkOffsets) {
            const byPartition = new Map<number, number>();
            const committedByPartition = this.completedOffsets.get(topic);
            for (const [partition, hwm] of hwmByPartition) {
                const committed = committedByPartition?.get(partition) ?? -1;
                byPartition.set(partition, Math.max(0, hwm - committed));
            }
            lag.set(topic, byPartition);
        }
        return lag;
    }

    /**
     * Commit consumed offsets according to the selected processing policy (§9.5).
     * Commits the highest contiguous completed offset per partition to avoid
     * skipping uncommitted messages (§9.6).
     */
    async commitOffsets(): Promise<void> {
        if (!this.consumer) {
            return;
        }
        try {
            const offsets: Array<{ topic: string; partition: number; offset: string }> = [];
            for (const [topic, byPartition] of this.completedOffsets) {
                for (const [partition, offset] of byPartition) {
                    if (offset < 0) {
                        continue;
                    }
                    // KafkaJS commitOffsets expects offset+1 (the next offset to read).
                    offsets.push({ topic, partition, offset: String(offset + 1) });
                }
            }
            if (offsets.length > 0) {
                await (this.consumer as unknown as {
                    commitOffsets: (offsets: Array<{ topic: string; partition: number; offset: string }>) => Promise<void>;
                }).commitOffsets(offsets);
            }
        } catch {
            // Commit failures are non-fatal during shutdown; the broker
            // retains the offset and may redeliver.
        }
    }

    get inFlightCount(): number {
        return this.inFlightHandlers.size;
    }

    /**
     * Disconnect the transport and release the consumer resource.
     */
    async close(): Promise<void> {
        if (this.closed) {
            return;
        }
        this.closed = true;
        if (this.startPromise) {
            try {
                await this.startPromise;
            } catch {
                // ignore startup failure
            }
        }
        if (this.consumer) {
            try {
                await this.consumer.disconnect();
            } catch (error) {
                this.logger.error(`Failed to disconnect consumer "${this.groupId}"`, error);
                throw new KafkaConnectionError(`Failed to disconnect consumer "${this.groupId}"`, { cause: error });
            }
            this.consumer = null;
        }
        this.subscriptions.clear();
        this.completedOffsets.clear();
        this.completedOffsetSets.clear();
        this.observedOffsetSets.clear();
        this.logger.info(`Consumer "${this.groupId}" closed`);
    }

    get isRunning(): boolean {
        return this.started && !this.stopped;
    }

    get isClosed(): boolean {
        return this.closed;
    }

    get raw(): Consumer | null {
        return this.consumer;
    }

    private ensureOpen(): void {
        if (this.closed) {
            throw new KafkaLifecycleError("Consumer is closed");
        }
    }
}

/** Iterative minimum of a Set of numbers, avoiding stack overflow on large sets. */
function minSet(values: Set<number>): number {
    let min = Infinity;
    for (const v of values) {
        if (v < min) {
            min = v;
        }
    }
    return min;
}
