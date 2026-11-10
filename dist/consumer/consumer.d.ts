import { type Logger } from "../transport/kafkajs/logger.js";
import { Kafka, type Consumer } from "kafkajs";
import { type ConsumeMiddleware } from "../middleware/compose.js";
import { type RetryPolicy } from "../resilience/retry.js";
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
export declare class KafkaConsumer<T = unknown> {
    private readonly groupId;
    private readonly kafka;
    private readonly logger;
    private consumer;
    private started;
    private startPromise;
    private stopped;
    private closed;
    private readonly middleware;
    private readonly subscriptions;
    private readonly retryPolicy;
    private readonly deadLetterTopic;
    private readonly dlqPublisher;
    private readonly pauseReasons;
    private readonly inFlightHandlers;
    private readonly maxInFlight;
    private readonly handlerTimeoutMs;
    private readonly handlerStartTimes;
    private readonly completedOffsets;
    private readonly completedOffsetSets;
    private readonly observedOffsetSets;
    private readonly highWatermarkOffsets;
    private inFlightSemaphore;
    private readonly inFlightWaiters;
    constructor(groupId: string, kafka: Kafka, options?: KafkaConsumerOptions);
    /**
     * Pause consumption under a named reason (§9.7). The consumer stays
     * paused until every outstanding reason is resumed, so a single
     * `resume()` cannot accidentally undo another component's pause.
     */
    pause(reason?: string, topics?: string[]): Promise<void>;
    /**
     * Resume a previously recorded pause reason (§9.7).
     */
    resume(reason?: string, topics?: string[]): Promise<void>;
    get pausedReasons(): ReadonlySet<string>;
    get isPaused(): boolean;
    /**
     * Register publish middleware for this consumer. Runs in registration
     * order for the inbound path and reverse order on unwind (§11.3).
     */
    use(middleware: ConsumeMiddleware<T>): this;
    /**
     * Subscribe to a topic. Registration is deferred until `start()`;
     * multiple topics may be registered before starting.
     */
    subscribe(topic: string, handler: MessageHandler<T>, options?: KafkaConsumerOptions): Promise<void>;
    /**
     * Connect and start one managed runner per consumer (§9.2). Calling
     * start() again while running returns the existing start promise.
     */
    start(): Promise<void>;
    /**
     * Invoke a user message handler with retry and dead-letter handling (§10).
     *
     * On handler failure the error is classified:
     *   retry    → re-invoke the handler with exponential backoff (§10.2)
     *   dead-letter → publish the original record to the DLQ (§10.4) and
     *                continue (the message is not re-delivered)
     *   stop     → re-throw (no retry, no DLQ)
     */
    private invokeHandlerWithResilience;
    private optionsFromFirstSubscription;
    /**
     * Stop intake and await runner shutdown (§9.4). Does not disconnect the
     * transport; use close() for that.
     */
    stop(): Promise<void>;
    /**
     * Wait for all in-flight handlers to settle, up to a configured deadline (§7.7).
     * Resolves when every handler has finished or the deadline elapses. Each
     * handler is individually bounded by `handlerTimeoutMs` (set via
     * `KafkaConsumerOptions.handlerTimeoutMs`) so shutdown cannot block
     * indefinitely on a deadlocked user handler (§9.6).
     */
    awaitInFlight(timeoutMs: number): Promise<void>;
    /**
     * Acquire a semaphore slot for in-flight handling. When at capacity,
     * waits until a slot is released (§9.6 backpressure).
     */
    private acquireInFlightSlot;
    /**
     * Release an in-flight slot and notify the next waiter (§9.6 backpressure).
     */
    private releaseInFlightSlot;
    /**
     * Record the highest contiguous offset completed for a partition (§9.6).
     * Offsets are tracked per topic+partition so only safe contiguous ranges
     * are committed.
     */
    private recordObservedOffset;
    private recordCompletedOffset;
    /**
     * Get the highest contiguous committed offset per topic+partition (§9.6).
     */
    getCompletedOffsets(): Map<string, Map<number, number>>;
    /**
     * Get consumer lag per topic+partition: high-watermark offset minus
     * committed offset (§16.1). Returns 0 for partitions with no committed
     * offset yet.
     */
    getLag(): Map<string, Map<number, number>>;
    /**
     * Commit consumed offsets according to the selected processing policy (§9.5).
     * Commits the highest contiguous completed offset per partition to avoid
     * skipping uncommitted messages (§9.6).
     */
    commitOffsets(): Promise<void>;
    get inFlightCount(): number;
    /**
     * Disconnect the transport and release the consumer resource.
     */
    close(): Promise<void>;
    get isRunning(): boolean;
    get isClosed(): boolean;
    get raw(): Consumer | null;
    private ensureOpen;
}
