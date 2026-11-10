import { Kafka } from "kafkajs";
export interface DeadLetterRecord {
    readonly originalTopic: string;
    readonly originalPartition: number;
    readonly originalOffset: string;
    readonly originalKey: unknown;
    readonly originalPayload: unknown;
    readonly originalHeaders: Record<string, unknown>;
    readonly failureTimestamp: string;
    readonly attemptCount: number;
    readonly errorName: string;
    readonly errorMessage: string;
    readonly consumerGroup?: string;
    readonly correlationId?: string;
    readonly messageId?: string;
}
export interface DeadLetterPublisherOptions {
    readonly dlqTopic: string;
    readonly maxAttempts?: number;
}
/**
 * Publishes failed messages to a dead-letter queue, preserving original
 * record metadata (§10.4). Each retry attempt creates a fresh producer and
 * attempts connect, so connect failures are retried per-attempt.
 */
export declare class DeadLetterPublisher {
    private readonly kafka;
    private readonly dlqTopic;
    private readonly maxAttempts;
    constructor(kafka: Kafka, options: DeadLetterPublisherOptions);
    /**
     * Publish a failed message to the dead-letter queue, preserving the
     * original record metadata (§10.4).
     *
     * If DLQ publication itself fails after all retries, the error is thrown
     * so the caller can decide whether to commit the original offset (it
     * should not, per §10.5).
     */
    publish(record: DeadLetterRecord): Promise<void>;
}
