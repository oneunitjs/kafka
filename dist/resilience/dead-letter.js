import { Kafka } from "kafkajs";
import { KafkaProducerError } from "../errors/index.js";
/**
 * Publishes failed messages to a dead-letter queue, preserving original
 * record metadata (§10.4). Each retry attempt creates a fresh producer and
 * attempts connect, so connect failures are retried per-attempt.
 */
export class DeadLetterPublisher {
    kafka;
    dlqTopic;
    maxAttempts;
    constructor(kafka, options) {
        this.kafka = kafka;
        this.dlqTopic = options.dlqTopic;
        this.maxAttempts = options.maxAttempts ?? 3;
    }
    /**
     * Publish a failed message to the dead-letter queue, preserving the
     * original record metadata (§10.4).
     *
     * If DLQ publication itself fails after all retries, the error is thrown
     * so the caller can decide whether to commit the original offset (it
     * should not, per §10.5).
     */
    async publish(record) {
        let lastError;
        for (let attempt = 0; attempt < this.maxAttempts; attempt++) {
            const producer = this.kafka.producer();
            try {
                await producer.connect();
                await producer.send({
                    topic: this.dlqTopic,
                    messages: [
                        {
                            key: Buffer.from(JSON.stringify({
                                originalTopic: record.originalTopic,
                                originalPartition: record.originalPartition,
                                originalOffset: record.originalOffset,
                                correlationId: record.correlationId,
                                messageId: record.messageId,
                            })),
                            value: Buffer.from(JSON.stringify(record)),
                        },
                    ],
                });
                await producer.disconnect();
                return;
            }
            catch (error) {
                lastError = error;
                try {
                    await producer.disconnect();
                }
                catch {
                    // non-fatal
                }
            }
        }
        throw new KafkaProducerError(`Failed to publish dead-letter record for topic "${this.dlqTopic}"`, { cause: lastError, code: "KAFKA_PRODUCER_ERROR", topic: this.dlqTopic, retryable: true });
    }
}
