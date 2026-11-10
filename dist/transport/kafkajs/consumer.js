import { createLogger } from "./logger.js";
import { KafkaConfigError, KafkaConnectionError } from "../../errors/index.js";
import { connectKafkaResource } from "./factory.js";
import { Kafka } from "kafkajs";
export async function createConsumer(kafka, groupId, options = {}) {
    const logger = createLogger(options.logger);
    if (!groupId) {
        throw new KafkaConfigError("Kafka consumer groupId is required");
    }
    const { logger: _logger, topic: _topic, fromBeginning: _fromBeginning, ...consumerOptions } = options;
    return connectKafkaResource(() => kafka.consumer({
        groupId,
        allowAutoTopicCreation: false,
        maxWaitTimeInMs: 1000,
        ...consumerOptions,
    }), (consumer) => consumer.connect(), (consumer) => consumer.disconnect(), logger, `consumer "${groupId}"`);
}
export async function subscribeToTopic(consumer, topic, options = {}) {
    const logger = createLogger(options.logger);
    if (!topic || String(topic).trim() === "") {
        throw new KafkaConfigError("Kafka topic is required");
    }
    const fromBeginning = Boolean(options.fromBeginning ?? true);
    const { logger: _logger, topic: _topic, fromBeginning: _fromBeginning, ...subscribeOptions } = options;
    await consumer.subscribe({
        ...subscribeOptions,
        topic,
        fromBeginning,
    });
    logger.info(`Subscribed to Kafka topic "${topic}"`);
}
export async function consumeMessages(consumer, handler, options = {}) {
    const logger = createLogger(options.logger);
    await consumer.run({
        autoCommit: false,
        eachMessage: handler ?? (async ({ topic, message }) => {
            logger.info(`Received message on topic "${topic}"`, message);
        }),
    });
}
