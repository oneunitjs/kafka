import { createLogger, type Logger } from "./logger.js";
import { KafkaConfigError, KafkaConnectionError } from "../../errors/index.js";
import { connectKafkaResource } from "./factory.js";
import { Kafka, type Consumer, type EachMessagePayload } from "kafkajs";

export type { Consumer, EachMessagePayload } from "kafkajs";

export interface CreateConsumerOptions {
    logger?: Logger;
    topic?: string;
    fromBeginning?: boolean;
    [key: string]: unknown;
}

export async function createConsumer(
    kafka: Kafka,
    groupId: string,
    options: CreateConsumerOptions = {},
): Promise<Consumer> {
    const logger = createLogger(options.logger);

    if (!groupId) {
        throw new KafkaConfigError("Kafka consumer groupId is required");
    }

    const { logger: _logger, topic: _topic, fromBeginning: _fromBeginning, ...consumerOptions } = options;

    return connectKafkaResource(
        () =>
            kafka.consumer({
                groupId,
                allowAutoTopicCreation: false,
                maxWaitTimeInMs: 1000,
                ...consumerOptions,
            }),
        (consumer) => consumer.connect(),
        (consumer) => consumer.disconnect(),
        logger,
        `consumer "${groupId}"`,
    );
}

export async function subscribeToTopic(
    consumer: Consumer,
    topic: string,
    options: CreateConsumerOptions = {},
): Promise<void> {
    const logger = createLogger(options.logger);

    if (!topic || String(topic).trim() === "") {
        throw new KafkaConfigError("Kafka topic is required");
    }

    const fromBeginning = Boolean(options.fromBeginning ?? true);
    const { logger: _logger, topic: _topic, fromBeginning: _fromBeginning, ...subscribeOptions } = options as Record<string, unknown>;

    await consumer.subscribe({
        ...subscribeOptions,
        topic,
        fromBeginning,
    });
    logger.info(`Subscribed to Kafka topic "${topic}"`);
}

export async function consumeMessages(
    consumer: Consumer,
    handler?: (payload: EachMessagePayload) => Promise<void>,
    options: { logger?: Logger } = {},
): Promise<void> {
    const logger = createLogger(options.logger);

    await consumer.run({
        autoCommit: false,
        eachMessage: handler ?? (async ({ topic, message }: { topic: string; message: object }) => {
            logger.info(`Received message on topic "${topic}"`, message);
        }),
    });
}
