import { createLogger, type Logger } from "./logger.js";
import { KafkaConnectionError } from "../../errors/index.js";
import { resolvePartitioner } from "./config-mapper.js";
import { Kafka, type Producer, type ICustomPartitioner } from "kafkajs";

export type { Producer } from "kafkajs";

export interface CreateProducerOptions {
    logger?: Logger;
    partitioner?: string;
    createPartitioner?: (() => (args: { topic: string; partitionMetadata: unknown[]; key: Buffer | null }) => number) | string;
    [key: string]: unknown;
}

/**
 * Create a KafkaJS resource, connect it, and handle connect failures
 * with consistent error wrapping and best-effort disconnect cleanup.
 */
export async function connectKafkaResource<T>(
    create: () => T,
    connect: (resource: T) => Promise<void>,
    disconnect: (resource: T) => Promise<void>,
    logger: Logger,
    resourceName: string,
): Promise<T> {
    const resource = create();
    logger.info(`Creating Kafka ${resourceName}`);
    try {
        await connect(resource);
        logger.info(`Kafka ${resourceName} connected`);
        return resource;
    } catch (error) {
        logger.error(`Failed to connect Kafka ${resourceName}`, error);
        try {
            await disconnect(resource);
        } catch {
            // Disconnect failure during startup is non-fatal.
        }
        throw new KafkaConnectionError(`Failed to connect Kafka ${resourceName}`, { cause: error });
    }
}

export async function createProducer(
    kafka: Kafka,
    options: CreateProducerOptions = {},
): Promise<Producer> {
    const logger = createLogger(options.logger);
    const { partitioner, createPartitioner, logger: _logger, ...producerOptions } = options;

    const partitionerFn = resolvePartitioner({ partitioner, createPartitioner });

    return connectKafkaResource(
        () =>
            kafka.producer({
                allowAutoTopicCreation: false,
                createPartitioner: partitionerFn as ICustomPartitioner,
                ...producerOptions,
            }),
        (producer) => producer.connect(),
        (producer) => producer.disconnect(),
        logger,
        "producer",
    );
}
