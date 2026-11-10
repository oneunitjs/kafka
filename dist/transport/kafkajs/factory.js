import { createLogger } from "./logger.js";
import { KafkaConnectionError } from "../../errors/index.js";
import { resolvePartitioner } from "./config-mapper.js";
import { Kafka } from "kafkajs";
/**
 * Create a KafkaJS resource, connect it, and handle connect failures
 * with consistent error wrapping and best-effort disconnect cleanup.
 */
export async function connectKafkaResource(create, connect, disconnect, logger, resourceName) {
    const resource = create();
    logger.info(`Creating Kafka ${resourceName}`);
    try {
        await connect(resource);
        logger.info(`Kafka ${resourceName} connected`);
        return resource;
    }
    catch (error) {
        logger.error(`Failed to connect Kafka ${resourceName}`, error);
        try {
            await disconnect(resource);
        }
        catch {
            // Disconnect failure during startup is non-fatal.
        }
        throw new KafkaConnectionError(`Failed to connect Kafka ${resourceName}`, { cause: error });
    }
}
export async function createProducer(kafka, options = {}) {
    const logger = createLogger(options.logger);
    const { partitioner, createPartitioner, logger: _logger, ...producerOptions } = options;
    const partitionerFn = resolvePartitioner({ partitioner, createPartitioner });
    return connectKafkaResource(() => kafka.producer({
        allowAutoTopicCreation: false,
        createPartitioner: partitionerFn,
        ...producerOptions,
    }), (producer) => producer.connect(), (producer) => producer.disconnect(), logger, "producer");
}
