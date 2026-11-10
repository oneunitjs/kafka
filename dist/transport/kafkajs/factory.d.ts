import { type Logger } from "./logger.js";
import { Kafka, type Producer } from "kafkajs";
export type { Producer } from "kafkajs";
export interface CreateProducerOptions {
    logger?: Logger;
    partitioner?: string;
    createPartitioner?: (() => (args: {
        topic: string;
        partitionMetadata: unknown[];
        key: Buffer | null;
    }) => number) | string;
    [key: string]: unknown;
}
/**
 * Create a KafkaJS resource, connect it, and handle connect failures
 * with consistent error wrapping and best-effort disconnect cleanup.
 */
export declare function connectKafkaResource<T>(create: () => T, connect: (resource: T) => Promise<void>, disconnect: (resource: T) => Promise<void>, logger: Logger, resourceName: string): Promise<T>;
export declare function createProducer(kafka: Kafka, options?: CreateProducerOptions): Promise<Producer>;
