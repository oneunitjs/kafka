import { Kafka, Partitioners } from "kafkajs";
import { type Logger } from "./logger.js";
export interface KafkaClientOptions {
    brokers?: string | string[];
    clientId?: string;
    groupId?: string;
    logger?: Logger;
    kafka?: Kafka;
    ssl?: boolean | object;
    sasl?: boolean | object;
    retry?: object;
    logLevel?: string;
    partitioner?: string;
    createPartitioner?: (() => (args: {
        topic: string;
        partitionMetadata: unknown[];
        key: Buffer | null;
    }) => number) | string;
    producer?: object;
    consumer?: object;
    admin?: object;
    codec?: unknown;
    shutdown?: {
        exit?: boolean;
        graceful?: boolean;
        timeout?: number;
    };
    [key: string]: unknown;
}
export interface NamespacedKafkaOptions {
    connection?: KafkaClientOptions;
    producer?: Record<string, unknown>;
    consumer?: Record<string, unknown>;
    admin?: Record<string, unknown>;
    serialization?: {
        codec?: unknown;
    };
    logging?: {
        logger?: Logger;
    };
    shutdown?: {
        exit?: boolean;
        graceful?: boolean;
        timeout?: number;
    };
    [key: string]: unknown;
}
/**
 * Merge namespaced configuration (§5.1) into the flat KafkaClientOptions shape.
 *
 * Namespaces:
 *   connection     -> flat kafka options (brokers, clientId, timeouts, ssl, sasl, ...)
 *   producer       -> producer factory options
 *   consumer       -> consumer factory options (+ groupId hoisted to top level)
 *   admin          -> admin factory options
 *   serialization  -> codec
 *   logging        -> logger
 *   shutdown       -> shutdown config
 *
 * Precedence (§5.2): flat top-level values win over namespace values; namespace
 * sub-objects merge into their corresponding factory options.
 */
export declare function resolveKafkaOptions(options: Record<string, unknown>): Record<string, unknown>;
export declare function parseBrokers(value: string | string[] | undefined, options?: Record<string, unknown>): string[];
export declare function isKafkaConfigured(options?: Record<string, unknown>): boolean;
export declare function resolvePartitioner(options?: Record<string, unknown>): typeof Partitioners.DefaultPartitioner;
export declare function createLogCreator(logger: Logger): () => ({ level, log }: {
    level: number;
    log?: unknown;
}) => void;
export declare function createKafka(loggerOrOptions: Logger | Record<string, unknown>, maybeOptions?: Record<string, unknown>): Kafka;
