import { Kafka, logLevel, Partitioners, type SASLOptions } from "kafkajs";
import { envNumber, envString } from "../../core/context.js";
import { KafkaConfigError } from "../../errors/index.js";
import { createLogger, resolveLoggerAndOptions, type Logger } from "./logger.js";
import { readConfigNumber, readConfigString } from "../../adapters/config.js";
import { getSslConfig } from "../../security/ssl.js";
import { getSaslConfig } from "../../security/sasl.js";

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
    createPartitioner?: (() => (args: { topic: string; partitionMetadata: unknown[]; key: Buffer | null }) => number) | string;
    producer?: object;
    consumer?: object;
    admin?: object;
    codec?: unknown;
    shutdown?: { exit?: boolean; graceful?: boolean; timeout?: number };
    [key: string]: unknown;
}

export interface NamespacedKafkaOptions {
    connection?: KafkaClientOptions;
    producer?: Record<string, unknown>;
    consumer?: Record<string, unknown>;
    admin?: Record<string, unknown>;
    serialization?: { codec?: unknown };
    logging?: { logger?: Logger };
    shutdown?: { exit?: boolean; graceful?: boolean; timeout?: number };
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
export function resolveKafkaOptions(options: Record<string, unknown>): Record<string, unknown> {
    const {
        connection,
        producer,
        consumer,
        admin,
        serialization,
        logging,
        shutdown,
        kafkaJS,
        ...flat
    } = options;

    const result = { ...flat } as Record<string, unknown>;

    if (shutdown !== undefined) {
        result.shutdown = shutdown;
    }

    if (connection && typeof connection === "object") {
        for (const [key, value] of Object.entries(connection as Record<string, unknown>)) {
            if (result[key] === undefined) {
                result[key] = value;
            }
        }
    }

    if (producer && typeof producer === "object") {
        const existing = (result.producer as Record<string, unknown> | undefined) ?? {};
        result.producer = { ...existing, ...producer };
    }

    if (consumer && typeof consumer === "object") {
        const { groupId, ...consumerOpts } = consumer as Record<string, unknown>;
        if (groupId !== undefined && result.groupId === undefined) {
            result.groupId = groupId;
        }
        const existing = (result.consumer as Record<string, unknown> | undefined) ?? {};
        result.consumer = { ...existing, ...consumerOpts };
    }

    if (admin && typeof admin === "object") {
        const existing = (result.admin as Record<string, unknown> | undefined) ?? {};
        result.admin = { ...existing, ...admin };
    }

    if (serialization && typeof serialization === "object" && result.codec === undefined) {
        result.codec = (serialization as Record<string, unknown>).codec;
    }

    if (logging && typeof logging === "object" && result.logger === undefined) {
        result.logger = (logging as Record<string, unknown>).logger;
    }

    if (kafkaJS && typeof kafkaJS === "object") {
        for (const [key, value] of Object.entries(kafkaJS as Record<string, unknown>)) {
            if (result[key] === undefined) {
                result[key] = value;
            }
        }
    }

    return result;
}

const KAFKA_LOG_LEVELS: Record<string, number> = {
    debug: logLevel.DEBUG,
    info: logLevel.INFO,
    warn: logLevel.WARN,
    error: logLevel.ERROR,
    nothing: logLevel.NOTHING,
    none: logLevel.NOTHING,
};

const PARTITIONERS: Record<string, typeof Partitioners.DefaultPartitioner> = {
    legacy: Partitioners.LegacyPartitioner,
    default: Partitioners.DefaultPartitioner,
    murmur2: Partitioners.JavaCompatiblePartitioner,
    "java-compatible": Partitioners.JavaCompatiblePartitioner,
};

export function parseBrokers(value: string | string[] | undefined, options: Record<string, unknown> = {}): string[] {
    const raw = value ?? readConfigString(options, "KAFKA_BROKERS") ?? process.env.KAFKA_BROKERS;
    if (raw === undefined || raw === null || String(raw).trim() === "") {
        throw new KafkaConfigError("KAFKA_BROKERS environment variable is required");
    }

    const brokers = Array.isArray(raw)
        ? raw.map((broker) => String(broker).trim()).filter(Boolean)
        : String(raw)
              .split(",")
              .map((broker) => broker.trim())
              .filter(Boolean);

    if (brokers.length === 0) {
        throw new KafkaConfigError("KAFKA_BROKERS must contain at least one broker");
    }

    return brokers;
}

export function isKafkaConfigured(options: Record<string, unknown> = {}): boolean {
    try {
        return parseBrokers(options.brokers as string | string[] | undefined, options).length > 0;
    } catch {
        return false;
    }
}

export function resolvePartitioner(options: Record<string, unknown> = {}): typeof Partitioners.DefaultPartitioner {
    if (typeof options.createPartitioner === "function") {
        return options.createPartitioner as typeof Partitioners.DefaultPartitioner;
    }

    const named = typeof options.createPartitioner === "string" ? options.createPartitioner : undefined;

    const name = String(options.partitioner ?? named ?? readConfigString(options, "KAFKA_CREATE_PARTITIONER", "default") ?? envString("KAFKA_CREATE_PARTITIONER", "default"))
        .trim()
        .toLowerCase();

    return PARTITIONERS[name] ?? Partitioners.DefaultPartitioner;
}

function resolveLogLevel(options: Record<string, unknown> = {}): number {
    if (typeof options.logLevel === "number") {
        return options.logLevel;
    }

    const level = String(options.logLevel ?? readConfigString(options, "KAFKA_LOG_LEVEL", "info") ?? envString("KAFKA_LOG_LEVEL", "info"))
        .trim()
        .toLowerCase();

    return KAFKA_LOG_LEVELS[level] ?? logLevel.INFO;
}

function resolveRetryConfig(retry: Record<string, unknown> = {}, options: Record<string, unknown> = {}): Record<string, unknown> {
    return {
        retries: retry.retries ?? readConfigNumber(options, "KAFKA_RETRIES", 8) ?? envNumber("KAFKA_RETRIES", 8),
        initialRetryTime: retry.initialRetryTime ?? readConfigNumber(options, "KAFKA_RETRY_INITIAL_TIME", 300) ?? envNumber("KAFKA_RETRY_INITIAL_TIME", 300),
        maxRetryTime: retry.maxRetryTime ?? readConfigNumber(options, "KAFKA_RETRY_MAX_TIME", 30000) ?? envNumber("KAFKA_RETRY_MAX_TIME", 30000),
        ...retry,
    };
}

function normalizeLogPayload(log: unknown): { message: unknown; extra?: Record<string, unknown> } {
    if (log === undefined || log === null) {
        return { message: "" };
    }

    if (typeof log === "string") {
        return { message: log };
    }

    if (log instanceof Error) {
        return { message: log.message, extra: { err: log } };
    }

    if (typeof log === "object") {
        const { message, ...extra } = log as Record<string, unknown>;
        return {
            message: message ?? "",
            extra: Object.keys(extra).length > 0 ? extra : undefined,
        };
    }

    return { message: log };
}

export function createLogCreator(logger: Logger) {
    return () =>
        ({ level, log }: { level: number; log?: unknown }) => {
            const { message, extra } = normalizeLogPayload(log);
            if (level === logLevel.ERROR) {
                logger.error(message, extra);
                return;
            }
            if (level === logLevel.WARN) {
                logger.warn(message, extra);
                return;
            }
            if (level === logLevel.INFO) {
                logger.info(message, extra);
                return;
            }
            logger.debug(message, extra);
        };
}

export function createKafka(loggerOrOptions: Logger | Record<string, unknown>, maybeOptions: Record<string, unknown> = {}): Kafka {
    const { logger: resolvedLogger, options } = resolveLoggerAndOptions(
        loggerOrOptions,
        maybeOptions,
    );
    const logger = createLogger(resolvedLogger);
    const resolved = resolveKafkaOptions(options);
    const ssl = getSslConfig(resolved);
    const sasl = getSaslConfig(resolved);

    const {
        brokers,
        clientId,
        retry,
        logLevel: _ignoredLogLevel,
        createPartitioner: _createPartitioner,
        partitioner: _partitioner,
        logger: _logger,
        ca: _ca,
        cert: _cert,
        key: _key,
        rejectUnauthorized: _rejectUnauthorized,
        sasl: _sasl,
        ssl: _ssl,
        logCreator,
        connectionTimeout,
        requestTimeout,
        authenticationTimeout,
        producer: _producer,
        consumer: _consumer,
        admin: _admin,
        groupId: _groupId,
        kafka: _kafka,
        topic: _topic,
        parseJson: _parseJson,
        fromBeginning: _fromBeginning,
        send: _send,
        exit: _exit,
        config: _config,
        codec: _codec,
        shutdown: _shutdown,
        kafkaJS: _kafkaJS,
        ...rest
    } = resolved as Record<string, unknown>;

    return new Kafka({
        clientId: String(clientId ?? readConfigString(resolved, "KAFKA_CLIENT_ID", "kafka-client") ?? envString("KAFKA_CLIENT_ID", "kafka-client")),
        brokers: parseBrokers(brokers as string | string[] | undefined, resolved),
        connectionTimeout: Number(connectionTimeout ?? readConfigNumber(resolved, "KAFKA_CONNECTION_TIMEOUT", 3000) ?? envNumber("KAFKA_CONNECTION_TIMEOUT", 3000)),
        requestTimeout: Number(requestTimeout ?? readConfigNumber(resolved, "KAFKA_REQUEST_TIMEOUT", 30000) ?? envNumber("KAFKA_REQUEST_TIMEOUT", 30000)),
        authenticationTimeout: Number(authenticationTimeout ?? readConfigNumber(resolved, "KAFKA_AUTHENTICATION_TIMEOUT", 10000) ?? envNumber("KAFKA_AUTHENTICATION_TIMEOUT", 10000)),
        ...(ssl ? { ssl } : {}),
        ...(sasl ? { sasl: sasl as SASLOptions } : {}),
        retry: resolveRetryConfig(retry as Record<string, unknown>, resolved),
        logLevel: resolveLogLevel(resolved),
        logCreator: logCreator as ((logLevel: number) => (entry: { level: number; log?: Record<string, unknown> }) => void) ?? createLogCreator(logger),
        ...rest,
    });
}