import { Kafka, logLevel, Partitioners } from "kafkajs";
import { envNumber, envString } from "../../core/context.js";
import { KafkaConfigError } from "../../errors/index.js";
import { createLogger, resolveLoggerAndOptions } from "./logger.js";
import { readConfigNumber, readConfigString } from "../../adapters/config.js";
import { getSslConfig } from "../../security/ssl.js";
import { getSaslConfig } from "../../security/sasl.js";
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
export function resolveKafkaOptions(options) {
    const { connection, producer, consumer, admin, serialization, logging, shutdown, kafkaJS, ...flat } = options;
    const result = { ...flat };
    if (shutdown !== undefined) {
        result.shutdown = shutdown;
    }
    if (connection && typeof connection === "object") {
        for (const [key, value] of Object.entries(connection)) {
            if (result[key] === undefined) {
                result[key] = value;
            }
        }
    }
    if (producer && typeof producer === "object") {
        const existing = result.producer ?? {};
        result.producer = { ...existing, ...producer };
    }
    if (consumer && typeof consumer === "object") {
        const { groupId, ...consumerOpts } = consumer;
        if (groupId !== undefined && result.groupId === undefined) {
            result.groupId = groupId;
        }
        const existing = result.consumer ?? {};
        result.consumer = { ...existing, ...consumerOpts };
    }
    if (admin && typeof admin === "object") {
        const existing = result.admin ?? {};
        result.admin = { ...existing, ...admin };
    }
    if (serialization && typeof serialization === "object" && result.codec === undefined) {
        result.codec = serialization.codec;
    }
    if (logging && typeof logging === "object" && result.logger === undefined) {
        result.logger = logging.logger;
    }
    if (kafkaJS && typeof kafkaJS === "object") {
        for (const [key, value] of Object.entries(kafkaJS)) {
            if (result[key] === undefined) {
                result[key] = value;
            }
        }
    }
    return result;
}
const KAFKA_LOG_LEVELS = {
    debug: logLevel.DEBUG,
    info: logLevel.INFO,
    warn: logLevel.WARN,
    error: logLevel.ERROR,
    nothing: logLevel.NOTHING,
    none: logLevel.NOTHING,
};
const PARTITIONERS = {
    legacy: Partitioners.LegacyPartitioner,
    default: Partitioners.DefaultPartitioner,
    murmur2: Partitioners.JavaCompatiblePartitioner,
    "java-compatible": Partitioners.JavaCompatiblePartitioner,
};
export function parseBrokers(value, options = {}) {
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
export function isKafkaConfigured(options = {}) {
    try {
        return parseBrokers(options.brokers, options).length > 0;
    }
    catch {
        return false;
    }
}
export function resolvePartitioner(options = {}) {
    if (typeof options.createPartitioner === "function") {
        return options.createPartitioner;
    }
    const named = typeof options.createPartitioner === "string" ? options.createPartitioner : undefined;
    const name = String(options.partitioner ?? named ?? readConfigString(options, "KAFKA_CREATE_PARTITIONER", "default") ?? envString("KAFKA_CREATE_PARTITIONER", "default"))
        .trim()
        .toLowerCase();
    return PARTITIONERS[name] ?? Partitioners.DefaultPartitioner;
}
function resolveLogLevel(options = {}) {
    if (typeof options.logLevel === "number") {
        return options.logLevel;
    }
    const level = String(options.logLevel ?? readConfigString(options, "KAFKA_LOG_LEVEL", "info") ?? envString("KAFKA_LOG_LEVEL", "info"))
        .trim()
        .toLowerCase();
    return KAFKA_LOG_LEVELS[level] ?? logLevel.INFO;
}
function resolveRetryConfig(retry = {}, options = {}) {
    return {
        retries: retry.retries ?? readConfigNumber(options, "KAFKA_RETRIES", 8) ?? envNumber("KAFKA_RETRIES", 8),
        initialRetryTime: retry.initialRetryTime ?? readConfigNumber(options, "KAFKA_RETRY_INITIAL_TIME", 300) ?? envNumber("KAFKA_RETRY_INITIAL_TIME", 300),
        maxRetryTime: retry.maxRetryTime ?? readConfigNumber(options, "KAFKA_RETRY_MAX_TIME", 30000) ?? envNumber("KAFKA_RETRY_MAX_TIME", 30000),
        ...retry,
    };
}
function normalizeLogPayload(log) {
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
        const { message, ...extra } = log;
        return {
            message: message ?? "",
            extra: Object.keys(extra).length > 0 ? extra : undefined,
        };
    }
    return { message: log };
}
export function createLogCreator(logger) {
    return () => ({ level, log }) => {
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
export function createKafka(loggerOrOptions, maybeOptions = {}) {
    const { logger: resolvedLogger, options } = resolveLoggerAndOptions(loggerOrOptions, maybeOptions);
    const logger = createLogger(resolvedLogger);
    const resolved = resolveKafkaOptions(options);
    const ssl = getSslConfig(resolved);
    const sasl = getSaslConfig(resolved);
    const { brokers, clientId, retry, logLevel: _ignoredLogLevel, createPartitioner: _createPartitioner, partitioner: _partitioner, logger: _logger, ca: _ca, cert: _cert, key: _key, rejectUnauthorized: _rejectUnauthorized, sasl: _sasl, ssl: _ssl, logCreator, connectionTimeout, requestTimeout, authenticationTimeout, producer: _producer, consumer: _consumer, admin: _admin, groupId: _groupId, kafka: _kafka, topic: _topic, parseJson: _parseJson, fromBeginning: _fromBeginning, send: _send, exit: _exit, config: _config, codec: _codec, shutdown: _shutdown, kafkaJS: _kafkaJS, ...rest } = resolved;
    return new Kafka({
        clientId: String(clientId ?? readConfigString(resolved, "KAFKA_CLIENT_ID", "kafka-client") ?? envString("KAFKA_CLIENT_ID", "kafka-client")),
        brokers: parseBrokers(brokers, resolved),
        connectionTimeout: Number(connectionTimeout ?? readConfigNumber(resolved, "KAFKA_CONNECTION_TIMEOUT", 3000) ?? envNumber("KAFKA_CONNECTION_TIMEOUT", 3000)),
        requestTimeout: Number(requestTimeout ?? readConfigNumber(resolved, "KAFKA_REQUEST_TIMEOUT", 30000) ?? envNumber("KAFKA_REQUEST_TIMEOUT", 30000)),
        authenticationTimeout: Number(authenticationTimeout ?? readConfigNumber(resolved, "KAFKA_AUTHENTICATION_TIMEOUT", 10000) ?? envNumber("KAFKA_AUTHENTICATION_TIMEOUT", 10000)),
        ...(ssl ? { ssl } : {}),
        ...(sasl ? { sasl: sasl } : {}),
        retry: resolveRetryConfig(retry, resolved),
        logLevel: resolveLogLevel(resolved),
        logCreator: logCreator ?? createLogCreator(logger),
        ...rest,
    });
}
