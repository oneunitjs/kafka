import { createKafka, isKafkaConfigured, resolveKafkaOptions } from "../transport/kafkajs/adapter.js";
import { createProducer } from "../transport/kafkajs/adapter.js";
import { createConsumer, subscribeToTopic, consumeMessages } from "../transport/kafkajs/adapter.js";
import { createAdmin } from "../transport/kafkajs/adapter.js";
import { registerShutdown, shutdownClient } from "../transport/kafkajs/adapter.js";
import { createLogger } from "../transport/kafkajs/logger.js";
import { envString } from "./context.js";
import { KafkaConnectionError, KafkaDecodeError, KafkaLifecycleError } from "../errors/index.js";
import { resolveCodec } from "../adapters/codec.js";
import { normalizeOutgoing } from "../utils/encoding.js";
import { Kafka } from "kafkajs";
export class KafkaClient {
    logger;
    options;
    kafka;
    producer;
    consumer;
    admin;
    connected;
    codec;
    subscriptions;
    generation;
    producerPending;
    consumerPending;
    adminPending;
    failedResources = new Set();
    closePromise = null;
    closed = false;
    constructor(options = {}) {
        this.logger = createLogger(options.logger);
        this.options = resolveKafkaOptions(options);
        this.codec = resolveCodec(options.codec);
        this.kafka = options.kafka ?? createKafka(this.logger, options);
        this.producer = null;
        this.consumer = null;
        this.admin = null;
        this.subscriptions = new Map();
        this.generation = 0;
        this.closed = false;
        this.producerPending = { value: null };
        this.consumerPending = { value: null };
        this.adminPending = { value: null };
        this.connected = {
            producer: false,
            consumer: false,
            admin: false,
        };
    }
    static isConfigured(options) {
        return isKafkaConfigured(options);
    }
    async getProducer(options = {}) {
        return this.getResource("producer", () => this.producer, (p) => { this.producer = p; this.connected.producer = true; }, this.producerPending, () => createProducer(this.kafka, {
            ...this.clientLevelProducerOptions(),
            ...(this.options.producer ?? {}),
            ...options,
            logger: this.logger,
        }));
    }
    async getConsumer(groupIdOrOptions, maybeOptions = {}) {
        const options = typeof groupIdOrOptions === "string"
            ? { ...maybeOptions, groupId: groupIdOrOptions }
            : { ...(groupIdOrOptions ?? {}), ...maybeOptions };
        const groupId = options.groupId ?? this.options.groupId ?? envString("KAFKA_GROUP_ID");
        return this.getResource("consumer", () => this.consumer, (c) => { this.consumer = c; this.connected.consumer = true; }, this.consumerPending, () => createConsumer(this.kafka, groupId, {
            ...stripClientOnlyOptions((this.options.consumer ?? {})),
            ...stripClientOnlyOptions(options),
            logger: this.logger,
        }));
    }
    async getAdmin(options = {}) {
        return this.getResource("admin", () => this.admin, (a) => { this.admin = a; this.connected.admin = true; }, this.adminPending, () => createAdmin(this.kafka, {
            ...(this.options.admin ?? {}),
            ...options,
            logger: this.logger,
        }));
    }
    /**
     * Generic resource acquisition with generation-based close safety (§7.4).
     * Used by getProducer/getConsumer/getAdmin to eliminate duplicated
     * closed/failed/cache/try-catch boilerplate.
     */
    async getResource(resourceName, cacheGetter, cacheSetter, pending, factory) {
        if (this.closed) {
            throw new KafkaLifecycleError(`KafkaClient is closed; ${resourceName} cannot be created`, { code: "KAFKA_LIFECYCLE_ERROR", operation: `get${resourceName}`, resource: resourceName, retryable: false });
        }
        const cached = cacheGetter();
        if (cached) {
            return cached;
        }
        if (this.failedResources.has(resourceName)) {
            throw new KafkaLifecycleError(`${resourceName} is in FAILED state; call retry() to clear and reconnect`, { code: "KAFKA_LIFECYCLE_ERROR", operation: `get${resourceName}`, resource: resourceName, retryable: true });
        }
        const generation = this.generation;
        try {
            const resource = await resolveOnce(pending, factory);
            await this.adoptIfCurrent(generation, resource, shutdownClient.bind(undefined, this.logger, resource, `Kafka ${resourceName}`));
            if (generation !== this.generation) {
                throw new KafkaConnectionError("Kafka client was disconnected while it was connecting");
            }
            this.failedResources.delete(resourceName);
            cacheSetter(resource);
            return resource;
        }
        catch (error) {
            this.failedResources.add(resourceName);
            pending.value = null;
            throw error;
        }
    }
    async send(topic, messages, options = {}) {
        const producer = await this.getProducer(options.producer);
        const payload = Array.isArray(messages) ? messages : [messages];
        const codec = this.resolveCodecOption(options.codec);
        return producer.send({
            ...options.send,
            topic,
            messages: payload.map((message) => normalizeOutgoing(message, codec)),
        });
    }
    resolveCodecOption(codec) {
        return codec === undefined ? this.codec : resolveCodec(codec);
    }
    /**
     * Guards against a client being connected after `disconnect()` already ran.
     * `disconnect()` bumps the generation, so a connection that was in flight at that
     * moment is torn down instead of being cached into a client that looks connected.
     */
    async adoptIfCurrent(generation, _client, discard) {
        if (generation === this.generation) {
            return;
        }
        await discard();
        throw new KafkaConnectionError("Kafka client was disconnected while it was connecting");
    }
    clientLevelProducerOptions() {
        const options = {};
        if (this.options.partitioner !== undefined) {
            options.partitioner = this.options.partitioner;
        }
        if (this.options.createPartitioner !== undefined) {
            options.createPartitioner = this.options.createPartitioner;
        }
        return options;
    }
    async subscribe(topic, options = {}) {
        const consumer = await this.getConsumer(options);
        await this.subscribeOnce(consumer, topic, options);
        return consumer;
    }
    async subscribeOnce(consumer, topic, options) {
        const existing = this.subscriptions.get(topic);
        if (existing) {
            return existing;
        }
        // Track the in-flight promise, not just the finished state, so two concurrent
        // calls cannot both pass an empty check and subscribe the same topic twice.
        const pending = subscribeToTopic(consumer, topic, {
            ...options,
            logger: this.logger,
        }).catch((error) => {
            this.subscriptions.delete(topic);
            throw error;
        });
        this.subscriptions.set(topic, pending);
        return pending;
    }
    async consume(topicOrHandler, handlerOrOptions, maybeOptions = {}) {
        let topic;
        let handler;
        let options;
        if (typeof topicOrHandler === "function") {
            handler = topicOrHandler;
            options = handlerOrOptions ?? {};
        }
        else {
            topic = topicOrHandler;
            handler = handlerOrOptions;
            options = maybeOptions ?? {};
        }
        const consumer = await this.getConsumer(options);
        if (topic) {
            await this.subscribeOnce(consumer, topic, options);
        }
        await consumeMessages(consumer, wrapHandler(handler, this.logger, options, this.resolveCodecOption(options.codec)), {
            logger: this.logger,
        });
        return consumer;
    }
    registerShutdown(extra = {}) {
        return registerShutdown(this.logger, {
            producer: () => this.producer ?? undefined,
            consumer: () => this.consumer ?? undefined,
            admin: () => this.admin ?? undefined,
            ...extra,
        });
    }
    async disconnect() {
        if (this.closePromise) {
            return this.closePromise;
        }
        this.closePromise = (async () => {
            this.generation += 1;
            await Promise.allSettled([
                shutdownClient(this.logger, this.producer ?? undefined, "Kafka producer"),
                shutdownClient(this.logger, this.consumer ?? undefined, "Kafka consumer"),
                shutdownClient(this.logger, this.admin ?? undefined, "Kafka admin"),
            ]);
            this.producer = null;
            this.consumer = null;
            this.admin = null;
            this.subscriptions.clear();
            this.producerPending.value = null;
            this.consumerPending.value = null;
            this.adminPending.value = null;
            this.connected = {
                producer: false,
                consumer: false,
                admin: false,
            };
            this.closed = true;
        })();
        return this.closePromise;
    }
    /**
     * Clears the FAILED lifecycle state for all resources so reconnection can
     * be attempted. A closed client cannot be retried; CLOSED is terminal
     * (§7.2).
     */
    async retry() {
        if (this.closed) {
            throw new KafkaLifecycleError("KafkaClient is closed; retry is not permitted after disconnect", { code: "KAFKA_LIFECYCLE_ERROR", operation: "retry", retryable: false });
        }
        this.failedResources.clear();
        this.producerPending.value = null;
        this.consumerPending.value = null;
        this.adminPending.value = null;
    }
    /**
     * Returns the set of resources currently in FAILED state.
     */
    get failed() {
        return this.failedResources;
    }
}
export function createKafkaClient(options = {}) {
    return new KafkaClient(options);
}
const CLIENT_ONLY_OPTION_KEYS = ["parseJson", "fromBeginning", "codec", "send", "exit"];
function resolveOnce(pending, factory) {
    if (pending.value) {
        return pending.value;
    }
    const promise = factory();
    pending.value = promise;
    promise.catch(() => {
        if (pending.value === promise) {
            pending.value = null;
        }
    });
    return promise;
}
function stripClientOnlyOptions(options) {
    const rest = { ...options };
    for (const key of CLIENT_ONLY_OPTION_KEYS) {
        delete rest[key];
    }
    return rest;
}
function decodeIncoming(value, parseJson, codec) {
    if (value === undefined || value === null) {
        return value;
    }
    if (!parseJson) {
        return Buffer.isBuffer(value) || value instanceof Uint8Array
            ? Buffer.from(value).toString()
            : value;
    }
    return codec.decode(value);
}
function wrapHandler(handler, logger, options = {}, codec) {
    if (!handler) {
        return undefined;
    }
    return async (payload) => {
        const message = payload.message ?? {};
        let value = message.value;
        let key = message.key;
        try {
            value = decodeIncoming(value, options.parseJson !== false, codec);
            key = decodeIncoming(key, options.parseJson !== false, codec);
        }
        catch (error) {
            const decodeError = new KafkaDecodeError("Failed to decode Kafka message", { cause: error });
            logger.error("Failed to decode Kafka message", decodeError);
            throw decodeError;
        }
        const { message: _message, ...restPayload } = payload;
        const decoded = { ...message, key: key ?? undefined, value };
        await handler({
            ...restPayload,
            message: decoded,
            key: key ?? undefined,
            value,
        });
    };
}
