import { createKafka, isKafkaConfigured, resolveKafkaOptions, type KafkaClientOptions } from "../transport/kafkajs/adapter.js";
import { createProducer, type Producer } from "../transport/kafkajs/adapter.js";
import { createConsumer, subscribeToTopic, consumeMessages, type Consumer } from "../transport/kafkajs/adapter.js";
import { createAdmin, type Admin } from "../transport/kafkajs/adapter.js";
import { registerShutdown, shutdownClient } from "../transport/kafkajs/adapter.js";
import { createLogger, type Logger } from "../transport/kafkajs/logger.js";
import { envString } from "./context.js";
import { KafkaConnectionError, KafkaDecodeError, KafkaLifecycleError } from "../errors/index.js";
import { resolveCodec, type Codec } from "../adapters/codec.js";
import { normalizeOutgoing } from "../utils/encoding.js";
import { Kafka, type RecordMetadata, type EachMessagePayload, type Message, type ProducerRecord } from "kafkajs";

export class KafkaClient {
    logger: Logger;
    options: KafkaClientOptions;
    kafka: Kafka;
    producer: Producer | null;
    consumer: Consumer | null;
    admin: Admin | null;
    connected: {
        producer: boolean;
        consumer: boolean;
        admin: boolean;
    };
    codec: Codec;
    private subscriptions: Map<string, Promise<void>>;
    private generation: number;
    private producerPending: Pending<Producer>;
    private consumerPending: Pending<Consumer>;
    private adminPending: Pending<Admin>;
    private readonly failedResources = new Set<"producer" | "consumer" | "admin">();
    private closePromise: Promise<void> | null = null;
    private closed = false;

    constructor(options: KafkaClientOptions = {}) {
        this.logger = createLogger(options.logger);
        this.options = resolveKafkaOptions(options) as KafkaClientOptions;
        this.codec = resolveCodec((options as Record<string, unknown>).codec as Codec | undefined);
        this.kafka = (options as Record<string, unknown>).kafka as Kafka ?? createKafka(this.logger, options as Record<string, unknown>);
        this.producer = null;
        this.consumer = null;
        this.admin = null;
        this.subscriptions = new Map<string, Promise<void>>();
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

    static isConfigured(options: Pick<KafkaClientOptions, "brokers">): boolean {
        return isKafkaConfigured(options);
    }

    async getProducer(options: KafkaClientOptions["producer"] = {}): Promise<Producer> {
        return this.getResource(
            "producer",
            () => this.producer,
            (p) => { this.producer = p; this.connected.producer = true; },
            this.producerPending,
            () => createProducer(this.kafka, {
                ...this.clientLevelProducerOptions(),
                ...(this.options.producer ?? {}),
                ...options,
                logger: this.logger,
            }),
        );
    }

    async getConsumer(groupIdOrOptions: string | KafkaClientOptions["consumer"] | undefined, maybeOptions: KafkaClientOptions["consumer"] = {}): Promise<Consumer> {
        const options = typeof groupIdOrOptions === "string"
            ? { ...maybeOptions, groupId: groupIdOrOptions }
            : { ...(groupIdOrOptions ?? {}), ...maybeOptions };

        const groupId = options.groupId ?? this.options.groupId ?? envString("KAFKA_GROUP_ID");

        return this.getResource(
            "consumer",
            () => this.consumer,
            (c) => { this.consumer = c; this.connected.consumer = true; },
            this.consumerPending,
            () => createConsumer(this.kafka, groupId as string, {
                ...stripClientOnlyOptions((this.options.consumer ?? {}) as Record<string, unknown>),
                ...stripClientOnlyOptions(options as Record<string, unknown>),
                logger: this.logger,
            }),
        );
    }

    async getAdmin(options: KafkaClientOptions["admin"] = {}): Promise<Admin> {
        return this.getResource(
            "admin",
            () => this.admin,
            (a) => { this.admin = a; this.connected.admin = true; },
            this.adminPending,
            () => createAdmin(this.kafka, {
                ...(this.options.admin ?? {}),
                ...options,
                logger: this.logger,
            }),
        );
    }

    /**
     * Generic resource acquisition with generation-based close safety (§7.4).
     * Used by getProducer/getConsumer/getAdmin to eliminate duplicated
     * closed/failed/cache/try-catch boilerplate.
     */
    private async getResource<T>(
        resourceName: "producer" | "consumer" | "admin",
        cacheGetter: () => T | null,
        cacheSetter: (value: T) => void,
        pending: Pending<T>,
        factory: () => Promise<T>,
    ): Promise<T> {
        if (this.closed) {
            throw new KafkaLifecycleError(
                `KafkaClient is closed; ${resourceName} cannot be created`,
                { code: "KAFKA_LIFECYCLE_ERROR", operation: `get${resourceName}`, resource: resourceName, retryable: false },
            );
        }
        const cached = cacheGetter();
        if (cached) {
            return cached;
        }
        if (this.failedResources.has(resourceName)) {
            throw new KafkaLifecycleError(
                `${resourceName} is in FAILED state; call retry() to clear and reconnect`,
                { code: "KAFKA_LIFECYCLE_ERROR", operation: `get${resourceName}`, resource: resourceName, retryable: true },
            );
        }

        const generation = this.generation;
        try {
            const resource = await resolveOnce(pending, factory);
            await this.adoptIfCurrent(
                generation,
                resource,
                shutdownClient.bind(
                    undefined,
                    this.logger,
                    resource as unknown as { disconnect: () => Promise<void> } | undefined,
                    `Kafka ${resourceName}`,
                ),
            );
            if (generation !== this.generation) {
                throw new KafkaConnectionError("Kafka client was disconnected while it was connecting");
            }
            this.failedResources.delete(resourceName);
            cacheSetter(resource);
            return resource;
        } catch (error) {
            this.failedResources.add(resourceName);
            pending.value = null;
            throw error;
        }
    }

    async send(topic: string, messages: unknown | unknown[], options: SendOptions = {}): Promise<RecordMetadata[]> {
        const producer = await this.getProducer(options.producer);
        const payload = Array.isArray(messages) ? messages : [messages];
        const codec = this.resolveCodecOption(options.codec);
        return producer.send({
            ...(options.send as ProducerSendOptions | undefined),
            topic,
            messages: payload.map((message) => normalizeOutgoing(message, codec)) as Message[],
        });
    }

    private resolveCodecOption(codec: CodecInput | undefined): Codec {
        return codec === undefined ? this.codec : resolveCodec(codec);
    }

    /**
     * Guards against a client being connected after `disconnect()` already ran.
     * `disconnect()` bumps the generation, so a connection that was in flight at that
     * moment is torn down instead of being cached into a client that looks connected.
     */
    private async adoptIfCurrent<T>(generation: number, _client: T, discard: () => Promise<void>): Promise<void> {
        if (generation === this.generation) {
            return;
        }
        await discard();
        throw new KafkaConnectionError("Kafka client was disconnected while it was connecting");
    }

    private clientLevelProducerOptions(): Record<string, unknown> {
        const options: Record<string, unknown> = {};
        if (this.options.partitioner !== undefined) {
            options.partitioner = this.options.partitioner;
        }
        if (this.options.createPartitioner !== undefined) {
            options.createPartitioner = this.options.createPartitioner;
        }
        return options;
    }

    async subscribe(topic: string, options: ConsumeOptions = {}): Promise<Consumer> {
        const consumer = await this.getConsumer(options);
        await this.subscribeOnce(consumer, topic, options);
        return consumer;
    }

    private async subscribeOnce(consumer: Consumer, topic: string, options: ConsumeOptions): Promise<void> {
        const existing = this.subscriptions.get(topic);
        if (existing) {
            return existing;
        }

        // Track the in-flight promise, not just the finished state, so two concurrent
        // calls cannot both pass an empty check and subscribe the same topic twice.
        const pending = subscribeToTopic(consumer, topic, {
            ...(options as Record<string, unknown>),
            logger: this.logger,
        }).catch((error: unknown) => {
            this.subscriptions.delete(topic);
            throw error;
        });

        this.subscriptions.set(topic, pending);
        return pending;
    }

    async consume(
        topicOrHandler: string | MessageHandler,
        handlerOrOptions: MessageHandler | ConsumeOptions | undefined,
        maybeOptions: ConsumeOptions = {},
    ): Promise<Consumer> {
        let topic: string | undefined;
        let handler: MessageHandler | undefined;
        let options: ConsumeOptions;

        if (typeof topicOrHandler === "function") {
            handler = topicOrHandler;
            options = handlerOrOptions as ConsumeOptions ?? {};
        } else {
            topic = topicOrHandler;
            handler = handlerOrOptions as MessageHandler;
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

    registerShutdown(extra: ShutdownClients = {}): (signal: string) => Promise<void> {
        return registerShutdown(this.logger, {
            producer: () => this.producer ?? undefined,
            consumer: () => this.consumer ?? undefined,
            admin: () => this.admin ?? undefined,
            ...extra,
        });
    }

    async disconnect(): Promise<void> {
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
    async retry(): Promise<void> {
        if (this.closed) {
            throw new KafkaLifecycleError(
                "KafkaClient is closed; retry is not permitted after disconnect",
                { code: "KAFKA_LIFECYCLE_ERROR", operation: "retry", retryable: false },
            );
        }
        this.failedResources.clear();
        this.producerPending.value = null;
        this.consumerPending.value = null;
        this.adminPending.value = null;
    }

    /**
     * Returns the set of resources currently in FAILED state.
     */
    get failed(): ReadonlySet<"producer" | "consumer" | "admin"> {
        return this.failedResources;
    }
}

export function createKafkaClient(options: KafkaClientOptions = {}): KafkaClient {
    return new KafkaClient(options);
}

const CLIENT_ONLY_OPTION_KEYS = ["parseJson", "fromBeginning", "codec", "send", "exit"] as const;

interface Pending<T> {
    value: Promise<T> | null;
}

function resolveOnce<T>(pending: Pending<T>, factory: () => Promise<T>): Promise<T> {
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

function stripClientOnlyOptions(options: Record<string, unknown>): Record<string, unknown> {
    const rest = { ...options };
    for (const key of CLIENT_ONLY_OPTION_KEYS) {
        delete rest[key];
    }
    return rest;
}

function decodeIncoming(value: unknown, parseJson: boolean, codec: Codec): unknown {
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

function wrapHandler(
    handler: MessageHandler | undefined,
    logger: Logger,
    options: ConsumeOptions = {},
    codec: Codec,
): ((payload: EachMessagePayload) => Promise<void>) | undefined {
    if (!handler) {
        return undefined;
    }

    return async (payload: EachMessagePayload): Promise<void> => {
        const message = payload.message ?? {};
        let value: unknown = message.value;
        let key: unknown = message.key;

        try {
            value = decodeIncoming(value, options.parseJson !== false, codec);
            key = decodeIncoming(key, options.parseJson !== false, codec);
        } catch (error) {
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
        } as EachMessagePayload & { key: unknown; value: unknown });
    };
}

interface SendOptions {
    producer?: object;
    send?: ProducerSendOptions;
    codec?: CodecInput;
}

interface ConsumeOptions {
    parseJson?: boolean;
    fromBeginning?: boolean;
    groupId?: string;
    codec?: CodecInput;
}

type CodecInput = Parameters<typeof resolveCodec>[0];

type ProducerSendOptions = Omit<ProducerRecord, "topic" | "messages">;

type MessageHandler = (payload: EachMessagePayload) => unknown | Promise<unknown>;

interface ShutdownClients {
    producer?: Producer | null;
    consumer?: Consumer | null;
    admin?: Admin | null;
    clients?: Array<{ disconnect(): Promise<void> }>;
    exit?: boolean;
}