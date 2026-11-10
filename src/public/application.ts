import { createKafkaClient, type KafkaClient } from "../core/runtime.js";
import { ResourceRegistry } from "../core/resource-registry.js";
import { ResourceSlot } from "../core/resource-slot.js";
import type { LifecycleState } from "../core/lifecycle.js";
import { type KafkaClientOptions } from "../transport/kafkajs/adapter.js";
import { createProducer, type Producer } from "../transport/kafkajs/factory.js";
import { createAdmin, type Admin } from "../transport/kafkajs/admin.js";
import type { Consumer } from "../transport/kafkajs/consumer.js";
import { shutdownClient } from "../core/shutdown.js";
import { resolveCodec, type Codec } from "../adapters/codec.js";
import { KafkaConnectionError, KafkaConfigError, KafkaLifecycleError } from "../errors/index.js";
import { KafkaConsumer, type KafkaConsumerOptions, type MessageContext } from "../consumer/consumer.js";
import {
    composePublish,
    composeConsume,
    type PublishContext,
    type ConsumeContext,
    type PublishMiddleware,
    type ConsumeMiddleware,
    type ErrorMiddleware,
    type AdminMiddleware,
    tracingMiddleware,
    consumeTracingMiddleware,
} from "../middleware/index.js";
import { PluginManager } from "../plugins/manager.js";
import type { KafkaPlugin, PluginContext } from "../plugins/types.js";
import { RpcClient } from "../rpc/client.js";
import { RpcServer } from "../rpc/server.js";
import type { RpcRequestOptions, RpcServerOptions } from "../rpc/types.js";
import {
    HealthReporter,
    InMemoryMetricsCollector,
    createOpenTelemetryTracer,
    noopTracer,
    type Tracer,
} from "../observability/index.js";
import type { MetricsCollector } from "../observability/types.js";
import { runTransaction } from "../producer/transaction.js";
import { isValidator } from "../validation/index.js";
import type { KafkaTransaction } from "../producer/transaction.js";

export interface KafkaApplicationOptions extends KafkaClientOptions {}

/** Default graceful shutdown timeout in milliseconds (§7.7). */
const DEFAULT_GRACEFUL_TIMEOUT_MS = 15000;

export interface PublishOptions {
    codec?: Parameters<typeof resolveCodec>[0];
    send?: Record<string, unknown>;
    validator?: unknown;
}

export interface PublishResult {
    topic: string;
    partition?: number;
    offset?: string | null;
    timestamp?: string | null;
    raw?: unknown;
    /** Per-record metadata when the transport provides it (§8.4). */
    records?: Array<{
        partition?: number;
        offset?: string | null;
        timestamp?: string | null;
    }>;
}

export interface Subscription {
    topic: string;
    stop(): Promise<void>;
}

export interface HealthReport {
    state: "ready" | "closing" | "closed" | "failed";
    brokers: string[];
    connected: boolean;
}

export interface RuntimeStatus {
    state: "starting" | "ready" | "closing" | "closed" | "failed";
    resources: Array<{
        name: string;
        type: "producer" | "consumer" | "admin";
        state: string;
    }>;
    producers: number;
    consumers: number;
    subscriptions: number;
}

export class KafkaApplication<Topics = Record<string, unknown>> {
    private readonly client: KafkaClient;
    private closed = false;
    private started = false;
    private readonly producers = new Map<string, Producer>();
    private readonly consumers = new Map<string, KafkaConsumer>();
    private _admin: Admin | null = null;
    private readonly producerSlots = new Map<string, ResourceSlot<Producer>>();
    private readonly consumerSlots = new Map<string, ResourceSlot<KafkaConsumer>>();
    private readonly adminSlots = new Map<string, ResourceSlot<Admin>>();
    private readonly disposingResources = new WeakSet<object>();
    private readonly publishMiddleware: PublishMiddleware<unknown>[] = [];
    private readonly consumeMiddleware: ConsumeMiddleware<unknown>[] = [];
    private readonly errorMiddleware: ErrorMiddleware[] = [];
    private readonly adminMiddleware: AdminMiddleware[] = [];
    private readonly codecs = new Map<string, Codec>();
    private readonly metricCollectors = new Array<MetricsCollector>();
    private pluginManager: PluginManager | null = null;
    private rpcClient: RpcClient | null = null;
    private readonly rpcServers = new Map<string, RpcServer>();
    private readonly metricsCollector = new InMemoryMetricsCollector();
    private healthReporter: HealthReporter | null = null;
    private tracer: Tracer = noopTracer();
    private readonly healthChecks = new Map<string, () => Promise<{ ok: boolean; detail?: string }>>();
    private readonly diagnosticProviders = new Array<() => Record<string, unknown>>();
    private readonly stoppedSubscriptions = new Set<string>();
    private shutdownStartTime: number | null = null;
    private readonly registry = new ResourceRegistry();

    constructor(options: KafkaApplicationOptions = {}) {
        this.client = createKafkaClient(options);
        this.validate();

        const context: PluginContext = {
            registerPublishMiddleware: (middleware) => {
                this.publishMiddleware.push(middleware as PublishMiddleware<unknown>);
                return () => {
                    const idx = this.publishMiddleware.indexOf(
                        middleware as PublishMiddleware<unknown>,
                    );
                    if (idx >= 0) {
                        this.publishMiddleware.splice(idx, 1);
                    }
                };
            },
            registerConsumeMiddleware: (middleware) => {
                this.consumeMiddleware.push(middleware as ConsumeMiddleware<unknown>);
                return () => {
                    const idx = this.consumeMiddleware.indexOf(
                        middleware as ConsumeMiddleware<unknown>,
                    );
                    if (idx >= 0) {
                        this.consumeMiddleware.splice(idx, 1);
                    }
                };
            },
            registerCodec: (name, codec) => {
                this.codecs.set(name, codec);
                return () => {
                    this.codecs.delete(name);
                };
            },
            registerHealthCheck: (name, check) => {
                this.healthChecks.set(name, check);
                return () => {
                    this.healthChecks.delete(name);
                };
            },
            registerMetricCollector: (collector) => {
                this.metricCollectors.push(collector);
                return () => {
                    const idx = this.metricCollectors.indexOf(collector);
                    if (idx >= 0) {
                        this.metricCollectors.splice(idx, 1);
                    }
                };
            },
            registerDiagnostics: (provider) => {
                this.diagnosticProviders.push(provider);
                return () => {
                    const idx = this.diagnosticProviders.indexOf(provider);
                    if (idx >= 0) {
                        this.diagnosticProviders.splice(idx, 1);
                    }
                };
            },
            on: (event, handler) => {
                if (event === "ready") {
                    this.readyHandlers.push(handler);
                } else {
                    this.closeHandlers.push(handler);
                }
                return () => {
                    const handlers =
                        event === "ready" ? this.readyHandlers : this.closeHandlers;
                    const idx = handlers.indexOf(handler);
                    if (idx >= 0) {
                        handlers.splice(idx, 1);
                    }
                };
            },
        };
        this.pluginManager = new PluginManager(context);
        this.healthReporter = new HealthReporter(this as unknown as KafkaApplication);

        const tracing = (options as Record<string, unknown>).tracing as
            | { tracer?: Tracer; serviceName?: string }
            | undefined;
        if (tracing?.tracer) {
            this.tracer = tracing.tracer;
        } else if (tracing?.serviceName) {
            this.tracer = createOpenTelemetryTracer(tracing.serviceName);
        }

        // Auto-register tracing middleware when a tracer is configured (§16.3).
        if (this.tracer !== noopTracer()) {
            this.useConsume(consumeTracingMiddleware(this.tracer));
            this.use(tracingMiddleware(this.tracer));
        }
    }

    private readonly readyHandlers: Array<() => unknown | Promise<unknown>> = [];
    private readonly closeHandlers: Array<() => unknown | Promise<unknown>> = [];

    /**
     * Validate configuration before any network connection is attempted (§5.3).
     */
    private validate(): void {
        const brokers = this.client.options.brokers;
        if (!brokers) {
            throw new KafkaConfigError(
                "brokers is required",
                { code: "KAFKA_CONFIG_ERROR", operation: "validate", resource: "brokers", path: "brokers" },
            );
        }

        const brokerList = Array.isArray(brokers) ? brokers : [brokers];
        if (brokerList.length === 0 || brokerList.some((b) => !String(b).trim())) {
            throw new KafkaConfigError(
                "brokers must be a non-empty list",
                { code: "KAFKA_CONFIG_ERROR", operation: "validate", resource: "brokers", path: "brokers" },
            );
        }

        const opts = this.client.options as Record<string, unknown>;

        const checkNonNegative = (name: string, value: unknown): void => {
            if (value === undefined || value === null) return;
            const num = Number(value);
            if (Number.isNaN(num) || num < 0) {
                throw new KafkaConfigError(
                    `${name} must be a non-negative number`,
                    { code: "KAFKA_CONFIG_ERROR", operation: "validate", resource: name, path: name },
                );
            }
        };

        checkNonNegative("connectionTimeout", opts.connectionTimeout);
        checkNonNegative("requestTimeout", opts.requestTimeout);
        checkNonNegative("authenticationTimeout", opts.authenticationTimeout);

        const retry = opts.retry as Record<string, unknown> | undefined;
        if (retry && typeof retry === "object") {
            checkNonNegative("retry.retries", retry.retries);
            checkNonNegative("retry.initialRetryTime", retry.initialRetryTime);
            checkNonNegative("retry.maxRetryTime", retry.maxRetryTime);
        }

        const logLevel = opts.logLevel;
        if (logLevel !== undefined && typeof logLevel === "string") {
            const validLevels = ["debug", "info", "warn", "error", "nothing", "none"];
            if (!validLevels.includes(String(logLevel).toLowerCase())) {
                throw new KafkaConfigError(
                    `logLevel "${logLevel}" is not supported`,
                    { code: "KAFKA_CONFIG_ERROR", operation: "validate", resource: "logLevel", path: "logLevel" },
                );
            }
        }

        const sasl = opts.sasl as Record<string, unknown> | undefined;
        if (sasl && typeof sasl === "object" && "mechanism" in sasl && sasl.mechanism) {
            const supported = ["plain", "scram-sha-256", "scram-sha-512", "aws", "oauthbearer"];
            const mechanism = String(sasl.mechanism).toLowerCase();
            if (!supported.includes(mechanism)) {
                throw new KafkaConfigError(
                    `sasl.mechanism is not supported`,
                    { code: "KAFKA_CONFIG_ERROR", operation: "validate", resource: "sasl.mechanism", path: "sasl.mechanism" },
                );
            }
        }

        // §17 / §5.3: Validate TLS and SASL combinations map to valid
        // KafkaJS configuration. OAuth bearer requires TLS in practice.
        const ssl = opts.ssl as Record<string, unknown> | undefined;
        if (sasl && typeof sasl === "object" && sasl.mechanism === "oauthbearer") {
            if (!ssl || (typeof ssl === "object" && Object.keys(ssl).length === 0)) {
                throw new KafkaConfigError(
                    "sasl.mechanism oauthbearer requires TLS (ssl) to be configured",
                    { code: "KAFKA_CONFIG_ERROR", operation: "validate", resource: "sasl.mechanism", path: "sasl.mechanism" },
                );
            }
        }

        // §5.3: Mutually incompatible producer options must produce
        // actionable errors.
        const producer = opts.producer as Record<string, unknown> | undefined;
        if (producer && typeof producer === "object") {
            const idempotent = producer.idempotent;
            const transactionalId = producer.transactionalId;
            if (idempotent === true && transactionalId !== undefined) {
                throw new KafkaConfigError(
                    "producer.idempotent cannot be combined with producer.transactionalId",
                    { code: "KAFKA_CONFIG_ERROR", operation: "validate", resource: "producer", path: "producer" },
                );
            }
            const acks = producer.acks;
            if (idempotent === true && acks !== undefined && String(acks) !== "all") {
                throw new KafkaConfigError(
                    "producer.idempotent requires producer.acks=all",
                    { code: "KAFKA_CONFIG_ERROR", operation: "validate", resource: "producer", path: "producer.acks" },
                );
            }
        }

        // §4.1: A consumer must have a valid groupId. Do not invent a
        // group ID silently. groupId may be hoisted from the consumer
        // namespace to the top level by resolveKafkaOptions.
        const groupId = opts.groupId as string | undefined;
        const hasConsumerConfig = opts.consumer !== undefined || groupId !== undefined;
        if (hasConsumerConfig && (!groupId || typeof groupId !== "string" || String(groupId).trim() === "")) {
            throw new KafkaConfigError(
                "consumer.groupId is required when creating a consumer",
                { code: "KAFKA_CONFIG_ERROR", operation: "validate", resource: "consumer.groupId", path: "consumer.groupId" },
            );
        }

        // §5.3: Reject unknown package-owned keys that are not part of
        // the documented configuration schema. KafkaJS pass-through keys
        // supplied through the kafkaJS namespace are allowed.
        const KNOWN_PACKAGE_KEYS = new Set([
            "brokers", "clientId", "groupId", "codec", "logger",
            "connection", "producer", "consumer", "admin",
            "serialization", "logging", "shutdown", "kafkaJS",
            "kafka", "credential", "tracing",
        ]);
        const KNOWN_KAFKAJS_TOPLEVEL_KEYS = new Set([
            "brokers", "clientId", "ssl", "sasl", "retry", "logLevel",
            "connectionTimeout", "requestTimeout", "authenticationTimeout",
            "partitioner", "createPartitioner", "logCreator", "topicId",
        ]);
        const allowedKeys = new Set([...KNOWN_PACKAGE_KEYS, ...KNOWN_KAFKAJS_TOPLEVEL_KEYS]);
        for (const key of Object.keys(opts)) {
            if (!allowedKeys.has(key)) {
                throw new KafkaConfigError(
                    `Unknown configuration key "${key}"`,
                    { code: "KAFKA_CONFIG_ERROR", operation: "validate", resource: key, path: key },
                );
            }
        }
    }

    /**
     * Register publish middleware. Runs in registration order for the
     * inbound path and reverse order on unwind (§11.3).
     */
    use(middleware: PublishMiddleware<unknown>): this {
        this.publishMiddleware.push(middleware);
        return this;
    }

    /**
     * Register consume middleware. Runs in registration order for the
     * inbound path and reverse order on unwind (§11.3).
     */
    useConsume(middleware: ConsumeMiddleware<unknown>): this {
        this.consumeMiddleware.push(middleware);
        return this;
    }

    /**
     * Register error middleware (§11.1). Registered callbacks are invoked
     * when a publish or consume pipeline rejects.
     */
    useError(middleware: ErrorMiddleware): this {
        this.errorMiddleware.push(middleware);
        return this;
    }

    /**
     * Register admin middleware (§11.1). Runs in registration order for the
     * inbound path and reverse order on unwind.
     */
    useAdmin(middleware: AdminMiddleware): this {
        this.adminMiddleware.push(middleware);
        return this;
    }

    /**
     * Register a named codec for use by the application and plugins (§12.1).
     */
    registerCodec(name: string, codec: Codec): void {
        this.codecs.set(name, codec);
    }

    /**
     * Install a plugin. Plugins are installed in declaration order and their
     * onReady hooks run when the application starts (§15).
     */
    async install(plugin: KafkaPlugin): Promise<void> {
        await this.pluginManager!.install(plugin);
    }

    /**
     * Start the application: run plugin onReady hooks (§15.2) and mark ready.
     * Resources are still created lazily on first use.
     */
    async start(): Promise<void> {
        if (this.started) {
            return;
        }
        this.ensureOpen();
        await this.pluginManager!.ready();
        for (const handler of this.readyHandlers) {
            await handler();
        }
        this.started = true;
    }

    private getRpcClient(): RpcClient {
        if (!this.rpcClient) {
            this.rpcClient = new RpcClient(this.client.kafka, {
                defaultTimeoutMs: 30000,
            });
        }
        return this.rpcClient;
    }

    async publish<K extends keyof Topics>(
        topic: K,
        value: Topics[K],
        options: PublishOptions = {},
    ): Promise<PublishResult> {
        return this.publishBatch([value], topic, options);
    }

    async publishMany<K extends keyof Topics>(
        topic: K,
        values: readonly Topics[K][],
        options: PublishOptions = {},
    ): Promise<PublishResult> {
        return this.publishBatch(values, topic, options);
    }

    private async publishBatch<K extends keyof Topics>(
        values: readonly Topics[K][],
        topic: K,
        options: PublishOptions,
    ): Promise<PublishResult> {
        this.ensureOpen();
        const codec = options.codec === undefined ? undefined : resolveCodec(options.codec);
        const validator = isValidator(options.validator) ? options.validator : undefined;
        const validatedValues = validator
            ? await Promise.all(values.map((value) => validator.parse(value)))
            : values;

        const context: PublishContext<readonly Topics[K][]> = {
            topic: topic as string,
            value: validatedValues as readonly Topics[K][],
            headers: {},
            metadata: {},
        };

        const transport = async (ctx: PublishContext<readonly Topics[K][]>): Promise<PublishResult> => {
            const metadata = await this.client.send(ctx.topic, ctx.value, { codec, send: options.send });
            const first = metadata[0];
            return {
                topic: ctx.topic,
                partition: first?.partition ?? undefined,
                offset: first?.offset ?? null,
                timestamp: first?.timestamp ?? null,
                records: metadata.map((entry) => ({
                    partition: entry.partition,
                    offset: entry.offset ?? null,
                    timestamp: entry.timestamp ?? null,
                })),
                raw: metadata,
            };
        };

        const run = composePublish(this.publishMiddleware, transport);
        return this.runWithErrorMiddleware(run(context) as Promise<PublishResult>, context.topic);
    }

    /**
     * Runs a promise through the registered error middleware chain (§11.1).
     * Middleware can transform the error; failures inside middleware are
     * logged but do not prevent the original error from propagating.
     */
    private async runWithErrorMiddleware<T>(
        promise: Promise<T>,
        topic: string,
    ): Promise<T> {
        try {
            return await promise;
        } catch (error) {
            let currentError = error;
            for (const middleware of this.errorMiddleware) {
                try {
                    const result = await middleware(currentError, { topic });
                    if (result instanceof Error) {
                        currentError = result;
                    }
                } catch (middlewareError) {
                    this.client.logger.error("Error middleware failed", middlewareError);
                }
            }
            throw currentError;
        }
    }

    /**
     * Send an RPC request and wait for the response (§14).
     */
    async request<T = unknown, R = unknown>(
        topic: string,
        payload: T,
        options: RpcRequestOptions = {},
    ): Promise<R> {
        this.ensureOpen();
        const client = this.getRpcClient();
        return client.request(topic, payload, options);
    }

    /**
     * Register an RPC request handler and start consumption (§14).
     */
    async respond<T = unknown, R = unknown>(
        topic: string,
        handler: (request: T, context: { correlationId: string; requestTopic: string; headers: Record<string, unknown> }) => R | Promise<R>,
        options: RpcServerOptions = {},
    ): Promise<void> {
        this.ensureOpen();
        const server = new RpcServer<T, R>(this.client.kafka, options);
        await server.respond(topic, handler, options);
        this.rpcServers.set(topic, server as RpcServer);
    }

    /**
     * Run a transactional block (§8.6). The callback receives a tx with
     * publish/publishMany/commit/abort; commit is automatic on success and
     * abort on failure. Both primary and abort errors are preserved.
     */
    async transaction<T = unknown>(
        callback: (tx: KafkaTransaction) => Promise<T>,
        options: { transactionalId?: string; codec?: Codec | undefined } = {},
    ): Promise<T> {
        this.ensureOpen();
        return runTransaction(this.client.kafka, callback, {
            codec: options.codec,
            transactionalId: options.transactionalId,
        });
    }


    async subscribe<K extends keyof Topics>(
        topic: K,
        handler: (payload: MessageContext<Topics[K]>) => unknown,
        options: Record<string, unknown> = {},
    ): Promise<Subscription> {
        this.ensureOpen();

        const transport = async (payload: ConsumeContext<Topics[K]>): Promise<void> => {
            await handler(payload as unknown as MessageContext<Topics[K]>);
        };

        const run = composeConsume(this.consumeMiddleware, transport);

        const validator = isValidator(options.validator) ? options.validator : undefined;

        await this.client.consume(
            topic as string,
            (payload) => {
                // Skip processing if this subscription has been stopped (§4.4).
                if (this.stoppedSubscriptions.has(topic as string)) {
                    return;
                }
                const message = payload.message ?? ({} as Record<string, unknown>);
                const rawValue = message.value as unknown;
                const value = validator ? validator.parse(rawValue) : rawValue;
                const headers = (message.headers ?? {}) as Record<string, string | Buffer | undefined>;
                const correlationId = headers["correlation-id"];
                const messageId = headers["message-id"];
                const contentType = headers["content-type"];
                const schemaVersion = headers["schema-version"];
                const controller = new AbortController();
                const ctx: ConsumeContext<Topics[K]> = {
                    topic: payload.topic,
                    value: (value as unknown) as Topics[K],
                    key: message.key,
                    partition: payload.partition ?? 0,
                    offset: (message.offset ?? "") as string,
                    timestamp: (message.timestamp ?? "") as string,
                    headers,
                    signal: controller.signal,
                    metadata: {
                        correlationId: typeof correlationId === "string" ? correlationId : String(correlationId ?? ""),
                        messageId: typeof messageId === "string" ? messageId : String(messageId ?? ""),
                        contentType: typeof contentType === "string" ? contentType : String(contentType ?? ""),
                        schemaVersion: typeof schemaVersion === "string" ? schemaVersion : String(schemaVersion ?? ""),
                    },
                };
                return run(ctx);
            },
            options,
        );

        return {
            topic: topic as string,
            stop: async () => {
                // Stop this specific subscription by marking it stopped so the
                // shared consumer skips messages for this topic (§4.4).
                this.stoppedSubscriptions.add(topic as string);
            },
        };
    }

    async producer(...args: unknown[]): Promise<Producer> {
        const { name, options } = this.resolveProducerArgs(args);
        this.ensureOpen();

        let slot = this.producerSlots.get(name);
        if (!slot) {
            slot = new ResourceSlot<Producer>();
            this.producerSlots.set(name, slot);
        }

        return slot.get(async (_generation) => {
            const producer = await createProducer(this.client.kafka, {
                ...options,
                logger: this.client.logger,
            });
            if (!this.producers.has(name)) {
                this.producers.set(name, producer);
                this.registry.register({
                    id: `producer:${name}`,
                    name,
                    type: "producer",
                    state: "connected" as LifecycleState,
                });
            }
            return producer;
        });
    }

    async consumer(...args: unknown[]): Promise<KafkaConsumer> {
        const { name, groupId, options } = this.resolveConsumerArgs(args);
        this.ensureOpen();

        let slot = this.consumerSlots.get(name);
        if (!slot) {
            slot = new ResourceSlot<KafkaConsumer>();
            this.consumerSlots.set(name, slot);
        }

        return slot.get(async (_generation) => {
            const consumer = new KafkaConsumer(groupId, this.client.kafka, {
                ...options,
                logger: this.client.logger,
            });
            if (!this.consumers.has(name)) {
                this.consumers.set(name, consumer);
                this.registry.register({
                    id: `consumer:${name}`,
                    name,
                    type: "consumer",
                    state: "connected" as LifecycleState,
                });
            }
            return consumer;
        });
    }

    async connectAdmin(options: Record<string, unknown> = {}): Promise<Admin> {
        this.ensureOpen();

        let slot = this.adminSlots.get("admin");
        if (!slot) {
            slot = new ResourceSlot<Admin>();
            this.adminSlots.set("admin", slot);
        }

        return slot.get(async (_generation) => {
            const admin = await createAdmin(this.client.kafka, {
                ...options,
                logger: this.client.logger,
            });
            if (!this._admin) {
                this._admin = admin;
                this.registry.register({
                    id: "admin",
                    name: "admin",
                    type: "admin",
                    state: "connected" as LifecycleState,
                });
            }
            return admin;
        });
    }

    get admin(): Admin {
        if (this._admin) {
            return this._admin;
        }
        throw new KafkaConnectionError("Admin client is not connected. Call connectAdmin() first.");
    }

    /**
     * Runs an async operation, suppressing any error. Used during shutdown so
     * that one resource's failure does not prevent others from being closed
     * (§7.7).
     */
    private static async safe(promise: Promise<unknown>): Promise<void> {
        try {
            await promise;
        } catch {
            // non-fatal
        }
    }

    private resolveProducerArgs(args: unknown[]): { name: string; options: Record<string, unknown> } {
        let name = "default";
        let options: Record<string, unknown> = {};
        if (args.length > 0) {
            const first = args[0];
            if (typeof first === "string") {
                name = first;
                options = (args[1] as Record<string, unknown>) ?? {};
            } else if (first && typeof first === "object") {
                const typed = first as Record<string, unknown>;
                name = String(typed.name ?? typed.resource ?? "default");
                options = typed;
            }
        }
        return { name, options };
    }

    private resolveConsumerArgs(args: unknown[]): {
        name: string;
        groupId: string;
        options: KafkaConsumerOptions;
    } {
        let name = "default";
        let groupId: string | undefined = this.client.options.groupId ?? undefined;
        let options: KafkaConsumerOptions = {};
        if (args.length > 0) {
            const first = args[0];
            if (typeof first === "string") {
                name = first;
                const second = args[1];
                if (typeof second === "string") {
                    groupId = second;
                    options = (args[2] as KafkaConsumerOptions) ?? {};
                } else if (second && typeof second === "object") {
                    options = second as KafkaConsumerOptions;
                    groupId = (options.groupId as string | undefined) ?? groupId;
                }
            } else if (first && typeof first === "object") {
                const typed = first as Record<string, unknown>;
                options = typed as KafkaConsumerOptions;
                name = String(typed.name ?? typed.resource ?? typed.label ?? "default");
                groupId = (typed.groupId as string | undefined) ?? (typed.group as string | undefined) ?? groupId;
            }
        }
        if (!groupId) {
            throw new KafkaConfigError("consumer groupId is required", { path: "consumer.groupId" });
        }
        return { name, groupId, options };
    }

    async health(): Promise<HealthReport> {

        this.ensureOpen();
        this.metricsCollector.gauge("kafka.producers", this.producers.size);
        this.metricsCollector.gauge("kafka.consumers", this.consumers.size);
        this.metricsCollector.gauge("kafka.pendingRequests", this.rpcClient?.pendingRequests ?? 0);
        return this.healthReport();
    }

    private healthReport() {
        const brokers = this.client.options.brokers
            ? Array.isArray(this.client.options.brokers)
                ? this.client.options.brokers
                : [this.client.options.brokers]
            : [];
        return {
            state: (this.closed ? "closed" : "ready") as HealthReport["state"],
            brokers,
            connected: this.client.connected.producer || this.client.connected.consumer || this.client.connected.admin,
        };
    }

    /**
     * Liveness: is the runtime process and internal state functioning? (§16.2)
     */
    liveness() {
        return this.healthReporter!.liveness();
    }

    /**
     * Readiness: can this application perform the required Kafka operations? (§16.2)
     */
    async readiness() {
        return this.healthReporter!.readiness();
    }

    /**
     * Diagnostics snapshot (§16.4).
     */
    diagnostics() {
        return this.healthReporter!.diagnostics();
    }

    /**
     * Current metrics snapshot (§16.1).
     */
    metrics() {
        this.recordConsumerLag();
        const merged = new InMemoryMetricsCollector();
        merged.merge(this.metricsCollector);
        for (const collector of this.metricCollectors) {
            merged.merge(collector);
        }
        return merged.snapshot();
    }

    /**
     * Record consumer lag gauges for all managed consumers (§16.1).
     */
    private recordConsumerLag(): void {
        for (const [name, consumer] of this.consumers) {
            const lag = consumer.getLag();
            for (const [topic, byPartition] of lag) {
                for (const [partition, lagValue] of byPartition) {
                    this.metricsCollector.gauge(
                        "kafka.consumer.lag",
                        lagValue,
                        { topic, partition: String(partition), consumer: String(name) },
                    );
                }
            }
        }
    }

    /**
     * Wait for pending producer work where the transport supports flushing.
     */
    async flush(options: Record<string, unknown> = {}): Promise<void> {
        this.ensureOpen();
        const producers = [...this.producers.values()];
        await Promise.allSettled(
            producers.map(async (producer) => {
                const candidate = producer as { flush?: (opts?: unknown) => Promise<void> | void };
                if (typeof candidate.flush === "function") {
                    await candidate.flush(options);
                }
            }),
        );
    }

    /**
     * Runtime status snapshot (§16.4).
     */
    status(): RuntimeStatus {
        const state: RuntimeStatus["state"] = this.closed ? "closed" : (this.started ? "ready" : "starting");
        const resources: RuntimeStatus["resources"] = [];
        for (const [name] of this.producers) {
            resources.push({ name: String(name), type: "producer", state: "connected" });
        }
        for (const [name, consumer] of this.consumers) {
            resources.push({ name: String(name), type: "consumer", state: (consumer as { isRunning?: boolean }).isRunning ? "running" : "connected" });
        }
        if (this._admin) {
            resources.push({ name: "admin", type: "admin", state: "connected" });
        }
        return {
            state,
            resources,
            producers: this.producers.size,
            consumers: this.consumers.size,
            subscriptions: 0,
        };
    }

    async close(): Promise<void> {
        if (this.closed) {
            return;
        }
        // §7.7 step 1: mark the application as closing; reject new work.
        this.closed = true;
        this.shutdownStartTime = Date.now();

        const shutdown = (this.client.options as Record<string, unknown>).shutdown as
            | { graceful?: boolean; timeout?: number }
            | undefined;
        const gracefulTimeout = shutdown?.timeout ?? DEFAULT_GRACEFUL_TIMEOUT_MS;

        // §7.7 step 2: stop consumer intake (halt, do not disconnect yet).
        for (const consumer of this.consumers.values()) {
            await KafkaApplication.safe(consumer.stop());
        }

        // §7.7 step 3: wait for in-flight handlers up to the configured deadline.
        await Promise.all(
            [...this.consumers.values()].map((consumer) => consumer.awaitInFlight(gracefulTimeout)),
        );

        // §7.7 step 4: commit offsets according to the selected processing policy.
        if (shutdown?.graceful !== false) {
            for (const consumer of this.consumers.values()) {
                await KafkaApplication.safe(consumer.commitOffsets());
            }
        }

        // §7.7 step 5: flush pending producer work, then close producer slots
        // (disconnect transports).
        for (const producer of this.producers.values()) {
            const candidate = producer as { flush?: (opts?: unknown) => Promise<void> | void };
            if (typeof candidate.flush === "function") {
                await KafkaApplication.safe(Promise.resolve(candidate.flush({})));
            }
        }
        await Promise.allSettled(
            [...this.producerSlots.values()].map(async (slot) => {
                await slot.close(async (producer) => {
                    if (!producer) {
                        return;
                    }
                    if (!this.disposingResources.has(producer)) {
                        this.disposingResources.add(producer);
                        await shutdownClient(this.client.logger, producer, "producer");
                    }
                });
            }),
        );

        // §7.7 step 6: close consumer slots (disconnect transports).
        await Promise.allSettled(
            [...this.consumerSlots.values()].map(async (slot) => {
                await slot.close(async (consumer) => {
                    if (!consumer) {
                        return;
                    }
                    if (!this.disposingResources.has(consumer)) {
                        this.disposingResources.add(consumer);
                        try {
                            await consumer.stop();
                        } catch {
                            // Ignore stop failures during shutdown.
                        }
                        await consumer.close();
                    }
                });
            }),
        );

        // §7.7 step 7: close admin slot.
        await Promise.allSettled(
            [...this.adminSlots.values()].map(async (slot) => {
                await slot.close(async (admin) => {
                    if (!admin) {
                        return;
                    }
                    if (!this.disposingResources.has(admin)) {
                        this.disposingResources.add(admin);
                        await shutdownClient(this.client.logger, admin, "admin");
                    }
                });
            }),
        );

        // RPC servers.
        for (const [, server] of this.rpcServers) {
            await KafkaApplication.safe(server.close());
        }
        await KafkaApplication.safe(Promise.resolve(this.rpcClient?.close()));

        // §7.7 step 8: run plugin shutdown hooks.
        await KafkaApplication.safe(this.pluginManager!.close());
        for (const handler of this.closeHandlers) {
            await KafkaApplication.safe(Promise.resolve(handler()));
        }

        // §7.7 step 9: disconnect remaining KafkaJS clients.
        await KafkaApplication.safe(this.client.disconnect());

        // §7.7 step 10: emit final diagnostics and mark the application closed.
        this.producers.clear();
        this.consumers.clear();
        this.stoppedSubscriptions.clear();
        this._admin = null;
        this.producerSlots.clear();
        this.consumerSlots.clear();
        this.adminSlots.clear();
        this.registry.clear();

        // §16.1: record shutdown duration metric.
        if (this.shutdownStartTime !== null) {
            this.metricsCollector.timing("kafka.shutdown.durationMs", Date.now() - this.shutdownStartTime);
        }
    }

    private ensureOpen(): void {
        if (this.closed) {
            throw new KafkaConnectionError("Kafka application is closed");
        }
    }

    /**
     * Advanced escape hatch (§4.5). Returns the exact underlying KafkaJS
     * transport instances for resources that have already been created.
     * Direct calls bypass package middleware, metrics, serialization, and
     * lifecycle policies. The package still owns disconnecting these
     * resources on `close()`.
     */
    get raw() {
        return {
            producer: (name = "default"): Producer | null =>
                this.producers.get(name) ?? null,
            consumer: (name = "default"): Consumer | null =>
                this.consumers.get(name)?.raw ?? null,
            admin: (): Admin | null => this._admin ?? null,
        };
    }

    get isClosed(): boolean {
        return this.closed;
    }

    /** Whether the application has been started. */
    get isStarted(): boolean {
        return this.started;
    }

    /** Configured broker list. */
    get brokers(): string[] {
        const brokers = this.client.options.brokers;
        return brokers
            ? Array.isArray(brokers)
                ? brokers
                : [brokers]
            : [];
    }

    /** Connection status for each resource type. */
    get connectionStatus(): { producer: boolean; consumer: boolean; admin: boolean } {
        return { ...this.client.connected };
    }

    /**
     * Diagnostic resource summaries (name, type, state) for all managed
     * resources. Exposed so HealthReporter can report without accessing
     * private members.
     */
    get diagnosticResources(): Array<{
        name: string;
        type: "producer" | "consumer" | "admin";
        state: string;
    }> {
        const resources: Array<{
            name: string;
            type: "producer" | "consumer" | "admin";
            state: string;
        }> = [];
        for (const [name] of this.producers) {
            resources.push({ name: String(name), type: "producer", state: "connected" });
        }
        for (const [name, consumer] of this.consumers) {
            resources.push({
                name: String(name),
                type: "consumer",
                state: (consumer as { isRunning?: boolean }).isRunning ? "running" : "connected",
            });
        }
        if (this._admin) {
            resources.push({ name: "admin", type: "admin", state: "connected" });
        }
        return resources;
    }

    /** Read-only view of registered health checks. */
    get registeredHealthChecks(): readonly [string, () => Promise<{ ok: boolean; detail?: string }>][] {
        return [...this.healthChecks];
    }

    /** Read-only view of registered diagnostic providers. */
    get registeredDiagnosticProviders(): readonly (() => Record<string, unknown>)[] {
        return [...this.diagnosticProviders];
    }

    /**
     * Register a health check. Public so HealthReporter and plugins can
     * contribute without accessing private members.
     */
    registerHealthCheck(
        name: string,
        check: () => Promise<{ ok: boolean; detail?: string }>,
    ): void {
        this.healthChecks.set(name, check);
    }

    /**
     * Register a diagnostics provider. Public so HealthReporter and plugins
     * can contribute without accessing private members.
     */
    registerDiagnostics(provider: () => Record<string, unknown>): void {
        this.diagnosticProviders.push(provider);
    }

    /**
     * Returns the application's tracer for distributed tracing (§16.3).
     * Defaults to a no-op tracer unless a tracing option was supplied.
     */
    trace(): Tracer {
        return this.tracer;
    }
}

export function createApplication<Topics = Record<string, unknown>>(
    options: KafkaApplicationOptions = {},
): KafkaApplication<Topics> {
    return new KafkaApplication<Topics>(options);
}

/**
 * Builds a new `KafkaApplication` from legacy `KafkaClientOptions` (§21).
 *
 * This is the migration adapter: legacy callers can move from
 * `createKafkaClient()` to `Kafka.create()` / `createApplication()` without
 * maintaining two separate implementations. Both façades share the same
 * `src/client/` KafkaJS adapter runtime.
 */
export function createApplicationFromLegacyOptions(
    options: KafkaClientOptions = {},
): KafkaApplication {
    return new KafkaApplication(options as KafkaApplicationOptions);
}
