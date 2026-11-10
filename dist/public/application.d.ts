import { type KafkaClientOptions } from "../transport/kafkajs/adapter.js";
import { type Producer } from "../transport/kafkajs/factory.js";
import { type Admin } from "../transport/kafkajs/admin.js";
import type { Consumer } from "../transport/kafkajs/consumer.js";
import { resolveCodec, type Codec } from "../adapters/codec.js";
import { KafkaConsumer, type MessageContext } from "../consumer/consumer.js";
import { type PublishMiddleware, type ConsumeMiddleware, type ErrorMiddleware, type AdminMiddleware } from "../middleware/index.js";
import type { KafkaPlugin } from "../plugins/types.js";
import type { RpcRequestOptions, RpcServerOptions } from "../rpc/types.js";
import { type Tracer } from "../observability/index.js";
import type { KafkaTransaction } from "../producer/transaction.js";
export interface KafkaApplicationOptions extends KafkaClientOptions {
}
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
export declare class KafkaApplication<Topics = Record<string, unknown>> {
    private readonly client;
    private closed;
    private started;
    private readonly producers;
    private readonly consumers;
    private _admin;
    private readonly producerSlots;
    private readonly consumerSlots;
    private readonly adminSlots;
    private readonly disposingResources;
    private readonly publishMiddleware;
    private readonly consumeMiddleware;
    private readonly errorMiddleware;
    private readonly adminMiddleware;
    private readonly codecs;
    private readonly metricCollectors;
    private pluginManager;
    private rpcClient;
    private readonly rpcServers;
    private readonly metricsCollector;
    private healthReporter;
    private tracer;
    private readonly healthChecks;
    private readonly diagnosticProviders;
    private readonly stoppedSubscriptions;
    private shutdownStartTime;
    private readonly registry;
    constructor(options?: KafkaApplicationOptions);
    private readonly readyHandlers;
    private readonly closeHandlers;
    /**
     * Validate configuration before any network connection is attempted (§5.3).
     */
    private validate;
    /**
     * Register publish middleware. Runs in registration order for the
     * inbound path and reverse order on unwind (§11.3).
     */
    use(middleware: PublishMiddleware<unknown>): this;
    /**
     * Register consume middleware. Runs in registration order for the
     * inbound path and reverse order on unwind (§11.3).
     */
    useConsume(middleware: ConsumeMiddleware<unknown>): this;
    /**
     * Register error middleware (§11.1). Registered callbacks are invoked
     * when a publish or consume pipeline rejects.
     */
    useError(middleware: ErrorMiddleware): this;
    /**
     * Register admin middleware (§11.1). Runs in registration order for the
     * inbound path and reverse order on unwind.
     */
    useAdmin(middleware: AdminMiddleware): this;
    /**
     * Register a named codec for use by the application and plugins (§12.1).
     */
    registerCodec(name: string, codec: Codec): void;
    /**
     * Install a plugin. Plugins are installed in declaration order and their
     * onReady hooks run when the application starts (§15).
     */
    install(plugin: KafkaPlugin): Promise<void>;
    /**
     * Start the application: run plugin onReady hooks (§15.2) and mark ready.
     * Resources are still created lazily on first use.
     */
    start(): Promise<void>;
    private getRpcClient;
    publish<K extends keyof Topics>(topic: K, value: Topics[K], options?: PublishOptions): Promise<PublishResult>;
    publishMany<K extends keyof Topics>(topic: K, values: readonly Topics[K][], options?: PublishOptions): Promise<PublishResult>;
    private publishBatch;
    /**
     * Runs a promise through the registered error middleware chain (§11.1).
     * Middleware can transform the error; failures inside middleware are
     * logged but do not prevent the original error from propagating.
     */
    private runWithErrorMiddleware;
    /**
     * Send an RPC request and wait for the response (§14).
     */
    request<T = unknown, R = unknown>(topic: string, payload: T, options?: RpcRequestOptions): Promise<R>;
    /**
     * Register an RPC request handler and start consumption (§14).
     */
    respond<T = unknown, R = unknown>(topic: string, handler: (request: T, context: {
        correlationId: string;
        requestTopic: string;
        headers: Record<string, unknown>;
    }) => R | Promise<R>, options?: RpcServerOptions): Promise<void>;
    /**
     * Run a transactional block (§8.6). The callback receives a tx with
     * publish/publishMany/commit/abort; commit is automatic on success and
     * abort on failure. Both primary and abort errors are preserved.
     */
    transaction<T = unknown>(callback: (tx: KafkaTransaction) => Promise<T>, options?: {
        transactionalId?: string;
        codec?: Codec | undefined;
    }): Promise<T>;
    subscribe<K extends keyof Topics>(topic: K, handler: (payload: MessageContext<Topics[K]>) => unknown, options?: Record<string, unknown>): Promise<Subscription>;
    producer(...args: unknown[]): Promise<Producer>;
    consumer(...args: unknown[]): Promise<KafkaConsumer>;
    connectAdmin(options?: Record<string, unknown>): Promise<Admin>;
    get admin(): Admin;
    /**
     * Runs an async operation, suppressing any error. Used during shutdown so
     * that one resource's failure does not prevent others from being closed
     * (§7.7).
     */
    private static safe;
    private resolveProducerArgs;
    private resolveConsumerArgs;
    health(): Promise<HealthReport>;
    private healthReport;
    /**
     * Liveness: is the runtime process and internal state functioning? (§16.2)
     */
    liveness(): import("../observability/types.js").LivenessReport;
    /**
     * Readiness: can this application perform the required Kafka operations? (§16.2)
     */
    readiness(): Promise<import("../observability/types.js").ReadinessReport>;
    /**
     * Diagnostics snapshot (§16.4).
     */
    diagnostics(): import("../observability/types.js").RuntimeDiagnostics;
    /**
     * Current metrics snapshot (§16.1).
     */
    metrics(): import("../observability/types.js").MetricsSnapshot;
    /**
     * Record consumer lag gauges for all managed consumers (§16.1).
     */
    private recordConsumerLag;
    /**
     * Wait for pending producer work where the transport supports flushing.
     */
    flush(options?: Record<string, unknown>): Promise<void>;
    /**
     * Runtime status snapshot (§16.4).
     */
    status(): RuntimeStatus;
    close(): Promise<void>;
    private ensureOpen;
    /**
     * Advanced escape hatch (§4.5). Returns the exact underlying KafkaJS
     * transport instances for resources that have already been created.
     * Direct calls bypass package middleware, metrics, serialization, and
     * lifecycle policies. The package still owns disconnecting these
     * resources on `close()`.
     */
    get raw(): {
        producer: (name?: string) => Producer | null;
        consumer: (name?: string) => Consumer | null;
        admin: () => Admin | null;
    };
    get isClosed(): boolean;
    /** Whether the application has been started. */
    get isStarted(): boolean;
    /** Configured broker list. */
    get brokers(): string[];
    /** Connection status for each resource type. */
    get connectionStatus(): {
        producer: boolean;
        consumer: boolean;
        admin: boolean;
    };
    /**
     * Diagnostic resource summaries (name, type, state) for all managed
     * resources. Exposed so HealthReporter can report without accessing
     * private members.
     */
    get diagnosticResources(): Array<{
        name: string;
        type: "producer" | "consumer" | "admin";
        state: string;
    }>;
    /** Read-only view of registered health checks. */
    get registeredHealthChecks(): readonly [string, () => Promise<{
        ok: boolean;
        detail?: string;
    }>][];
    /** Read-only view of registered diagnostic providers. */
    get registeredDiagnosticProviders(): readonly (() => Record<string, unknown>)[];
    /**
     * Register a health check. Public so HealthReporter and plugins can
     * contribute without accessing private members.
     */
    registerHealthCheck(name: string, check: () => Promise<{
        ok: boolean;
        detail?: string;
    }>): void;
    /**
     * Register a diagnostics provider. Public so HealthReporter and plugins
     * can contribute without accessing private members.
     */
    registerDiagnostics(provider: () => Record<string, unknown>): void;
    /**
     * Returns the application's tracer for distributed tracing (§16.3).
     * Defaults to a no-op tracer unless a tracing option was supplied.
     */
    trace(): Tracer;
}
export declare function createApplication<Topics = Record<string, unknown>>(options?: KafkaApplicationOptions): KafkaApplication<Topics>;
/**
 * Builds a new `KafkaApplication` from legacy `KafkaClientOptions` (§21).
 *
 * This is the migration adapter: legacy callers can move from
 * `createKafkaClient()` to `Kafka.create()` / `createApplication()` without
 * maintaining two separate implementations. Both façades share the same
 * `src/client/` KafkaJS adapter runtime.
 */
export declare function createApplicationFromLegacyOptions(options?: KafkaClientOptions): KafkaApplication;
