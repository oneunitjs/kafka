import type { ConsumeMiddleware, PublishMiddleware } from "../middleware/compose.js";
import type { Codec } from "../adapters/codec.js";
import type { MetricsCollector } from "../observability/types.js";
export interface HealthCheck {
    readonly name: string;
    check: () => Promise<{
        ok: boolean;
        detail?: string;
    }>;
}
export interface PluginContext {
    /**
     * Register publish middleware. Runs in registration order for the
     * inbound path and reverse order on unwind (§11.3).
     * Returns an unregister function.
     */
    registerPublishMiddleware<T = unknown>(middleware: PublishMiddleware<T>): () => void;
    /**
     * Register consume middleware. Runs in registration order for the
     * inbound path and reverse order on unwind (§11.3).
     * Returns an unregister function.
     */
    registerConsumeMiddleware<T = unknown>(middleware: ConsumeMiddleware<T>): () => void;
    /**
     * Register a named codec for use by the application and plugins.
     * Returns an unregister function.
     */
    registerCodec(name: string, codec: Codec): () => void;
    /**
     * Register a health check that is reported through application health
     * and diagnostics (§15.1, §16).
     * Returns an unregister function.
     */
    registerHealthCheck(name: string, check: () => Promise<{
        ok: boolean;
        detail?: string;
    }>): () => void;
    /**
     * Register a metrics collector (§15.1, §16.1).
     * Returns an unregister function.
     */
    registerMetricCollector(collector: MetricsCollector): () => void;
    /**
     * Register a diagnostics provider that contributes to runtime
     * diagnostics (§15.1, §16.4).
     * Returns an unregister function.
     */
    registerDiagnostics(provider: () => Record<string, unknown>): () => void;
    /**
     * Subscribe to application lifecycle events.
     * Returns an unregister function.
     */
    on(event: "ready" | "close", handler: () => unknown | Promise<unknown>): () => void;
}
export interface KafkaPlugin {
    readonly name: string;
    install(context: PluginContext): void | Promise<void>;
    onReady?(): void | Promise<void>;
    onClose?(): void | Promise<void>;
}
export interface InstallResult {
    readonly name: string;
    readonly installed: boolean;
}
