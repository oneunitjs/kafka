import type { KafkaPlugin, PluginContext, InstallResult } from "./types.js";
export interface PluginManagerOptions {
    readonly onInstall?: (result: InstallResult) => void;
    /** Timeout in milliseconds for plugin onReady/onClose hooks (§15.3). */
    readonly hookTimeoutMs?: number;
}
export declare class PluginManager {
    private readonly plugins;
    private readonly installOrder;
    private readonly context;
    private readonly pluginRegistrations;
    private installed;
    private closed;
    private readonly hookTimeoutMs;
    constructor(context: PluginContext, options?: PluginManagerOptions);
    private ensureOpen;
    /**
     * Wraps the shared plugin context so every registration made during a
     * single plugin install is captured. Captured unregister functions are
     * replayed in reverse on rollback or close, ensuring a failed install
     * does not leave partially registered hooks behind (§15.3).
     */
    private wrapContext;
    /**
     * Install a plugin. Plugins are installed in declaration order.
     * Duplicate names are rejected (§15.3).
     *
     * If installation fails, already-installed plugins are rolled back in
     * reverse order and their registrations are unregistered (§15.2).
     */
    install(plugin: KafkaPlugin): Promise<InstallResult>;
    /**
     * Run a plugin hook with a timeout (§15.3). If the hook does not resolve
     * before the timeout, it is rejected with a KafkaPluginError.
     */
    private runHookWithTimeout;
    /**
     * Mark all plugins ready and run their onReady hooks in install order.
     */
    ready(): Promise<void>;
    /**
     * Run all plugins' onClose hooks in reverse install order, then clear.
     * Failures in one plugin do not prevent others from closing (§15.3).
     */
    close(): Promise<void>;
    private rollback;
    /**
     * Tears down all installed plugins: runs onClose hooks in reverse install
     * order, unregisters their registrations, and clears state. Returns
     * collected errors (one per failed hook).
     */
    private tearDownPlugins;
    private unregisterRegistrations;
    get isInstalled(): boolean;
    get isClosed(): boolean;
    get names(): readonly string[];
}
