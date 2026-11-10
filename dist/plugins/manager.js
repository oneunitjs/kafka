import { KafkaPluginError } from "../errors/index.js";
export class PluginManager {
    plugins = new Map();
    installOrder = [];
    context;
    pluginRegistrations = new Map();
    installed = false;
    closed = false;
    hookTimeoutMs;
    constructor(context, options = {}) {
        this.context = context;
        this.hookTimeoutMs = options.hookTimeoutMs ?? 10000;
    }
    ensureOpen() {
        if (this.closed) {
            throw new KafkaPluginError("Plugin manager is closed", {
                code: "KAFKA_PLUGIN_ERROR",
                operation: "install",
                retryable: false,
            });
        }
    }
    /**
     * Wraps the shared plugin context so every registration made during a
     * single plugin install is captured. Captured unregister functions are
     * replayed in reverse on rollback or close, ensuring a failed install
     * does not leave partially registered hooks behind (§15.3).
     */
    wrapContext(registrations) {
        const c = this.context;
        return {
            registerPublishMiddleware: (middleware) => {
                const unregister = c.registerPublishMiddleware(middleware);
                registrations.push(unregister);
            },
            registerConsumeMiddleware: (middleware) => {
                const unregister = c.registerConsumeMiddleware(middleware);
                registrations.push(unregister);
            },
            registerCodec: (name, codec) => {
                const unregister = c.registerCodec(name, codec);
                registrations.push(unregister);
            },
            registerHealthCheck: (name, check) => {
                const unregister = c.registerHealthCheck(name, check);
                registrations.push(unregister);
            },
            registerMetricCollector: (collector) => {
                const unregister = c.registerMetricCollector(collector);
                registrations.push(unregister);
            },
            registerDiagnostics: (provider) => {
                const unregister = c.registerDiagnostics(provider);
                registrations.push(unregister);
            },
            on: (event, handler) => {
                const unregister = c.on(event, handler);
                registrations.push(unregister);
            },
        };
    }
    /**
     * Install a plugin. Plugins are installed in declaration order.
     * Duplicate names are rejected (§15.3).
     *
     * If installation fails, already-installed plugins are rolled back in
     * reverse order and their registrations are unregistered (§15.2).
     */
    async install(plugin) {
        this.ensureOpen();
        if (this.plugins.has(plugin.name)) {
            throw new KafkaPluginError(`Duplicate plugin name "${plugin.name}"`, { code: "KAFKA_PLUGIN_ERROR", operation: "install", resource: plugin.name, retryable: false });
        }
        if (this.installed) {
            throw new KafkaPluginError("Plugins cannot be installed after the application is ready", { code: "KAFKA_PLUGIN_ERROR", operation: "install", resource: plugin.name, retryable: false });
        }
        const registrations = [];
        try {
            const result = plugin.install(this.wrapContext(registrations));
            await Promise.resolve(result);
        }
        catch (error) {
            await this.unregisterRegistrations(registrations);
            await this.rollback();
            throw new KafkaPluginError(`Failed to install plugin "${plugin.name}"`, { cause: error, code: "KAFKA_PLUGIN_ERROR", operation: "install", resource: plugin.name, retryable: false });
        }
        this.plugins.set(plugin.name, plugin);
        this.installOrder.push(plugin.name);
        this.pluginRegistrations.set(plugin.name, registrations);
        const result = { name: plugin.name, installed: true };
        return result;
    }
    /**
     * Run a plugin hook with a timeout (§15.3). If the hook does not resolve
     * before the timeout, it is rejected with a KafkaPluginError.
     */
    async runHookWithTimeout(hook, pluginName, hookName) {
        const promise = Promise.resolve(hook());
        const timeout = new Promise((_, reject) => {
            const timer = setTimeout(() => {
                reject(new KafkaPluginError(`Plugin "${pluginName}" ${hookName} hook timed out after ${this.hookTimeoutMs}ms`, { code: "KAFKA_PLUGIN_ERROR", operation: hookName, resource: pluginName, retryable: false }));
            }, this.hookTimeoutMs);
            promise.finally(() => clearTimeout(timer));
        });
        await Promise.race([promise, timeout]);
    }
    /**
     * Mark all plugins ready and run their onReady hooks in install order.
     */
    async ready() {
        if (this.installed) {
            return;
        }
        this.installed = true;
        for (const name of this.installOrder) {
            const plugin = this.plugins.get(name);
            if (plugin.onReady) {
                const hook = plugin.onReady.bind(plugin);
                await this.runHookWithTimeout(() => hook(), name, "onReady");
            }
        }
    }
    /**
     * Run all plugins' onClose hooks in reverse install order, then clear.
     * Failures in one plugin do not prevent others from closing (§15.3).
     */
    async close() {
        if (this.closed) {
            return;
        }
        this.closed = true;
        const errors = await this.tearDownPlugins();
        if (errors.length > 0) {
            throw new KafkaPluginError(`${errors.length} plugin(s) failed during close`, { code: "KAFKA_PLUGIN_ERROR", operation: "close", retryable: false, cause: errors[0] });
        }
    }
    async rollback() {
        const errors = await this.tearDownPlugins();
        if (errors.length > 0) {
            throw new KafkaPluginError(`Rollback failed for ${errors.length} plugin(s)`, { code: "KAFKA_PLUGIN_ERROR", operation: "rollback", retryable: false, cause: errors[0] });
        }
    }
    /**
     * Tears down all installed plugins: runs onClose hooks in reverse install
     * order, unregisters their registrations, and clears state. Returns
     * collected errors (one per failed hook).
     */
    async tearDownPlugins() {
        const errors = [];
        for (let i = this.installOrder.length - 1; i >= 0; i--) {
            const name = this.installOrder[i];
            const plugin = this.plugins.get(name);
            if (plugin.onClose) {
                const hook = plugin.onClose.bind(plugin);
                try {
                    await this.runHookWithTimeout(() => hook(), name, "onClose");
                }
                catch (error) {
                    errors.push(error);
                }
            }
            await this.unregisterRegistrations(this.pluginRegistrations.get(name) ?? []);
            this.pluginRegistrations.delete(name);
        }
        this.plugins.clear();
        this.installOrder.length = 0;
        return errors;
    }
    async unregisterRegistrations(registrations) {
        for (let i = registrations.length - 1; i >= 0; i--) {
            try {
                registrations[i]();
            }
            catch {
                // Unregistration failures are non-fatal during rollback/close.
            }
        }
    }
    get isInstalled() {
        return this.installed;
    }
    get isClosed() {
        return this.closed;
    }
    get names() {
        return [...this.installOrder];
    }
}
