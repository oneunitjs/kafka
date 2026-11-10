export class HealthReporter {
    application;
    constructor(application) {
        this.application = application;
    }
    /**
     * Liveness: is the runtime process and internal state functioning?
     * (§16.2) A running (not closed) application is alive.
     */
    liveness() {
        return {
            alive: !this.application.isClosed,
            state: this.application.isClosed ? "closed" : "ready",
        };
    }
    /**
     * Readiness: can this application currently perform the required Kafka
     * operations? (§16.2)
     */
    async readiness() {
        const reasons = [];
        if (this.application.isClosed) {
            return { ready: false, state: "closed", reasons: ["application is closed"] };
        }
        try {
            const health = await this.application.health();
            if (health.state === "closed" || health.state === "failed") {
                reasons.push(`application state is ${health.state}`);
            }
            if (health.brokers.length === 0) {
                reasons.push("no brokers configured");
            }
        }
        catch (error) {
            reasons.push(`health check failed: ${error.message}`);
        }
        return {
            ready: reasons.length === 0,
            state: "ready",
            reasons,
        };
    }
    /**
     * Register a health check contributed by a plugin (§15.1).
     */
    registerCheck(name, check) {
        this.application.registerHealthCheck(name, check);
    }
    /**
     * Diagnostics snapshot (§16.4). Does not expose credentials, payloads,
     * or SASL tokens.
     */
    diagnostics() {
        const state = this.application.isClosed ? "closed" : "ready";
        const resources = this.application.diagnosticResources;
        const subscriptions = resources.filter((r) => r.type === "consumer").length;
        return {
            state,
            resources,
            subscriptions,
        };
    }
}
