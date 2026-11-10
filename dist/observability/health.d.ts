import type { KafkaApplication } from "../public/application.js";
import type { RuntimeDiagnostics, LivenessReport, ReadinessReport } from "./types.js";
export declare class HealthReporter {
    private readonly application;
    constructor(application: KafkaApplication);
    /**
     * Liveness: is the runtime process and internal state functioning?
     * (§16.2) A running (not closed) application is alive.
     */
    liveness(): LivenessReport;
    /**
     * Readiness: can this application currently perform the required Kafka
     * operations? (§16.2)
     */
    readiness(): Promise<ReadinessReport>;
    /**
     * Register a health check contributed by a plugin (§15.1).
     */
    registerCheck(name: string, check: () => Promise<{
        ok: boolean;
        detail?: string;
    }>): void;
    /**
     * Diagnostics snapshot (§16.4). Does not expose credentials, payloads,
     * or SASL tokens.
     */
    diagnostics(): RuntimeDiagnostics;
}
