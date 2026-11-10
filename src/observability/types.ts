export interface RuntimeDiagnostics {
    state: "starting" | "ready" | "closing" | "closed" | "failed";
    resources: Array<{
        name: string;
        type: "producer" | "consumer" | "admin";
        state: string;
    }>;
    subscriptions: number;
    inFlightHandlers?: number;
    pendingRequests?: number;
}

export interface MetricsSnapshot {
    counters: Record<string, number>;
    timings: Record<string, number>;
    gauges: Record<string, number>;
}

export interface LivenessReport {
    alive: boolean;
    state: string;
}

export interface ReadinessReport {
    ready: boolean;
    state: string;
    reasons?: string[];
}

export interface MetricsCollector {
    increment(name: string, delta?: number, labels?: Record<string, string>): void;
    gauge(name: string, value: number, labels?: Record<string, string>): void;
    timing(name: string, valueMs: number, labels?: Record<string, string>): void;
}
