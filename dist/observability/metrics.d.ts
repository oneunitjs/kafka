import type { MetricsSnapshot } from "./types.js";
import type { MetricsCollector as MetricsCollectorInterface } from "./types.js";
/**
 * An in-memory MetricsCollector implementation (§16.1).
 *
 * Labels are accepted and stored instead of being discarded, but cardinality
 * is bounded per metric name to prevent unbounded label growth (§16.1). The
 * public snapshot flattens back to a name-keyed record so existing consumers
 * keep working; label-dimensioned values are aggregated by name.
 */
export declare class InMemoryMetricsCollector implements MetricsCollectorInterface {
    private readonly counters;
    private readonly gauges;
    private readonly timings;
    increment(name: string, delta?: number, labels?: Record<string, string>): void;
    gauge(name: string, value: number, labels?: Record<string, string>): void;
    timing(name: string, valueMs: number, labels?: Record<string, string>): void;
    private record;
    private recordTiming;
    /** Convert to a MetricsSnapshot (§16), aggregating label dimensions by name. */
    snapshot(): MetricsSnapshot;
    reset(): void;
    merge(collector: MetricsCollectorInterface): void;
}
