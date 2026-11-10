import type { MetricsSnapshot } from "./types.js";
import type { MetricsCollector as MetricsCollectorInterface } from "./types.js";

const MAX_LABEL_SETS_PER_METRIC = 100;

function stableLabelKey(labels?: Record<string, string>): string {
    if (!labels || Object.keys(labels).length === 0) {
        return "";
    }
    return Object.keys(labels)
        .sort()
        .map((key) => `${key}=${JSON.stringify(labels[key])}`)
        .join("&");
}

/**
 * An in-memory MetricsCollector implementation (§16.1).
 *
 * Labels are accepted and stored instead of being discarded, but cardinality
 * is bounded per metric name to prevent unbounded label growth (§16.1). The
 * public snapshot flattens back to a name-keyed record so existing consumers
 * keep working; label-dimensioned values are aggregated by name.
 */
export class InMemoryMetricsCollector implements MetricsCollectorInterface {
    private readonly counters = new Map<string, Map<string, number>>();
    private readonly gauges = new Map<string, Map<string, number>>();
    private readonly timings = new Map<string, Map<string, number[]>>();

    increment(name: string, delta = 1, labels?: Record<string, string>): void {
        this.record(this.counters, name, labels, (existing) => (existing ?? 0) + delta);
    }

    gauge(name: string, value: number, labels?: Record<string, string>): void {
        this.record(this.gauges, name, labels, () => value);
    }

    timing(name: string, valueMs: number, labels?: Record<string, string>): void {
        this.recordTiming(this.timings, name, labels, valueMs);
    }

    private record(
        map: Map<string, Map<string, number>>,
        name: string,
        labels: Record<string, string> | undefined,
        op: (existing: number | undefined) => number,
    ): void {
        const key = stableLabelKey(labels);
        let byLabel = map.get(name);
        if (!byLabel) {
            byLabel = new Map();
            map.set(name, byLabel);
        }
        if (byLabel.size >= MAX_LABEL_SETS_PER_METRIC && !byLabel.has(key)) {
            return;
        }
        byLabel.set(key, op(byLabel.get(key)));
    }

    private recordTiming(
        map: Map<string, Map<string, number[]>>,
        name: string,
        labels: Record<string, string> | undefined,
        valueMs: number,
    ): void {
        const key = stableLabelKey(labels);
        let byLabel = map.get(name);
        if (!byLabel) {
            byLabel = new Map();
            map.set(name, byLabel);
        }
        if (byLabel.size >= MAX_LABEL_SETS_PER_METRIC && !byLabel.has(key)) {
            return;
        }
        const arr = byLabel.get(key);
        if (arr) {
            arr.push(valueMs);
        } else {
            byLabel.set(key, [valueMs]);
        }
    }

    /** Convert to a MetricsSnapshot (§16), aggregating label dimensions by name. */
    snapshot(): MetricsSnapshot {
        const counters: Record<string, number> = {};
        for (const [name, byLabel] of this.counters) {
            let sum = 0;
            for (const value of byLabel.values()) {
                sum += value;
            }
            counters[name] = sum;
        }

        const gauges: Record<string, number> = {};
        for (const [name, byLabel] of this.gauges) {
            const values = [...byLabel.values()];
            gauges[name] = values.length > 0 ? values[values.length - 1] : 0;
        }

        const timings: Record<string, number> = {};
        for (const [name, byLabel] of this.timings) {
            let total = 0;
            let count = 0;
            for (const values of byLabel.values()) {
                for (const v of values) {
                    total += v;
                    count += 1;
                }
            }
            timings[name] = count > 0 ? total / count : 0;
        }

        return { counters, timings, gauges };
    }

    reset(): void {
        this.counters.clear();
        this.gauges.clear();
        this.timings.clear();
    }

    merge(collector: MetricsCollectorInterface): void {
        const snapshot = (collector as InMemoryMetricsCollector).snapshot();
        for (const [name, value] of Object.entries(snapshot.counters)) {
            this.increment(name, value);
        }
        for (const [name, value] of Object.entries(snapshot.gauges)) {
            this.gauge(name, value);
        }
        for (const [name, valueMs] of Object.entries(snapshot.timings)) {
            this.timing(name, valueMs);
        }
    }
}
