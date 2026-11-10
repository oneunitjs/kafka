const MAX_LABEL_SETS_PER_METRIC = 100;
function stableLabelKey(labels) {
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
export class InMemoryMetricsCollector {
    counters = new Map();
    gauges = new Map();
    timings = new Map();
    increment(name, delta = 1, labels) {
        this.record(this.counters, name, labels, (existing) => (existing ?? 0) + delta);
    }
    gauge(name, value, labels) {
        this.record(this.gauges, name, labels, () => value);
    }
    timing(name, valueMs, labels) {
        this.recordTiming(this.timings, name, labels, valueMs);
    }
    record(map, name, labels, op) {
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
    recordTiming(map, name, labels, valueMs) {
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
        }
        else {
            byLabel.set(key, [valueMs]);
        }
    }
    /** Convert to a MetricsSnapshot (§16), aggregating label dimensions by name. */
    snapshot() {
        const counters = {};
        for (const [name, byLabel] of this.counters) {
            let sum = 0;
            for (const value of byLabel.values()) {
                sum += value;
            }
            counters[name] = sum;
        }
        const gauges = {};
        for (const [name, byLabel] of this.gauges) {
            const values = [...byLabel.values()];
            gauges[name] = values.length > 0 ? values[values.length - 1] : 0;
        }
        const timings = {};
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
    reset() {
        this.counters.clear();
        this.gauges.clear();
        this.timings.clear();
    }
    merge(collector) {
        const snapshot = collector.snapshot();
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
