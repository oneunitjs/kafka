/**
 * examples/prometheus-collector.js
 *
 * A ready-to-use Prometheus MetricsCollector adapter for @oneunit/kafka.
 * Wraps prom-client and maps increment/gauge/timing to Prometheus counters,
 * gauges, and histograms.
 *
 * Usage:
 *
 *   import { Kafka } from "@oneunit/kafka";
 *   import { createPrometheusCollector } from "./examples/prometheus-collector.js";
 *   import { register } from "prom-client";
 *
 *   const kafka = await Kafka.create({ brokers: ["localhost:9092"] });
 *   kafka.registerMetricCollector(createPrometheusCollector(register));
 *
 *   // Expose on your HTTP server:
 *   // app.get("/metrics", async (req, res) => {
 *   //   res.set("Content-Type", register.contentType);
 *   //   res.end(await register.metrics());
 *   // });
 */

import { Counter, Gauge, Histogram, Registry } from "prom-client";

const metricCache = new Map();

function getOrCreateCounter(registry, name, help, labelNames = []) {
  const key = `counter:${name}:${labelNames.join(",")}`;
  if (!metricCache.has(key)) {
    metricCache.set(
      key,
      new Counter({ name, help, labelNames, register: registry }),
    );
  }
  return metricCache.get(key);
}

function getOrCreateGauge(registry, name, help, labelNames = []) {
  const key = `gauge:${name}:${labelNames.join(",")}`;
  if (!metricCache.has(key)) {
    metricCache.set(
      key,
      new Gauge({ name, help, labelNames, register: registry }),
    );
  }
  return metricCache.get(key);
}

function getOrCreateHistogram(registry, name, help, labelNames = []) {
  const key = `histogram:${name}:${labelNames.join(",")}`;
  if (!metricCache.has(key)) {
    metricCache.set(
      key,
      new Histogram({ name, help, labelNames, register: registry }),
    );
  }
  return metricCache.get(key);
}

export function createPrometheusCollector(registry = new Registry()) {
  return {
    increment(name, delta = 1, labels = {}) {
      const labelNames = Object.keys(labels);
      const metric = getOrCreateCounter(registry, name, name, labelNames);
      metric.labels(labels).inc(delta);
    },

    gauge(name, value, labels = {}) {
      const labelNames = Object.keys(labels);
      const metric = getOrCreateGauge(registry, name, name, labelNames);
      metric.labels(labels).set(value);
    },

    timing(name, valueMs, labels = {}) {
      const labelNames = Object.keys(labels);
      const metric = getOrCreateHistogram(registry, name, name, labelNames);
      metric.labels(labels).observe(valueMs);
    },
  };
}

export { Registry };
