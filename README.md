# @oneunit/kafka

Beta release (v1.1.0-beta.0) — a highly reliable Kafka client for Node.js built on top of [KafkaJS](https://kafka.js.org/). Works standalone or with any framework, offering type-safe topic contracts, built-in resilience, graceful lifecycle management, onion middleware, transactional plugins, and enterprise-grade observability.

[![npm version](https://img.shields.io/npm/v/@oneunit/kafka)](https://www.npmjs.com/package/@oneunit/kafka)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)
[![Node.js](https://img.shields.io/badge/node-%3E%3D20-brightgreen.svg)](https://nodejs.org/)
[![Module: ESM](https://img.shields.io/badge/module-ESM-yellow.svg)](https://nodejs.org/api/esm.html)

---

## Table of Contents

- [Features Overview](#features-overview)
- [Installation & Requirements](#installation--requirements)
- [Quick Start](#quick-start)
  - [Type-Safe Topics](#type-safe-topics)
- [Core Usage](#core-usage)
  - [Producing Messages](#producing-messages)
  - [Consuming Messages](#consuming-messages)
  - [Consumer Lifecycle & Multi-Reason Pausing](#consumer-lifecycle--multi-reason-pausing)
  - [Contiguous Offset Tracking & Safe Commits](#contiguous-offset-tracking--safe-commits)
- [Resilience & Fault Tolerance](#resilience--fault-tolerance)
  - [Retry Policies](#retry-policies)
  - [Dead-Letter Queue (DLQ)](#dead-letter-queue-dlq)
- [Request / Response (RPC)](#request--response-rpc)
- [Transactions](#transactions)
- [High-Throughput Buffered Batching](#high-throughput-buffered-batching)
- [Onion Middleware Pipelines](#onion-middleware-pipelines)
- [Lifecycle-Aware Plugin System](#lifecycle-aware-plugin-system)
- [Enterprise Observability](#enterprise-observability)
  - [Metrics Collection & Consumer Lag](#metrics-collection--consumer-lag)
  - [Health Probes (Liveness & Readiness)](#health-probes-liveness--readiness)
  - [Distributed Tracing (W3C / OpenTelemetry)](#distributed-tracing-w3c--opentelemetry)
  - [Diagnostics & Secret Redaction](#diagnostics--secret-redaction)
- [Security & Authentication](#security--authentication)
  - [SASL (PLAIN, SCRAM, AWS IAM, OAuthBearer)](#sasl-plain-scram-aws-iam-oauthbearer)
  - [TLS / SSL](#tls--ssl)
- [Serialization, Codecs & Schemas](#serialization-codecs--schemas)
  - [Codecs (JSON & Bytes)](#codecs-json--bytes)
  - [Schema Registry Integration](#schema-registry-integration)
  - [Runtime Validation (Zod, Valibot, etc.)](#runtime-validation-zod-valibot-etc)
  - [Reserved Headers](#reserved-headers)
- [Cluster Administration (KafkaAdmin)](#cluster-administration-kafkaadmin)
- [Lifecycle & 10-Step Graceful Shutdown](#lifecycle--10-step-graceful-shutdown)
- [Structured Error Hierarchy](#structured-error-hierarchy)
- [Configuration Reference](#configuration-reference)
  - [Flat vs. Namespaced Configuration](#flat-vs-namespaced-configuration)
  - [Environment Variables](#environment-variables)
- [Escape Hatch: Direct KafkaJS Access](#escape-hatch-direct-kafkajs-access)
- [Legacy & Low-Level API Support](#legacy--low-level-api-support)
- [Package Subpath Exports](#package-subpath-exports)
- [Development & Verification](#development--verification)
- [License](#license)

---

## Features Overview

| Category | Capabilities |
|---|---|
| **Core API Facade** | High-level `Kafka.create()` and `KafkaApplication<Topics>`, compile-time typed topics, lazy resource initialization, `publish`, `publishMany`, `subscribe`, and named resource slots. |
| **Consumer Engine** | Managed runner loop per consumer group, multi-topic registry, backpressure bounded by `maxInFlight` semaphore, per-handler timeout (`handlerTimeoutMs`), ref-counted multi-reason pause/resume, and abort signal propagation. |
| **Offset Reliability** | Contiguous offset tracking per partition (guaranteeing that out-of-order async completions never trigger unsafe offset commits), automatic and manual `commitOffsets()`, and real-time consumer lag calculation. |
| **Resilience & DLQ** | Exponential backoff retry with uniform random jitter, extensible error classification (`retry`, `dead-letter`, `stop`), and automatic Dead Letter Queue publishing preserving full message metadata and diagnostics. |
| **RPC Messaging** | Asynchronous request-response over Kafka with correlation IDs, dedicated response topics, per-request timeouts, and clean handling of late or unmatched responses. |
| **Transactions** | First-class `kafka.transaction(async (tx) => { ... })` transactional block with automatic commit on success and automatic rollback on failure, capturing both primary and abort exceptions. |
| **Batching** | Opt-in `BufferedBatchProducer` accumulating records across topics with time, count, and byte size flush triggers, plus backpressure limits. |
| **Extensibility** | Inverted onion middleware pipelines for publish, consume, error, and admin operations; plugin system with transactional install, rollback on failure, and `onReady`/`onClose` lifecycle hooks with timeouts. |
| **Observability** | In-memory metrics collector with label cardinality bounds (`MAX_LABEL_SETS_PER_METRIC = 100`), consumer lag gauges, liveness and readiness health checks, W3C traceparent/tracestate distributed tracing, and sanitized diagnostics. |
| **Security** | SASL mechanisms (`plain`, `scram-sha-256`, `scram-sha-512`, `aws`, `oauthbearer`), TLS/SSL with file paths or inline PEM certificates, and automatic secret redaction (`KafkaError.redact()`). |
| **Serialization** | Synchronous `Codec` contract (default JSON and raw bytes), schema registry support (`InMemorySchemaRegistry`, `AbstractSchemaRegistry`, `ConfluentSchemaRegistry`), runtime schema validation, and reserved headers. |
| **Cluster Admin** | Managed `KafkaAdmin` with `TopicAdmin` (create, delete, list, inspect metadata) and `GroupAdmin` (list, describe, delete consumer groups) integrated with admin middleware. |
| **Runtime Core** | In-flight promise deduplication (`ResourceSlot`), generation/epoch guards preventing stale connection adoption across reconnects, idempotent disposal, and 10-step graceful shutdown. |
| **Packaging** | Native ESM (`"type": "module"`), tree-shakeable (`sideEffects: false`), strict TypeScript type declarations, source maps, and 22 subpath exports. |

---

## Installation & Requirements

### Requirements
- **Node.js**: `>= 20.0.0`
- **Dependency**: `kafkajs@2.2.4` (bundled as a runtime dependency — no separate install needed)
- **Kafka Broker**: Apache Kafka 2.4+ (local, Docker, Strimzi, Confluent Cloud, AWS MSK, Redpanda, etc.)

### Install

```bash
npm install @oneunit/kafka
# or
pnpm add @oneunit/kafka
# or
yarn add @oneunit/kafka
```

---

## Quick Start

```ts
import { Kafka } from "@oneunit/kafka";

// 1. Initialize the application (lazy; does not connect immediately)
const kafka = await Kafka.create({
  brokers: ["localhost:9092"],
  clientId: "order-service",
});

// 2. Publish a message (automatically encoded as JSON)
const result = await kafka.publish("orders", {
  orderId: "order-101",
  customer: "Alice",
  amount: 49.99,
});
console.log(`Published to ${result.topic} [partition ${result.partition}, offset ${result.offset}]`);

// 3. Subscribe to a topic
const subscription = await kafka.subscribe(
  "orders",
  async (ctx) => {
    console.log("Received order:", ctx.value);
    console.log("Partition:", ctx.partition, "Offset:", ctx.offset);
    console.log("Correlation ID:", ctx.metadata.correlationId);
  },
  { groupId: "order-workers" },
);

// 4. Perform graceful shutdown when your service terminates
process.on("SIGTERM", async () => {
  await kafka.close();
  process.exit(0);
});
```

### Type-Safe Topics

Define your topic schemas in TypeScript to enforce compile-time verification on message publishing and consuming:

```ts
interface MyTopics {
  "orders.created": { orderId: string; total: number };
  "orders.cancelled": { orderId: string; reason: string };
  "notifications": { userId: string; text: string };
}

// Pass topic types as the generic parameter
const kafka = await Kafka.create<MyTopics>({
  brokers: ["localhost:9092"],
  clientId: "typed-order-service",
});

// ✅ Type-checked: value must match { orderId: string; total: number }
await kafka.publish("orders.created", {
  orderId: "ord-42",
  total: 100.5,
});

// ❌ TypeScript Error: Property 'total' is missing
// await kafka.publish("orders.created", { orderId: "ord-42" });

// ✅ Type-checked subscription payload
await kafka.subscribe("orders.cancelled", async (ctx) => {
  // ctx.value is inferred as { orderId: string; reason: string }
  console.log(`Order ${ctx.value.orderId} cancelled: ${ctx.value.reason}`);
});
```

> [!NOTE]
> If topic types are not specified, `Kafka.create()` defaults to `Record<string, unknown>`, allowing arbitrary topic names and values.

---

## Core Usage

### Producing Messages

#### `publish(topic, value, options?)`
Publishes a single message through the publish middleware pipeline.

```ts
const result = await kafka.publish(
  "orders.created",
  { orderId: "123", item: "Widget" },
  {
    codec: "json",          // "json" (default), "bytes", or custom Codec
    validator: orderSchema, // Optional runtime validator (e.g. Zod schema)
    send: { acks: -1 },     // KafkaJS producer send options
  },
);

console.log(result.topic, result.partition, result.offset, result.timestamp);
```

#### `publishMany(topic, values, options?)`
Publishes multiple messages in a single batch to a topic through the publish middleware pipeline.

```ts
const batchResult = await kafka.publishMany(
  "orders.created",
  [
    { orderId: "101", item: "Keyboard" },
    { orderId: "102", item: "Mouse" },
  ],
  { codec: "json" },
);

console.log(`Published ${batchResult.records?.length} records`);
```

#### Named Producer Instances
For scenarios requiring dedicated producers with specialized configurations (e.g., dedicated idempotency or custom partitioners):

```ts
const producer = await kafka.producer("analytics-producer", {
  idempotent: true,
  acks: "all",
});

await producer.send({
  topic: "analytics",
  messages: [{ key: "pageview", value: Buffer.from("...") }],
});
```

Producer slots are deduplicated: calling `kafka.producer("analytics-producer")` concurrently shares a single in-flight connection promise.

#### Flushing Producers
```ts
// Wait for all active producers to flush their buffered network buffers
await kafka.flush();
```

---

### Consuming Messages

#### `subscribe(topic, handler, options?)`
Registers a subscription. Under the hood, a managed `KafkaConsumer` runner is lazily allocated for the consumer group.

```ts
const subscription = await kafka.subscribe(
  "orders.created",
  async (ctx) => {
    // Message context
    const { value, key, topic, partition, offset, timestamp, headers, signal, metadata } = ctx;

    console.log(`Processing ${topic} [${partition}@${offset}] with key=${key}`);
    console.log("Decoded value:", value);
    console.log("Headers:", headers);
    console.log("Correlation ID:", metadata.correlationId);

    // AbortSignal triggered if consumer shuts down during execution
    if (signal?.aborted) {
      console.warn("Handler aborted by shutdown");
      return;
    }
  },
  {
    groupId: "order-processor",
    fromBeginning: false,  // Start from latest offset (default)
    maxInFlight: 50,       // Max concurrent handler executions across partitions
    handlerTimeoutMs: 10000, // Per-handler timeout (prevents shutdown blocking on deadlocked handlers)
    retryPolicy: { maxAttempts: 5, baseDelayMs: 200 },
    deadLetterTopic: "orders.created.dlq",
  },
);

// Unsubscribe individual subscription
await subscription.stop();
```

#### Message Context (`MessageContext<T>`)
Every consumer handler receives a rich context object:
- `value: T`: Deserialized message value (decoded via configured Codec).
- `key: unknown`: Deserialized message key.
- `topic: string`: Originating Kafka topic.
- `partition: number`: Partition number.
- `offset: string`: Offset of the message.
- `timestamp: string`: Broker timestamp.
- `headers: Record<string, string | Buffer | undefined>`: Raw message headers.
- `signal?: AbortSignal`: Cancellation signal triggered if consumer intake stops or times out.
- `metadata`: Extracted metadata from reserved headers:
  - `correlationId?: string`
  - `messageId?: string`
  - `contentType?: string`
  - `schemaVersion?: string`

---

### Consumer Lifecycle & Multi-Reason Pausing

For low-level consumer management, obtain a dedicated `KafkaConsumer` instance:

```ts
const consumer = await kafka.consumer("order-consumer", "order-group", {
  fromBeginning: true,
  maxInFlight: 25,
});

consumer.subscribe("orders.created", async (msg) => {
  // process message
});

// Start the consumer runner loop
await consumer.start();

// Status inspections
console.log("Is running:", consumer.isRunning);
console.log("In-flight handlers:", consumer.inFlightCount);
```

#### Multi-Reason Pause & Resume
In complex systems, multiple components (e.g., rate limiters, backpressure monitors, database circuit breakers) may independently require a consumer to pause. Traditional boolean pausing suffers from race conditions where one component calls `resume()` while another still requires pausing.

`@oneunit/kafka` uses **reference-counted, named pause reasons**:

```ts
// Rate limiter pauses intake
await consumer.pause("rate-limit");

// Circuit breaker independently pauses intake
await consumer.pause("db-circuit-breaker");

// Rate limiter recovers and resumes
await consumer.resume("rate-limit");

// Consumer remains paused because "db-circuit-breaker" is still active!
console.log(consumer.isPaused); // true
console.log(consumer.pausedReasons); // Set { "db-circuit-breaker" }

// Circuit breaker recovers
await consumer.resume("db-circuit-breaker");

// Now consumer automatically resumes consumption from Kafka brokers
console.log(consumer.isPaused); // false
```

### Per-Handler Timeout (`handlerTimeoutMs`)

For workloads where a handler can deadlock or hang indefinitely, `@oneunit/kafka` supports a per-handler timeout that guarantees shutdown will not wait indefinitely on a misbehaving handler:

```ts
const consumer = await kafka.consumer("order-consumer", "order-group", {
  handlerTimeoutMs: 10000, // Abort any handler exceeding 10 seconds
});

await consumer.start();

// Handlers that exceed `handlerTimeoutMs` reject with `KafkaTimeoutError`,
// their semaphore slot is released immediately, and their offset is not
// committed — preserving at-least-once delivery semantics.
```

**Behavior:**
- The timeout starts when the handler begins execution.
- If the handler exceeds `handlerTimeoutMs`, its `AbortSignal` is aborted (so cooperative handlers can exit early) and the promise rejects with `KafkaTimeoutError`.
- **Important:** `AbortSignal` is cooperative — user code must observe `signal.aborted` to exit early. The package cannot forcibly terminate arbitrary JavaScript work; a timed-out handler's side effects may continue running in the background even after the semaphore slot is released.
- The semaphore slot is released immediately, allowing `awaitInFlight` during shutdown to drain without waiting for the deadlocked handler.
- Timeout errors flow through your configured retry policy — classify them as `"stop"` or `"dead-letter"` depending on your tolerance for message re-delivery.

**Tuning:** Set `handlerTimeoutMs` to approximately 1.5× your expected p99 handler latency.

---

### Contiguous Offset Tracking & Safe Commits

When processing messages asynchronously with concurrency (`maxInFlight > 1`), handlers may finish out of order. Committing a later offset while an earlier message is still processing can cause **message loss** if the consumer restarts.

`@oneunit/kafka` tracks all completed offsets per partition and **only commits contiguous completed ranges**:

```
Message Offsets:  [100] [101] [102] [103]
Status:           Done  Running Done  Done
Committed:        100   <-- 102 & 103 are NOT committed until 101 finishes!
```

```ts
// Manually commit highest contiguous completed offsets across all assigned partitions
await consumer.commitOffsets();

// Inspect completed contiguous offsets
const completed = consumer.getCompletedOffsets();
// Map(topic => Map(partition => highestContiguousOffset))

// Inspect consumer lag per topic and partition
const lagMap = consumer.getLag();
// Map(topic => Map(partition => lagNumber))
```

---

## Resilience & Fault Tolerance

### Retry Policies

Consumer handlers support automatic retries with exponential backoff and uniform random jitter:

$$\text{delay} = \min(\text{maxDelayMs}, \text{baseDelayMs} \times 2^{\text{attempt}}) \times (1 + \text{jitter})$$

where `jitter` is uniformly random in the range `[0, 0.2)` (i.e. `random(0, 0.2)`), adding up to 20% variation to each backoff delay.

```ts
import { createRetryPolicy } from "@oneunit/kafka/resilience";

const customRetryPolicy = createRetryPolicy({
  maxAttempts: 5,            // Retry up to 5 times (total 6 executions)
  baseDelayMs: 200,          // Start backoff at 200ms
  maxDelayMs: 10_000,        // Cap backoff at 10s
  maxElapsedTimeMs: 60_000,  // Max total time spent retrying
  classify: (error) => {
    // Decide action based on error type
    if (error instanceof ValidationError) {
      return { action: "dead-letter" }; // Bad payload: route immediately to DLQ
    }
    if (error.isTemporaryNetworkError) {
      return { action: "retry" };       // Transient error: retry with backoff
    }
    return { action: "stop" };          // Fatal error: halt and rethrow
  },
});

await kafka.subscribe("orders", handler, {
  groupId: "order-processor",
  retryPolicy: customRetryPolicy,
});
```

#### Default Error Classifier
The built-in classifier (`defaultClassifier`) enforces safe defaults:
- `KafkaConfigError` $\to$ `"stop"` (configuration errors are fatal).
- `KafkaDecodeError` $\to$ `"dead-letter"` (corrupt data will never decode successfully; avoid infinite poison pill loops).
- Errors with `retryable: true` $\to$ `"retry"`.
- All other generic errors $\to$ `"stop"` (fail fast to avoid silently masking programming bugs).

---

### Dead-Letter Queue (DLQ)

When an error is classified as `"dead-letter"` or exceeds `maxAttempts`, the record is automatically published to a Dead Letter Queue topic using `DeadLetterPublisher`.

```ts
await kafka.subscribe("orders", handler, {
  groupId: "order-processor",
  deadLetterTopic: "orders.DLQ",
});
```

#### DLQ Message Envelope Structure
The message published to the DLQ preserves comprehensive diagnostic information:

```json
{
  "originalTopic": "orders",
  "originalPartition": 2,
  "originalOffset": "142857",
  "originalKey": "cust-882",
  "originalPayload": { "orderId": "123" },
  "originalHeaders": { "content-type": "application/json" },
  "failureTimestamp": "2026-11-09T12:00:00.000Z",
  "attemptCount": 5,
  "errorName": "DatabaseTimeoutError",
  "errorMessage": "Connection pool exhausted",
  "consumerGroup": "order-processor",
  "correlationId": "uuid-v4-string",
  "messageId": "msg-v4-string"
}
```

> [!IMPORTANT]
> If publishing to the DLQ fails, an exception is thrown and the original message offset is **not** committed, guaranteeing at-least-once delivery semantics.

---

## Request / Response (RPC)

`@oneunit/kafka` provides native request/response patterns over Kafka topics without requiring external transport mechanisms.

```ts
// ====================
// SERVER SIDE (Responder)
// ====================
await kafka.respond(
  "user.lookup",
  async (request: { userId: string }, ctx) => {
    console.log(`Received RPC request for user ${request.userId}`);
    console.log(`Correlation ID: ${ctx.correlationId}`);

    // Return response payload (automatically routed back)
    return {
      userId: request.userId,
      name: "Jane Doe",
      email: "jane@example.com",
    };
  },
  { groupId: "user-service" },
);

// ====================
// CLIENT SIDE (Caller)
// ====================
interface UserResponse {
  userId: string;
  name: string;
  email: string;
}

const user = await kafka.request<UserResponse>(
  "user.lookup",
  { userId: "user-42" },
  {
    timeoutMs: 5000,                 // Request timeout (default 30s)
    responseTopic: "user.lookup.responses", // Optional custom response topic
  },
);

console.log("User received via RPC:", user.name);
```

### RPC Guarantees
- Automatic correlation ID generation via `crypto.randomUUID()`.
- Per-request timeout timers; expired requests reject with `KafkaTimeoutError`.
- Clean cleanup of the internal pending map.
- Late or unmatched responses arriving after timeout are safely ignored without crashing the process.

---

## Transactions

Atomically publish records across Kafka topics using Kafka transactions:

```ts
const result = await kafka.transaction(async (tx) => {
  // 1. Publish to orders topic
  await tx.publish("orders", { orderId: "ord-1", status: "confirmed" });

  // 2. Publish to audit log
  await tx.publishMany("audit.events", [
    { action: "order_created", target: "ord-1" },
    { action: "inventory_reserved", target: "ord-1" },
  ]);

  // Return value is passed through to the caller
  return { orderId: "ord-1", committed: true };
});

console.log("Transaction committed:", result.orderId);
```

### Transaction Mechanics
- If the callback resolves, the transaction is **automatically committed**.
- If the callback throws an error, the transaction is **automatically aborted**, and the original error is rethrown.
- If both the user callback fails and `abort()` fails, both errors are captured and reported.
- Explicit `tx.commit()`, `tx.abort()`, and `tx.isActive()` methods are also available on the transaction object.

> [!NOTE]
> Kafka transactions provide atomicity for Kafka writes across multiple topics. They do **not** automatically provide exactly-once processing for external side effects (database writes, HTTP calls, etc.) made inside the transaction callback.

---

## High-Throughput Buffered Batching

For extreme throughput scenarios where individual network requests impose too much overhead, opt in to `BufferedBatchProducer`:

```ts
import { BufferedBatchProducer } from "@oneunit/kafka/producer";

const batchProducer = new BufferedBatchProducer({
  kafka: kafka.raw.producer() ?? (await kafka.producer()).raw, // Underlying KafkaJS Kafka instance
  maxRecordsPerTopic: 1000,     // Flush a topic when it reaches 1,000 records
  maxBufferedRecords: 10_000,   // Flush all topics when total buffer hits 10,000 records
  maxBufferedBytes: 1_000_000,  // Flush when buffer reaches 1 MB
  flushIntervalMs: 5000,        // Flush at least once every 5 seconds
  maxQueuedRecords: 50_000,     // Reject send() when queued records reach 50,000 (backpressure)
  maxQueuedBytes: 50_000_000,   // Reject send() when queued bytes reach 50 MB
});

// Enqueue messages; each send returns a promise resolving when the batch flushes
await batchProducer.send("telemetry", { metric: "cpu", value: 42 });

// Force an immediate manual flush
await batchProducer.flush();

// Flush remaining records and cleanly disconnect
await batchProducer.close();
```

---

## Onion Middleware Pipelines

`@oneunit/kafka` employs an **inverted onion middleware pipeline** (similar to Koa or Express). Middleware executes in registration order on the inbound journey and in reverse order on the unwind journey.

```
       Inbound                  Unwind
┌──────────────────┐      ┌──────────────────┐
│ Middleware A     │ ───> │ Middleware A     │
└────────┬─────────┘      └────────▲─────────┘
         │                         │
┌────────▼─────────┐      ┌────────┴─────────┐
│ Middleware B     │ ───> │ Middleware B     │
└────────┬─────────┘      └────────▲─────────┘
         │                         │
         └───> [Transport Call] ───┘
```

```ts
// 1. Publish Middleware
kafka.use(async (ctx, next) => {
  const start = Date.now();
  ctx.headers["x-published-by"] = "order-service";

  const result = await next(); // Forward to next middleware or broker transport

  console.log(`Publish to ${ctx.topic} took ${Date.now() - start}ms`);
  return result;
});

// 2. Consume Middleware
kafka.useConsume(async (ctx, next) => {
  console.log(`Consuming message from ${ctx.topic} [partition ${ctx.partition}@${ctx.offset}]`);
  await next();
  console.log(`Successfully processed offset ${ctx.offset}`);
});

// 3. Error Middleware
kafka.useError(async (error, ctx) => {
  console.error(`Pipeline failure on topic ${ctx.topic}:`, error);
  // Return a substitute Error or rethrow
  throw error;
});

// 4. Admin Middleware
kafka.useAdmin(async (ctx, next) => {
  console.log(`Admin operation initiated: ${ctx.operation}`, ctx.args);
  return next();
});
```

> [!IMPORTANT]
> In accordance with onion middleware invariants, calling `next()` more than once within a single middleware execution will throw an error.

---

## Lifecycle-Aware Plugin System

Package cross-cutting capabilities, health checks, codecs, and middleware into modular plugins with transactional installation and safe shutdown hooks:

```ts
import type { KafkaPlugin, PluginContext } from "@oneunit/kafka/plugins";

const auditPlugin: KafkaPlugin = {
  name: "audit-logger",

  install(context: PluginContext) {
    // 1. Register middleware
    context.registerPublishMiddleware(async (ctx, next) => {
      console.log(`[AUDIT] Publishing to ${ctx.topic}`);
      return next();
    });

    // 2. Register health check
    context.registerHealthCheck("audit-storage", async () => {
      return { ok: true, detail: "Audit sink connected" };
    });

    // 3. Register lifecycle hooks
    context.on("ready", () => console.log("Audit plugin initialized"));
    context.on("close", () => console.log("Audit plugin teardown complete"));
  },

  async onReady() {
    // Async preparation before traffic starts
  },

  async onClose() {
    // Async cleanup during application shutdown
  },
};

// Install the plugin
await kafka.install(auditPlugin);

// Start the application (executes plugin onReady hooks)
await kafka.start();
```

### Plugin Guarantees
- **Duplicate Protection**: Registering two plugins with the same name throws a `KafkaPluginError`.
- **Transactional Rollback**: If a plugin fails during `install()`, all previously registered middleware, checks, and handlers for that plugin are automatically unregistered in reverse order.
- **Shutdown Resilience**: Plugins close in **reverse order of installation**. A failure in one plugin's `onClose` does not prevent remaining plugins from shutting down cleanly.
- **Hook Timeouts**: Hook execution is bounded by `hookTimeoutMs` (default 10 seconds).

---

## Enterprise Observability

### Metrics Collection & Consumer Lag

The application maintains an in-memory metrics collector with **bounded label cardinality** (`MAX_LABEL_SETS_PER_METRIC = 100`) to guarantee that dynamic labels never trigger memory leaks:

```ts
const metrics = kafka.metrics();
console.log(metrics);
```

#### Built-in Metrics
- `kafka.producers`: Active producer instances gauge.
- `kafka.consumers`: Active consumer instances gauge.
- `kafka.pendingRequests`: Active in-flight RPC requests gauge.
- `kafka.consumer.lag`: Consumer lag gauge dimensioned by `{ topic, partition, consumer }`.
- `kafka.shutdown.durationMs`: Duration of the graceful shutdown sequence in milliseconds.

#### Registering Custom Metric Collectors
```ts
kafka.registerMetricCollector({
  increment(name, delta, labels) { /* your Prometheus / Datadog exporter */ },
  gauge(name, value, labels) { /* ... */ },
  timing(name, valueMs, labels) { /* ... */ },
});
```

A ready-to-use Prometheus `MetricsCollector` adapter is provided in `examples/prometheus-collector.js` (pre-deployment checklist #2). It wraps `prom-client` and maps `increment`/`gauge`/`timing` to Prometheus counters, gauges, and histograms:

```ts
import { Kafka } from "@oneunit/kafka";
import { createPrometheusCollector } from "./examples/prometheus-collector.js";
import { register } from "prom-client";

const kafka = await Kafka.create({ brokers: ["localhost:9092"] });
kafka.registerMetricCollector(createPrometheusCollector(register));

// Expose on your HTTP server:
// app.get("/metrics", async (req, res) => {
//   res.set("Content-Type", register.contentType);
//   res.end(await register.metrics());
// });
```

---

### Health Probes (Liveness & Readiness)

Built-in health probes suitable for Kubernetes liveness and readiness probes:

```ts
// 1. Quick high-level health report
const health = await kafka.health();
// { state: "ready", brokers: ["localhost:9092"], connected: true }

// 2. Liveness check (is the Node.js process alive and runtime not closed?)
const liveness = kafka.liveness();
// { alive: true, state: "ready" }

// 3. Readiness check (can the application perform Kafka operations?)
const readiness = await kafka.readiness();
// { ready: true, state: "ready", reasons: [] }
```

---

### Distributed Tracing (W3C / OpenTelemetry)

Pass OpenTelemetry tracing configuration to automatically inject and extract W3C `traceparent` and `tracestate` headers across message boundaries:

```ts
const kafka = await Kafka.create({
  brokers: ["localhost:9092"],
  tracing: { serviceName: "order-service" }, // Automatically initializes OpenTelemetry tracer
});
```

Alternatively, pass a custom OpenTelemetry `Tracer` instance:

```ts
import { trace } from "@opentelemetry/api";

const kafka = await Kafka.create({
  brokers: ["localhost:9092"],
  tracing: { tracer: trace.getTracer("order-service") },
});
```

When configured, `tracingMiddleware` (injecting spans into outgoing Kafka headers) and `consumeTracingMiddleware` (extracting parent context from incoming headers) are auto-registered.

---

### Diagnostics & Secret Redaction

Inspect runtime status without leaking passwords, tokens, or certificate material:

```ts
const diag = kafka.diagnostics();
console.log(diag);
// {
//   state: "ready",
//   brokers: ["localhost:9092"],
//   producers: [{ name: "default", state: "connected" }],
//   consumers: [{ name: "default", state: "running" }]
// }
```

Sensitive values in errors and diagnostics are sanitized using `KafkaError.redact(value)` (e.g. `"password123"` $\to$ `"pa***"`).

---

## Security & Authentication

### SASL (PLAIN, SCRAM, AWS IAM, OAuthBearer)

```ts
// PLAIN / SCRAM
const kafka = await Kafka.create({
  brokers: ["broker1:9092"],
  sasl: {
    mechanism: "scram-sha-512", // "plain" | "scram-sha-256" | "scram-sha-512" | "aws" | "oauthbearer"
    username: "app-user",
    password: "app-secret-password",
  },
});

// AWS IAM (for AWS MSK)
const kafkaMsk = await Kafka.create({
  brokers: ["msk-broker:9092"],
  sasl: {
    mechanism: "aws",
    accessKeyId: process.env.AWS_ACCESS_KEY_ID,
    secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY,
    sessionToken: process.env.AWS_SESSION_TOKEN,
  },
});

// OAuthBearer with token provider
const kafkaOauth = await Kafka.create({
  brokers: ["broker1:9092"],
  ssl: true, // Note: OAuthBearer requires TLS (ssl) to be configured
  sasl: {
    mechanism: "oauthbearer",
    oauthTokenProvider: async () => {
      const { token, expiresIn } = await fetchOidcToken();
      return { access_token: token, expires_in: expiresIn };
    },
  },
});
```

### Secrets as files (pre-deployment checklist #3)

SASL passwords and OAuth tokens read from environment variables persist in process memory for the lifetime of the Node.js process. For containerized deployments, mount secrets as files and read them at construction time instead of passing raw values inline:

```ts
import fs from "node:fs";

const kafka = await Kafka.create({
  brokers: ["broker1:9092"],
  sasl: {
    mechanism: "scram-sha-512",
    username: fs.readFileSync("/secrets/sasl-username", "utf8").trim(),
    password: fs.readFileSync("/secrets/sasl-password", "utf8").trim(),
  },
  ssl: {
    ca: ["/secrets/ca.pem"],
    cert: "/secrets/client-cert.pem",
    key: "/secrets/client-key.pem",
  },
});
```

When using Kubernetes, mount these as `secret` volumes rather than inline `env` values so they are not exposed in `kubectl describe` or process inspection.

---

### TLS / SSL

Supports file paths and inline PEM strings interchangeably:

```ts
import fs from "node:fs";

const kafka = await Kafka.create({
  brokers: ["secure-broker:9093"],
  ssl: {
    rejectUnauthorized: true,
    // File paths or raw PEM strings
    ca: ["/certs/ca.pem"],
    cert: fs.readFileSync("/certs/client-cert.pem", "utf8"),
    key: "/certs/client-key.pem",
  },
});
```

---

## Serialization, Codecs & Schemas

### Codecs (JSON & Bytes)

Codecs conform to a synchronous `Codec` interface:

```ts
export interface Codec {
  name: string;
  encode(value: unknown): Buffer | string | null | undefined;
  decode(bytes: unknown): unknown;
}
```

```ts
import { jsonCodec, bytesCodec } from "@oneunit/kafka/messaging";

// Register custom codec (e.g. MessagePack, Avro, Protobuf)
kafka.registerCodec("msgpack", {
  name: "msgpack",
  encode: (val) => myMsgpackEncode(val),
  decode: (buf) => myMsgpackDecode(buf),
});

// Publish with specific codec
await kafka.publish("events", data, { codec: "msgpack" });
```

---

### Schema Registry Integration

Available under `@oneunit/kafka/schema`:
- `InMemorySchemaRegistry`: For local testing and staging without external infrastructure.
- `AbstractSchemaRegistry`: Base class providing wire-format framing, compatibility checks, and schema caching.
- `ConfluentSchemaRegistry`: Production client for Confluent Schema Registry with HTTP authentication and caching.

```ts
import { ConfluentSchemaRegistry } from "@oneunit/kafka/schema";

const registry = new ConfluentSchemaRegistry({
  url: "http://schema-registry:8081",
  username: "registry-user",
  password: "registry-password",
  compatibility: "BACKWARD",
});

const schema = await registry.register({
  type: "json",
  schema: JSON.stringify({ type: "object", properties: { id: { type: "string" } } }),
});

const encodedBuffer = await registry.encode(schema.id, { id: "item-1" });
const decodedObject = await registry.decode(schema.id, encodedBuffer);
```

---

### Runtime Validation (Zod, Valibot, etc.)

Pass any validator implementing `{ parse(data): T | Promise<T> }` (such as Zod) to `publish` or `subscribe`:

```ts
import { z } from "zod";

const OrderSchema = z.object({
  orderId: z.string().uuid(),
  amount: z.number().positive(),
});

// Automatically validates outgoing payload before sending
await kafka.publish("orders", { orderId: "...", amount: 99 }, {
  validator: OrderSchema,
});

// Automatically validates incoming payload before invoking handler
await kafka.subscribe("orders", async (ctx) => {
  // ctx.value is guaranteed to match the parsed schema
}, { validator: OrderSchema });
```

---

### Reserved Headers

The package reserves internal metadata header names to prevent userland collisions:
- `content-type`, `content-type-encoding`
- `schema-version`, `schema-id`
- `correlation-id`, `message-id`
- `traceparent`, `tracestate`
- `x-oneunit-trace-id`, `x-oneunit-span-id`

Use `stripReservedHeaders(headers)` and `isReservedHeader(name)` from `@oneunit/kafka/messaging`.

---

## Cluster Administration (KafkaAdmin)

Inspect topics, consumer groups, and partitions with the wrapped `KafkaAdmin` client:

```ts
await kafka.connectAdmin();
const admin = kafka.admin;

// 1. Topic Administration
await admin.createTopics([
  {
    name: "orders.v2",
    numPartitions: 6,
    replicationFactor: 3,
    configEntries: { "cleanup.policy": "delete", "retention.ms": "604800000" },
  },
]);

const topicList = await admin.listTopics();
const metadata = await admin.fetchTopicMetadata(["orders.v2"]);
console.log(`Topic partitions: ${metadata[0].partitions.length}`);

// 2. Consumer Group Administration
const groups = await admin.listGroups();
const groupDescriptions = await admin.describeGroups(["order-processor"]);
console.log("Group state:", groupDescriptions[0].state);

// 3. Delete topics or groups
await admin.deleteTopics(["deprecated-topic"]);
await admin.deleteGroups(["stale-group"]);
```

---

## Lifecycle & 10-Step Graceful Shutdown

Calling `await kafka.close()` initiates a deterministic 10-step graceful teardown:

```
Step 1:  Mark application as closing (reject new publish/consume operations)
   │
Step 2:  Halt consumer intake (stop receiving new message batches)
   │
Step 3:  Wait for all in-flight handlers to finish (up to shutdown.timeout, default 15s)
   │
Step 4:  Commit contiguous completed offsets for all consumer partitions
   │
Step 5:  Flush buffered producer batches and disconnect producer transports
   │
Step 6:  Close and disconnect all consumer transports
   │
Step 7:  Close and disconnect admin client
   │
Step 8:  Execute plugin onClose hooks in reverse order of installation
   │
Step 9:  Disconnect remaining KafkaJS client connections (generation-bumped)
   │
Step 10: Record shutdown duration metrics, clear registries, mark closed
```

### Tuning `shutdown.timeout` (pre-deployment checklist #1)

The default graceful timeout is **15,000 ms**. Set it to exceed your handler p99 latency so in-flight handlers complete before the shutdown sequence forces them to abort:

```ts
const kafka = await Kafka.create({
    brokers: ["localhost:9092"],
    shutdown: {
        graceful: true,
        timeout: 30000, // 30s — tune to handler p99 + buffer
    },
});
```

If handlers regularly exceed the timeout, they will be signaled for cancellation via `AbortSignal` and their offsets will **not** be committed (preserving at-least-once semantics but increasing redelivery on restart).

> [!NOTE]
> Cancellation is cooperative: user handlers must observe `signal.aborted` to exit early. The package cannot forcibly terminate arbitrary JavaScript work; timed-out handler side effects may continue running in the background even after the shutdown sequence moves on.

```ts
await kafka.close();
console.log(kafka.isClosed); // true
```

---

## Structured Error Hierarchy

All exceptions extend the base `KafkaError` class, providing stable error codes, operation context, resource identifiers, and retryability flags:

```ts
import {
  KafkaError,
  KafkaConfigError,
  KafkaConnectionError,
  KafkaDecodeError,
  KafkaLifecycleError,
  KafkaProducerError,
  KafkaConsumerError,
  KafkaValidationError,
  KafkaTransactionError,
  KafkaTimeoutError,
  KafkaPluginError,
} from "@oneunit/kafka/errors";

try {
  await kafka.publish("orders", payload);
} catch (error) {
  if (error instanceof KafkaConnectionError && error.retryable) {
    console.warn(`Retryable connection failure during ${error.operation}`);
  } else if (error instanceof KafkaValidationError) {
    console.error(`Validation failed for topic ${error.topic}:`, error.message);
  }
}
```

| Error Class | Stable Code | Default Retryable | Typical Trigger |
|---|---|:---:|---|
| `KafkaError` | `KAFKA_ERROR` | `false` | Base exception for all package errors |
| `KafkaConfigError` | `KAFKA_CONFIG_ERROR` | `false` | Missing broker, invalid SASL/TLS combination, illegal config keys |
| `KafkaConnectionError` | `KAFKA_CONNECTION_ERROR` | `true` | Network disconnect, broker unreachable, stale generation race |
| `KafkaDecodeError` | `KAFKA_DECODE_ERROR` | `false` | Corrupted JSON or codec deserialization failure |
| `KafkaLifecycleError` | `KAFKA_LIFECYCLE_ERROR` | `false` | Operating on a closed resource or illegal state transition |
| `KafkaProducerError` | `KAFKA_PRODUCER_ERROR` | `true` | Producer send failure, leader election in progress |
| `KafkaConsumerError` | `KAFKA_CONSUMER_ERROR` | `false` | Consumer runner failure |
| `KafkaValidationError` | `KAFKA_VALIDATION_ERROR` | `false` | Schema validation mismatch on publish or consume |
| `KafkaTransactionError` | `KAFKA_TRANSACTION_ERROR` | `false` | Aborted transaction or publishing on inactive transaction |
| `KafkaTimeoutError` | `KAFKA_TIMEOUT_ERROR` | `true` | RPC request timeout or handler execution timeout |
| `KafkaPluginError` | `KAFKA_PLUGIN_ERROR` | `false` | Duplicate plugin registration or plugin hook failure |

---

## Configuration Reference

### Flat vs. Namespaced Configuration

Configuration options can be supplied flat or within organized namespaces. Top-level flat options take precedence over namespaced options:

```ts
const kafka = await Kafka.create({
  // Flat top-level options
  brokers: ["localhost:9092"],
  clientId: "order-service",

  // Namespaced options
  connection: {
    connectionTimeout: 5000,
    requestTimeout: 30000,
  },
  producer: {
    acks: "all",
    idempotent: true,
  },
  consumer: {
    groupId: "order-workers",
    fromBeginning: false,
  },
  shutdown: {
    graceful: true,
    timeout: 15000,
  },
  kafkaJS: {
    // Escape-hatch pass-through directly to KafkaJS
    retry: { retries: 8 },
  },
});
```

### Environment Variables

When options are omitted, `@oneunit/kafka` falls back to standard environment variables:

| Environment Variable | Description | Default |
|---|---|---|
| `KAFKA_BROKERS` | Comma-separated list of broker host:port addresses | `localhost:9092` |
| `KAFKA_CLIENT_ID` | Default client identifier | `kafka-client` |
| `KAFKA_GROUP_ID` | Default consumer group ID | `workers` |
| `KAFKA_SSL` | Enable TLS (`true`/`false`) | `false` |
| `KAFKA_CA` | File path or inline PEM for CA certificate | — |
| `KAFKA_CERT` | File path or inline PEM for client certificate | — |
| `KAFKA_KEY` | File path or inline PEM for client private key | — |
| `KAFKA_SSL_REJECT_UNAUTHORIZED` | Enforce server certificate verification (`true`/`false`) | `true` |
| `KAFKA_SASL_MECHANISM` | SASL mechanism (`plain`, `scram-sha-256`, `scram-sha-512`, `aws`, `oauthbearer`) | — |
| `KAFKA_SASL_USERNAME` | SASL username | — |
| `KAFKA_SASL_PASSWORD` | SASL password | — |
| `KAFKA_LOG_LEVEL` | Log level (`debug`, `info`, `warn`, `error`, `nothing`) | `info` |

---

## Production Tuning Guide

### `maxInFlight` — consumer concurrency (§9.6)

`maxInFlight` bounds the number of concurrent handler executions across all partitions of a consumer group.

| Workload | Recommended `maxInFlight` | Rationale |
|----------|--------------------------|-----------|
| Low-latency request/reply | 1–10 | Strict ordering per partition; minimal head-of-line blocking. |
| Throughput-oriented event processing | 50–200 | Balances parallelism with memory/CPU pressure from in-flight handlers. |
| Heavy DB/I/O-bound handlers | 10–50 | Prevents connection pool exhaustion on downstream services. |

**Tuning**: Start with `maxInFlight: 50`. Monitor `kafka.consumer.lag` — if lag grows while CPU is idle, increase `maxInFlight`. If downstream errors (timeouts, pool exhaustion) increase, decrease it.

**Never set `maxInFlight: Infinity`** in production — unbounded concurrency can exhaust memory under backpressure.

### `flushIntervalMs` — buffered batch producer (§12)

The `BufferedBatchProducer` flushes on count, byte size, or time triggers.

| Trigger | Default | Production guidance |
|---------|---------|---------------------|
| `maxRecordsPerTopic` | 1,000 | Lower for latency-sensitive topics; higher for telemetry/logs. |
| `maxBufferedRecords` | 10,000 | Cap total in-memory records to bound memory usage. |
| `maxBufferedBytes` | 1 MB | Cap total buffer size; set below your container memory limit. |
| `flushIntervalMs` | 5,000 | Time-based flush ensures data isn't stranded during low traffic. |
| `maxQueuedRecords` | 50,000 | Backpressure ceiling; `send()` rejects above this. |

**Tuning**: Set `flushIntervalMs` to ~2× your p99 handler latency so batches fill before the time trigger fires. Set `maxQueuedBytes` to no more than 50% of available container memory.

### `shutdown.timeout` — graceful shutdown (§18)

The default graceful timeout is 15,000 ms.

**Tuning**: Set `shutdown.timeout` to exceed your handler p99 latency plus a safety margin (e.g., if p99 is 8s, use 15–20s). If handlers regularly exceed the timeout, they abort via `AbortSignal` and offsets are not committed — increasing redelivery on restart.

> [!NOTE]
> `AbortSignal` cancellation is cooperative: handlers must observe `signal.aborted` to exit early. Arbitrary JavaScript work cannot be forcibly terminated.

```ts
shutdown: {
    graceful: true,
    timeout: 30000, // 30s — tune to handler p99 + buffer
},
```

### `handlerTimeoutMs` — per-handler timeout (§9.6)

Optional per-handler timeout that aborts handlers exceeding the limit, preventing shutdown from blocking on deadlocked user code.

```ts
await kafka.subscribe("orders", handler, {
    groupId: "order-processor",
    handlerTimeoutMs: 10000, // Abort handlers after 10s
});
```

**Tuning**: Set to ~1.5× your expected p99 handler latency. Timeout errors are classified by your retry policy — typically `"stop"` or `"dead-letter"`.

### Dependency: `kafkajs@2.2.4`

The package depends on `kafkajs@2.2.4`, pinned without caret/tilde to prevent surprise breaking changes.

**Known vulnerabilities**: Run `npm audit --audit-level=moderate` regularly. As of this release, `kafkajs@2.2.4` has no known critical or high CVEs. Pin your lockfile (`package-lock.json`) and review upgrades with `npm outdated kafkajs` before bumping.

**Lockfile strategy**: Commit `package-lock.json` to version control. Use `npm ci` (not `npm install`) in CI to guarantee reproducible installs. Before upgrading `kafkajs`, review the changelog for breaking changes to consumer/producer/admin APIs.

---

## Escape Hatch: Direct KafkaJS Access

If you have existing code requiring direct access to the raw KafkaJS instances, use `kafka.raw`:

```ts
const rawProducer = kafka.raw.producer("default"); // KafkaJS Producer | null
const rawConsumer = kafka.raw.consumer("default"); // KafkaJS Consumer | null
const rawAdmin = kafka.raw.admin();                // KafkaJS Admin | null

// Direct calls bypass package middleware and metrics,
// but the package still safely closes and disconnects them on kafka.close().
```

---

## Legacy & Low-Level API Support

For backwards compatibility with existing codebases, the legacy runtime client and low-level transport factories remain fully supported:

```ts
import {
  createKafkaClient,
  createKafka,
  createProducer,
  createConsumer,
  registerShutdown,
} from "@oneunit/kafka";

// 1. Legacy KafkaClient
const client = createKafkaClient({ brokers: ["localhost:9092"] });
await client.send("orders", { orderId: "1" });
await client.consume("orders", async ({ value }) => { /* ... */ });
registerShutdown(client.logger, client);

// 2. Direct KafkaJS factories
const kafkaInstance = createKafka({ brokers: ["localhost:9092"] });
const producer = await createProducer(kafkaInstance);
```

To migrate gradually from legacy `KafkaClientOptions` to the modern facade:

```ts
import { createApplicationFromLegacyOptions } from "@oneunit/kafka";

const app = createApplicationFromLegacyOptions(legacyOptions);
await app.publish("orders", payload);
```

---

## Package Subpath Exports

The package exposes 22 targeted subpath exports defined in `package.json`:

| Subpath Export | Primary Responsibilities & Exports |
|---|---|
| `@oneunit/kafka` | Primary root facade: `Kafka`, `KafkaApplication`, `createApplication`, errors, codecs, etc. |
| `@oneunit/kafka/application` | `KafkaApplication` class, `createApplication`, `KafkaApplicationOptions`. |
| `@oneunit/kafka/kafka` | `Kafka` namespace and factory. |
| `@oneunit/kafka/consumer` | `KafkaConsumer`, `KafkaConsumerOptions`, `MessageHandler`, `MessageContext`. |
| `@oneunit/kafka/producer` | `BufferedBatchProducer`, batch options. |
| `@oneunit/kafka/transaction` | `KafkaTransaction`, `TransactionalProducer`, `runTransaction`. |
| `@oneunit/kafka/rpc` | `RpcClient`, `RpcServer`, `generateCorrelationId`, `responseTopicFor`. |
| `@oneunit/kafka/middleware` | `composePublish`, `composeConsume`, `composeAdmin`, middleware types, tracing middleware. |
| `@oneunit/kafka/plugins` | `PluginManager`, `KafkaPlugin`, `PluginContext`. |
| `@oneunit/kafka/resilience` | `createRetryPolicy`, `exponentialBackoff`, `DeadLetterPublisher`, `defaultClassifier`. |
| `@oneunit/kafka/observability` | `InMemoryMetricsCollector`, `HealthReporter`, `createOpenTelemetryTracer`, `noopTracer`. |
| `@oneunit/kafka/admin` | `createKafkaAdmin`, `TopicAdmin`, `GroupAdmin`, admin types. |
| `@oneunit/kafka/schema` | `InMemorySchemaRegistry`, `AbstractSchemaRegistry`, `ConfluentSchemaRegistry`. |
| `@oneunit/kafka/messaging` | `jsonCodec`, `bytesCodec`, `createMessageEnvelope`, `stripReservedHeaders`. |
| `@oneunit/kafka/errors` | Structured `KafkaError` classes and `KafkaError.redact()`. |
| `@oneunit/kafka/validation` | `isValidator`, `composeValidators`, `Validator`. |
| `@oneunit/kafka/client` | Direct KafkaJS adapter functions (`createKafka`, `createProducer`, etc.). |
| `@oneunit/kafka/logger` | Logger adapter (`createLogger`, `isLogger`, `resolveLoggerAndOptions`, `silentLogger`, `consoleLogger`). |
| `@oneunit/kafka/kafka-client` | Legacy `KafkaClient` runtime class. |
| `@oneunit/kafka/codecs` | Codec resolution and helpers. |
| `@oneunit/kafka/adapters` | Logger, config, and codec adapters. |
| `@oneunit/kafka/env` | Environment context helpers (`envString`, `envNumber`, `envBoolean`). |
| `@oneunit/kafka/utils` | `Mutex`, `Deferred`, `CleanupRegistry`, `once`, encoding utilities. |

---

## Development & Verification

```bash
# Compile TypeScript to dist/
npm run build

# Run TypeScript typechecking on src/
npm run typecheck

# Run TypeScript type tests on test/types/
npm run typecheck:types

# Run ESLint
npm run lint

# Run prepack verification
npm run pack:check

# Run complete verification suite (typecheck + packaging)
npm run verify
```

---

## License

[MIT](LICENSE) © 2026 mayank

## Testing

Tests are not included in the published package. See the repository source for
the full unit and integration test suite.

---

## Reporting Bugs

Found a bug? Please open an issue at:
<https://github.com/oneunitjs/kafka/issues>

When reporting, include the following to help us diagnose and fix quickly:

1. **Version** of `@oneunit/kafka` (check `package.json` or `npm ls @oneunit/kafka`).
2. **Node.js version** (`node --version`).
3. **Minimal reproduction** — a small code snippet that triggers the issue.
4. **Expected behavior** vs. **actual behavior**.
5. **Error stack trace** (if applicable) — paste the full output, including any `cause` chain.
6. **Kafka broker details** — broker version, SASL/TLS configuration (omit secrets).

For security vulnerabilities, please do **not** open a public issue. Instead, contact the maintainers through the repository's security advisory process.

