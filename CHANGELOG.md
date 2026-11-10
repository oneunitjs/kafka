# Changelog

All notable changes to `@oneunit/kafka` are documented in this file.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and this
project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [1.1.0-beta.0] - 2026-11-10

### Added

- `KafkaConsumerOptions.handlerTimeoutMs` — per-handler timeout that aborts deadlocked handlers during shutdown via `KafkaTimeoutError`, preventing `awaitInFlight` from blocking indefinitely (§9.6).
- `connectKafkaResource()` helper in `src/transport/kafkajs/factory.js` consolidating connect/error-wrap/disconnect-on-failure logic across producer, consumer, and admin factories (~30 lines of duplicated try/catch removed).
- Bug report issue template at `.github/ISSUE_TEMPLATE/bug_report.md`.
- "Reporting Bugs" section in `README.md` with issue link and required information.

### Changed

- Consumer `awaitInFlight` now checks `this.stopped` before accepting new handlers during shutdown drain (§9.4).
- `releaseInFlightSlot` fixed — removed incorrect `inFlightSemaphore += 1` that desynchronized the semaphore when handing off to waiters.
- `RpcClient.request()` refactored out of the Promise constructor anti-pattern into `executeRequest()`; timeout timer now correctly rejects pending requests.
- `HealthReporter` uses public `KafkaApplication` accessors (`isClosed`, `diagnosticResources`, `registeredHealthChecks`, `registeredDiagnosticProviders`) instead of `as unknown as` type casts.
- `adapters/logger.js` simplified to re-export `createLogger` with a `createLoggerAdapter` alias.
- `package.json` repository, bugs, and homepage URLs updated to `github.com/oneunitjs/kafka`.
- Version bumped from `1.0.0` to `1.1.0-beta.0`.
- `package.json` `files` field narrowed to `dist`, `LICENSE`, `README.md`, `ARCHITECTURE.md`, `CHANGELOG.md`, `.env.example` (removed `src` and `examples`).
- Removed dev-only directories from distribution: `bench/`, `examples/`, `scripts/`, `deploy/`, `.github/` CI workflows, `Dockerfile`, `.gitleaks.toml`.

### Fixed

- Stale `dist/` artifacts (`core/events`, `utils/deferred`, `utils/mutex`, `utils/once`) with no source counterpart removed from the package.
- Duplicate documentation removed: `TESTING_STRATEGY.md` and `docs/TESTING.md`.
- `bench/index.ts` imports updated from removed paths (`src/codecs.js`, `src/logger.js`, `src/middleware/registry.js`) to current locations (`src/adapters/codec.js`, `src/transport/kafkajs/logger.js`, `src/middleware/index.js`).
- `middleware/index.ts` now re-exports `loggingMiddleware` (was missing).
- Docker HEALTHCHECK now invokes `app.readiness()` instead of only importing the module.

## [1.0.0] - 2026-09-29

### Added

- Standalone Kafka client with KafkaJS as the only runtime dependency
- Adapters for logger, config, and codecs so other packages stay outside this library
- High-level `KafkaClient` API for produce, consume, admin, and shutdown
- Built-in JSON codec plus injectable custom codecs
- SSL and SASL configuration from options or a config adapter
- TypeScript declarations, tests, examples, and npm package metadata

### Changed

- Renamed the package from `@bootstrap-framework/kafka` to `@oneunit/kafka`, matching
  the rest of the `@oneunit` workspace. The `repository`, `bugs`, and `homepage`
  fields now point at `github.com/oneunitjs/kafka`.
- README install and import instructions, and the npm `keywords` field, now use the
  `@oneunit` scope.
- `KafkaClient` de-duplicates concurrent `getProducer()`, `getConsumer()`, and
  `getAdmin()` calls, so parallel callers share a single in-flight connection.
- `KafkaClient.registerShutdown()` resolves the producer, consumer, and admin at
  shutdown time instead of at registration time, so lazily created clients are
  disconnected.
- The handler returned by `registerShutdown()` is idempotent; a manual call followed
  by `SIGTERM` no longer disconnects everything twice.
- `subscribeToTopic` no longer reports a missing topic as the string `"undefined"`.
  It now throws `KafkaConfigError`, as the other factories already did.
- Unknown and empty-string codec names raise `KafkaConfigError` instead of silently
  falling back to JSON.
- KafkaJS log entries are normalized before they reach your logger, so a bare string,
  an `Error`, and a structured object all arrive with the message in the message
  position and the context in the bindings position.
- `createLogCreator` is exported alongside `getSslConfig` and `getSaslConfig`.
- `createKafkaMessage` and `client.send` are unchanged, but the divergence in how
  they encode string keys is now documented in `ARCHITECTURE.md`.
- The published package now ships `src` alongside `dist`. Without it the emitted
  `.js.map` and `.d.ts.map` files pointed at `../src/*.ts`, which was not in the
  tarball, so consumers got broken source maps and broken go-to-definition.
- `getSaslConfig` validates the mechanism up front and raises `KafkaConfigError`
  listing the supported values, instead of letting an unknown mechanism fail later
  as a KafkaJS protocol error. The accepted set mirrors `SASLMechanism` in KafkaJS
  (`plain`, `scram-sha-256`, `scram-sha-512`, `aws`, `oauthbearer`).
- `parseBoolean` is exported for coercing boolean-like strings.

### Fixed

**Encoding and codecs**

- `send()` ignored a per-call `codec` option and always used the client-level codec,
  so `client.send(topic, payload, { codec })` published wrongly encoded bytes. The
  option is documented in `examples/producer.js`.
- `consume()` ignored a per-call `codec` option, making per-call decoding impossible.
- `options.send` was spread after `topic` and `messages`, so
  `send("orders", payload, { send: { topic: "other" } })` published to an unintended
  topic. The explicit arguments are now authoritative.
- `resolvePartitioner` ignored a string `createPartitioner` and silently fell back to
  the default partitioner.
- `createPartitioner` given as a string was forwarded to KafkaJS as a string rather
  than resolved to a partitioner function, failing at produce time.

**Connection lifecycle**

- Concurrent `getProducer()`, `getConsumer()`, and `getAdmin()` calls each created
  their own KafkaJS client. Because KafkaJS returns a new instance per factory call,
  parallel startup leaked connections that `disconnect()` never closed.
- `client.subscribe(topic)` followed by `client.consume(topic, handler)` subscribed
  the same topic twice.
- Concurrent `consume()` calls on one topic subscribed twice. The de-duplication now
  tracks the in-flight subscription, not just the finished state, so two callers
  cannot both pass an empty check.
- `disconnect()` while a client was still connecting leaked it: the pending
  connection resolved *after* the teardown, was cached into the client, and left a
  live connection that nothing owned. A connection is now only adopted if no
  `disconnect()` happened while it was in flight; otherwise it is disconnected
  immediately and the caller gets a `KafkaConnectionError`.
- A failed connect left a rejected promise cached, so every later call rethrew the
  original error instead of retrying.

**Configuration**

- `createKafkaClient({ partitioner })` and `createKafkaClient({ createPartitioner })`
  were declared options that never reached the producer.
- `getConsumer()` forwarded client-only options (`parseJson`, `fromBeginning`,
  `codec`, `send`, `exit`) into the KafkaJS consumer config.
- `getSaslConfig({ sasl: { mechanism }, username, password })` dropped the top-level
  credentials and silently disabled SASL.
- `isLogger()` accepted option objects as loggers when they carried `config`,
  `codec`, `parseJson`, `send`, or a timeout key, which made `createKafkaClient`
  discard the supplied options and fall back to environment configuration.
- `createConfigAdapter().number()` and `.boolean()` called `this.string()`, so they
  threw once the adapter was destructured.

**Logging**

- `invoke()` collapsed a message-only log to a single argument. For a Pino-style
  logger — one that takes `(bindings, message)` and exposes `child()` — the message
  landed in the bindings slot and the message read as `undefined`, so every
  connection lifecycle log rendered as `undefined Kafka producer connected`. Both
  positions are now always passed.
- `createLogCreator` assumed KafkaJS always passes an object with a `message` field,
  dropping the message when it received a bare string and losing the text of `Error`
  entries entirely.

**TLS and SASL**

- `rejectUnauthorized` was passed through untyped, so the string `"false"` reached
  Node's TLS options and, being truthy, left certificate verification **enabled**
  when the operator had explicitly asked to disable it.
- `KAFKA_SSL_REJECT_UNAUTHORIZED` was read from `process.env` only. Supplying a
  `ConfigAdapter` made the variable silently ineffective, again leaving verification
  on. It now goes through the config adapter like every other TLS option.
- A certificate path pointing at a directory threw a raw `EISDIR` from `fs`; it now
  raises `KafkaConfigError` naming the path.

**Messages**

- `client.consume` removed `message` from the handler payload, so `offset`,
  `timestamp`, and `headers` were unreachable and `message.offset` threw
  `TypeError`. This broke `examples/consumer.js` and `examples/standalone.js`. The
  payload now carries the decoded `key` and `value` alongside the original message
  metadata.

### Documentation

- Added `ARCHITECTURE.md` describing the module layout, the two API layers, the
  argument-resolution rules, and the connection lifecycle.
- Rewrote `CONTRIBUTING.md` with setup, scripts, conventions, and the review
  checklist.
- Updated every example for the current behaviour and documented how to run it.

### Removed

- Workspace packages (`@bootstrap-framework/config`, workspace msgpack)
- Runtime dependency on `@msgpack/msgpack`

[Unreleased]: https://github.com/oneunitjs/kafka/compare/kafka-v1.1.0-beta.0...HEAD
[1.1.0-beta.0]: https://github.com/oneunitjs/kafka/releases/tag/kafka-v1.1.0-beta.0
[1.0.0]: https://github.com/oneunitjs/kafka/releases/tag/kafka-v1.0.0
