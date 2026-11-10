# Testing Strategy — `@oneunit/kafka`

This guide explains how to run unit, integration, and Testcontainers-based tests for `@oneunit/kafka`.

## Test Suite Layers

| Layer | Command | Runs Against | Gate |
|-------|---------|--------------|------|
| Unit | `npm test` | Mocked transport, pure logic | Every PR / commit |
| Integration (Testcontainers) | `npm run test:testcontainers` | Real Kafka in Docker | Main / nightly |
| Full verification | `npm run verify` | typecheck + type tests + unit + packaging | Pre-publish |

## Prerequisites

- Node.js >= 20
- `npm install` completed
- Docker daemon running (for Testcontainers integration tests)

## 1. Unit Tests

```bash
npm test
```

Runs all `test/unit/*.test.js` with `tsx --test`. These use a fake KafkaJS transport (`createMockKafka`) and never touch a real broker.

## 2. Integration Tests (Testcontainers)

```bash
npm run test:testcontainers
```

Starts a `confluentinc/cp-kafka:7.6.1` KRaft-mode broker via Testcontainers, runs `test/integration/kafka.testcontainers.test.js`, then tears down the container.

### Container lifecycle

- Each test creates its own `Kafka` client with a unique `clientId` and `groupId` to avoid cross-test interference.
- Containers are stopped and removed in `after()` hooks to avoid leaving orphaned brokers.

### Running against a different Kafka version

Edit `test/helpers/testcontainers.js` `startKafkaContainer` default image, or pass `{ image: "confluentinc/cp-kafka:7.5.0" }` in your test setup.

## 3. Full Verification (CI Gate)

```bash
npm run verify
```

Runs `typecheck` + `typecheck:types` + `test` + `pack:verify`. This is the pre-publish gate.

## Writing New Tests

- Use `node:test` + `node:assert/strict` (matching existing convention).
- Use `createMockKafka()` from `test/helpers.js` for unit tests — never a real broker.
- Use `startKafkaContainer()` from `test/helpers/testcontainers.js` for integration tests.
- Assert one architectural invariant per test (epoch guard, contiguous commit, multi-reason pause, idempotent close).
- Avoid real timers; prefer controllable deferreds and `waitFor()` polling helpers.

## Flaky Test Policy

Flaky tests are treated as bugs. Fix or quarantine with a tracking issue; do not skip silently.
