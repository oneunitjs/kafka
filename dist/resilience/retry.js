import { sleep } from "../utils/sleep.js";
const DEFAULT_MAX_ATTEMPTS = 8;
const DEFAULT_MAX_ELAPSED_TIME_MS = 300000;
const DEFAULT_BASE_DELAY_MS = 100;
const DEFAULT_MAX_DELAY_MS = 30000;
function isKafkaError(error) {
    return Boolean(error && typeof error === "object" && "code" in error);
}
export function defaultClassifier(error) {
    // Configuration and validation errors are not retryable.
    if (isKafkaError(error) && error.code === "KAFKA_CONFIG_ERROR") {
        return { action: "stop" };
    }
    // Decode errors go to the dead-letter queue rather than being retried
    // indefinitely (the payload itself is the problem, not the transport).
    if (isKafkaError(error) && error.code === "KAFKA_DECODE_ERROR") {
        return { action: "dead-letter" };
    }
    // Connection/transport errors are retryable by default.
    if (isKafkaError(error) && error.retryable === true) {
        return { action: "retry" };
    }
    // Application errors require application-specific policy; default to stop
    // to avoid silently swallowing handler failures.
    return { action: "stop" };
}
export function createRetryPolicy(options = {}) {
    return {
        maxAttempts: options.maxAttempts ?? DEFAULT_MAX_ATTEMPTS,
        maxElapsedTimeMs: options.maxElapsedTimeMs ?? DEFAULT_MAX_ELAPSED_TIME_MS,
        baseDelayMs: options.baseDelayMs ?? DEFAULT_BASE_DELAY_MS,
        maxDelayMs: options.maxDelayMs ?? DEFAULT_MAX_DELAY_MS,
        classify: options.classify ?? defaultClassifier,
    };
}
/** Exponential backoff with jitter (§10.2). */
export function exponentialBackoff(attempt, baseDelayMs, maxDelayMs, random = Math.random) {
    // delay = min(maxDelay, baseDelay * 2^attempt) + jitter
    const exponential = baseDelayMs * Math.pow(2, attempt);
    const capped = Math.min(maxDelayMs, exponential);
    const jitter = Math.floor(random() * capped * 0.2); // up to 20% jitter
    return capped + jitter;
}
/** Retry an async function according to the policy (§10.1, §10.2). */
export async function withRetry(fn, policy) {
    const start = Date.now();
    let lastError;
    for (let attempt = 0; attempt < policy.maxAttempts; attempt++) {
        try {
            return await fn();
        }
        catch (error) {
            lastError = error;
            const elapsed = Date.now() - start;
            if (elapsed >= policy.maxElapsedTimeMs) {
                break;
            }
            const disposition = policy.classify(error);
            if (disposition.action === "stop") {
                throw error;
            }
            if (disposition.action === "dead-letter") {
                throw error;
            }
            const delayMs = disposition.delayMs ?? exponentialBackoff(attempt, policy.baseDelayMs, policy.maxDelayMs);
            await sleep(delayMs);
        }
    }
    throw lastError;
}
