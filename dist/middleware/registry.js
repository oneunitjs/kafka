import { randomUUID } from "node:crypto";
export function correlationIdMiddleware() {
    return async (context, next) => {
        const headers = (context.headers ??= {});
        if (!headers["x-correlation-id"]) {
            headers["x-correlation-id"] = randomUUID();
        }
        await next();
    };
}
export function consumeCorrelationMiddleware() {
    return async (context, next) => {
        const headers = context.headers ?? {};
        if (!headers["x-correlation-id"]) {
            headers["x-correlation-id"] = randomUUID();
        }
        await next();
    };
}
export function timingMiddleware(logger) {
    return async (_context, next) => {
        const start = Date.now();
        try {
            await next();
        }
        finally {
            const elapsed = Date.now() - start;
            logger?.info(`publish took ${elapsed}ms`);
        }
    };
}
/**
 * Structured logging middleware. Avoids logging full payloads by default
 * (§11.4).
 */
export function loggingMiddleware(logger, options = {}) {
    return async (context, next) => {
        const start = Date.now();
        const logEntry = {
            topic: context.topic,
        };
        if (options.logPayload === true) {
            logEntry.value = context.value;
        }
        else {
            logEntry.valueType = typeof context.value;
        }
        logger?.info("publish", logEntry);
        try {
            await next();
            logger?.info("publish.complete", {
                ...logEntry,
                elapsedMs: Date.now() - start,
            });
        }
        catch (error) {
            logger?.error("publish.failed", {
                ...logEntry,
                elapsedMs: Date.now() - start,
                error: error instanceof Error ? error.message : String(error),
            });
            throw error;
        }
    };
}
/**
 * Metrics middleware. Records counters and timings with bounded labels
 * (§16.1).
 */
export function metricsMiddleware(collector) {
    return async (context, next) => {
        const start = Date.now();
        collector.increment("kafka.publish.requests", 1, { topic: context.topic });
        try {
            await next();
            collector.increment("kafka.publish.ok", 1, { topic: context.topic });
        }
        catch (error) {
            collector.increment("kafka.publish.errors", 1, { topic: context.topic });
            throw error;
        }
        finally {
            collector.timing("kafka.publish.latencyMs", Date.now() - start, { topic: context.topic });
        }
    };
}
/**
 * Header enrichment middleware. Adds standard metadata headers without
 * clobbering caller values (§12.3).
 */
export function headerEnrichmentMiddleware(options = {}) {
    return async (context, next) => {
        const headers = context.headers ?? {};
        if (options.contentType && headers["content-type"] === undefined) {
            headers["content-type"] = options.contentType;
        }
        if (options.schemaVersion && headers["schema-version"] === undefined) {
            headers["schema-version"] = options.schemaVersion;
        }
        if (options.correlationId && headers["correlation-id"] === undefined) {
            headers["correlation-id"] = options.correlationId;
        }
        await next();
    };
}
/**
 * Payload validation middleware. Validates the outgoing value against a
 * Validator before the transport is invoked (§12.2).
 */
export function payloadValidationMiddleware(validator) {
    return async (context, next) => {
        context.value = (await validator.parse(context.value));
        await next();
    };
}
/**
 * Redaction middleware. Removes or masks sensitive fields from headers and
 * metadata before the transport is invoked (§11.4).
 */
export function redactionMiddleware(options = {}) {
    const redactHeaders = new Set((options.redactHeaders ?? []).map((h) => h.toLowerCase()));
    const redactMetadata = new Set((options.redactMetadata ?? []).map((m) => m.toLowerCase()));
    return async (context, next) => {
        if (context.headers && redactHeaders.size > 0) {
            for (const name of Object.keys(context.headers)) {
                if (redactHeaders.has(name.toLowerCase())) {
                    context.headers[name] = "[redacted]";
                }
            }
        }
        if (context.metadata && redactMetadata.size > 0) {
            for (const key of Object.keys(context.metadata)) {
                if (redactMetadata.has(key.toLowerCase())) {
                    context.metadata[key] = "[redacted]";
                }
            }
        }
        await next();
    };
}
/**
 * Tracing middleware for publish. Starts a span, injects trace context into
 * headers via the tracer, and ends the span on completion (§16.3, §11.4).
 */
export function tracingMiddleware(tracer) {
    return async (context, next) => {
        const span = tracer.startSpan(`kafka.publish:${context.topic}`, {
            "messaging.system": "kafka",
            "messaging.destination": context.topic,
        });
        try {
            const carrier = {};
            tracer.inject(span, carrier);
            for (const [k, v] of Object.entries(carrier)) {
                if (context.headers[k] === undefined) {
                    context.headers[k] = v;
                }
            }
            await next();
        }
        finally {
            span.end();
        }
    };
}
/**
 * Tracing middleware for consume. Extracts trace context from incoming
 * headers and attributes it to the span (§16.3, §11.4).
 */
export function consumeTracingMiddleware(tracer) {
    return async (context, next) => {
        const span = tracer.startSpan(`kafka.consume:${context.topic}`, {
            "messaging.system": "kafka",
            "messaging.destination": context.topic,
        });
        try {
            const extracted = tracer.extract(context.headers);
            if (extracted) {
                span.setAttribute("traceId", extracted.traceId);
                span.setAttribute("spanId", extracted.spanId);
            }
            await next();
        }
        finally {
            span.end();
        }
    };
}
export { correlationIdMiddleware as defaultCorrelationIdMiddleware };
