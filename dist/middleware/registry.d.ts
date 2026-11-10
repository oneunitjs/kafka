import type { MetricsCollector } from "../observability/types.js";
import type { Validator } from "../validation/index.js";
import type { Tracer } from "../observability/tracing.js";
import type { PublishMiddleware, ConsumeMiddleware } from "./compose.js";
export declare function correlationIdMiddleware<T extends {
    headers?: Record<string, unknown>;
} = any>(): PublishMiddleware<T>;
export declare function consumeCorrelationMiddleware(): ConsumeMiddleware;
export declare function timingMiddleware(logger: {
    info: (...args: unknown[]) => void;
} | undefined): PublishMiddleware;
/**
 * Structured logging middleware. Avoids logging full payloads by default
 * (§11.4).
 */
export declare function loggingMiddleware(logger: {
    info: (...args: unknown[]) => void;
    error: (...args: unknown[]) => void;
} | undefined, options?: {
    logPayload?: boolean;
}): PublishMiddleware;
/**
 * Metrics middleware. Records counters and timings with bounded labels
 * (§16.1).
 */
export declare function metricsMiddleware(collector: MetricsCollector): PublishMiddleware;
/**
 * Header enrichment middleware. Adds standard metadata headers without
 * clobbering caller values (§12.3).
 */
export declare function headerEnrichmentMiddleware(options?: {
    contentType?: string;
    schemaVersion?: string;
    correlationId?: string;
}): PublishMiddleware;
/**
 * Payload validation middleware. Validates the outgoing value against a
 * Validator before the transport is invoked (§12.2).
 */
export declare function payloadValidationMiddleware<T>(validator: Validator<T>): PublishMiddleware<T>;
/**
 * Redaction middleware. Removes or masks sensitive fields from headers and
 * metadata before the transport is invoked (§11.4).
 */
export declare function redactionMiddleware(options?: {
    redactHeaders?: string[];
    redactMetadata?: string[];
}): PublishMiddleware;
/**
 * Tracing middleware for publish. Starts a span, injects trace context into
 * headers via the tracer, and ends the span on completion (§16.3, §11.4).
 */
export declare function tracingMiddleware(tracer: Tracer): PublishMiddleware;
/**
 * Tracing middleware for consume. Extracts trace context from incoming
 * headers and attributes it to the span (§16.3, §11.4).
 */
export declare function consumeTracingMiddleware(tracer: Tracer): ConsumeMiddleware;
export { correlationIdMiddleware as defaultCorrelationIdMiddleware };
