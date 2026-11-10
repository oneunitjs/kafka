/**
 * Tracing hooks (§16.3).
 *
 * Provides an optional OpenTelemetry integration without making a tracing
 * library a core dependency. Trace context is propagated through Kafka
 * headers using W3C traceparent/tracestate conventions.
 */
export interface Span {
    readonly name: string;
    readonly spanId: string;
    readonly traceId: string;
    setAttribute(key: string, value: unknown): void;
    end(): void;
}
export interface Tracer {
    startSpan(name: string, attributes?: Record<string, unknown>): Span;
    /**
     * Inject the current trace context into carrier headers (e.g. Kafka
     * message headers) using W3C traceparent/tracestate conventions.
     */
    inject(span: Span, carrier: Record<string, string>): void;
    /**
     * Extract trace context from carrier headers.
     */
    extract(carrier: Record<string, string | undefined>): Span | undefined;
}
declare const NOOP_TRACER: Tracer;
/**
 * Returns a no-op tracer when OpenTelemetry is not configured.
 */
export declare function noopTracer(): Tracer;
/**
 * Create an OpenTelemetry-backed tracer. Lazily imports the OpenTelemetry
 * API so the package does not require it as a dependency. Returns the
 * no-op tracer when OpenTelemetry is unavailable.
 */
export declare function createOpenTelemetryTracer(serviceName: string, tracerProvider?: {
    getTracer(name: string, version?: string): {
        startSpan(name: string, options?: unknown): Span;
    };
}): Tracer;
export { NOOP_TRACER as defaultTracer };
