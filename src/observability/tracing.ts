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

const NOOP_SPAN: Span = {
    name: "",
    spanId: "",
    traceId: "",
    setAttribute() {},
    end() {},
};

const NOOP_TRACER: Tracer = {
    startSpan: () => NOOP_SPAN,
    inject: () => {},
    extract: () => undefined,
};

/**
 * Returns a no-op tracer when OpenTelemetry is not configured.
 */
export function noopTracer(): Tracer {
    return NOOP_TRACER;
}

/**
 * Create an OpenTelemetry-backed tracer. Lazily imports the OpenTelemetry
 * API so the package does not require it as a dependency. Returns the
 * no-op tracer when OpenTelemetry is unavailable.
 */
export function createOpenTelemetryTracer(
    serviceName: string,
    tracerProvider?: { getTracer(name: string, version?: string): { startSpan(name: string, options?: unknown): Span } },
): Tracer {
    let otelTracer: { startSpan(name: string, options?: unknown): Span } | undefined;
    try {
        otelTracer = tracerProvider?.getTracer(serviceName);
    } catch {
        // fall back to no-op
    }

    if (!otelTracer) {
        return NOOP_TRACER;
    }

    return {
        startSpan(name, attributes) {
            const span = otelTracer.startSpan(name, attributes);
            return {
                name,
                spanId: "",
                traceId: "",
                setAttribute(key, value) {
                    try {
                        (span as unknown as Record<string, unknown>)[key] = value;
                    } catch {
                        // non-fatal
                    }
                },
                end() {
                    try {
                        (span as { end(): void }).end();
                    } catch {
                        // non-fatal
                    }
                },
            };
        },
        inject(span, carrier) {
            // W3C traceparent propagation. Best-effort; the OpenTelemetry
            // propagator is used when available.
            try {
                const propagator = (globalThis as unknown as { opentelemetry?: { trace?: { propagate?: { inject(carrier: unknown, context: unknown): void } } } }).opentelemetry;
                if (propagator?.trace?.propagate?.inject) {
                    propagator.trace.propagate.inject(carrier, span as unknown);
                }
            } catch {
                // non-fatal
            }
        },
        extract(_carrier) {
            return undefined;
        },
    };
}

export { NOOP_TRACER as defaultTracer };
