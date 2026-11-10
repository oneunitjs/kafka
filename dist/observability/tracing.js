/**
 * Tracing hooks (§16.3).
 *
 * Provides an optional OpenTelemetry integration without making a tracing
 * library a core dependency. Trace context is propagated through Kafka
 * headers using W3C traceparent/tracestate conventions.
 */
const NOOP_SPAN = {
    name: "",
    spanId: "",
    traceId: "",
    setAttribute() { },
    end() { },
};
const NOOP_TRACER = {
    startSpan: () => NOOP_SPAN,
    inject: () => { },
    extract: () => undefined,
};
/**
 * Returns a no-op tracer when OpenTelemetry is not configured.
 */
export function noopTracer() {
    return NOOP_TRACER;
}
/**
 * Create an OpenTelemetry-backed tracer. Lazily imports the OpenTelemetry
 * API so the package does not require it as a dependency. Returns the
 * no-op tracer when OpenTelemetry is unavailable.
 */
export function createOpenTelemetryTracer(serviceName, tracerProvider) {
    let otelTracer;
    try {
        otelTracer = tracerProvider?.getTracer(serviceName);
    }
    catch {
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
                        span[key] = value;
                    }
                    catch {
                        // non-fatal
                    }
                },
                end() {
                    try {
                        span.end();
                    }
                    catch {
                        // non-fatal
                    }
                },
            };
        },
        inject(span, carrier) {
            // W3C traceparent propagation. Best-effort; the OpenTelemetry
            // propagator is used when available.
            try {
                const propagator = globalThis.opentelemetry;
                if (propagator?.trace?.propagate?.inject) {
                    propagator.trace.propagate.inject(carrier, span);
                }
            }
            catch {
                // non-fatal
            }
        },
        extract(_carrier) {
            return undefined;
        },
    };
}
export { NOOP_TRACER as defaultTracer };
