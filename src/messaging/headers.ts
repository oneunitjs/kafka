/**
 * Reserved header names (§12.3).
 *
 * These names are reserved for package-owned metadata. Application code may
 * read them but should not write to them, to avoid collisions with the
 * package's own serialization, tracing, and correlation metadata.
 *
 * Collision rule: when a caller-supplied header collides with a reserved
 * name, the reserved/package value wins for headers the package sets
 * (e.g. correlation-id, trace context). Callers may still provide their own
 * values for non-reserved names.
 */

export const RESERVED_HEADERS: ReadonlySet<string> = new Set([
    "content-type",
    "content-type-encoding",
    "schema-version",
    "schema-id",
    "correlation-id",
    "message-id",
    "traceparent",
    "tracestate",
    "x-oneunit-trace-id",
    "x-oneunit-span-id",
]);

export const HEADER_CONTENT_TYPE = "content-type";
export const HEADER_SCHEMA_VERSION = "schema-version";
export const HEADER_SCHEMA_ID = "schema-id";
export const HEADER_CORRELATION_ID = "correlation-id";
export const HEADER_MESSAGE_ID = "message-id";
export const HEADER_TRACEPARENT = "traceparent";
export const HEADER_TRACESTATE = "tracestate";

/**
 * Returns true when a header name is reserved by the package.
 */
export function isReservedHeader(name: string): boolean {
    return RESERVED_HEADERS.has(String(name).toLowerCase());
}

/**
 * Strip reserved headers from a caller-supplied record so the package can
 * populate them without collision. Returns a copy with reserved names removed.
 */
export function stripReservedHeaders(
    headers: Record<string, string | Buffer | undefined> | undefined,
): Record<string, string | Buffer | undefined> {
    if (!headers) {
        return {};
    }
    const out: Record<string, string | Buffer | undefined> = {};
    for (const [name, value] of Object.entries(headers)) {
        if (!isReservedHeader(name)) {
            out[name] = value;
        }
    }
    return out;
}
