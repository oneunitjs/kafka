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
export declare const RESERVED_HEADERS: ReadonlySet<string>;
export declare const HEADER_CONTENT_TYPE = "content-type";
export declare const HEADER_SCHEMA_VERSION = "schema-version";
export declare const HEADER_SCHEMA_ID = "schema-id";
export declare const HEADER_CORRELATION_ID = "correlation-id";
export declare const HEADER_MESSAGE_ID = "message-id";
export declare const HEADER_TRACEPARENT = "traceparent";
export declare const HEADER_TRACESTATE = "tracestate";
/**
 * Returns true when a header name is reserved by the package.
 */
export declare function isReservedHeader(name: string): boolean;
/**
 * Strip reserved headers from a caller-supplied record so the package can
 * populate them without collision. Returns a copy with reserved names removed.
 */
export declare function stripReservedHeaders(headers: Record<string, string | Buffer | undefined> | undefined): Record<string, string | Buffer | undefined>;
