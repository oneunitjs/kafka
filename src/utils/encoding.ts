import { resolveCodec, type Codec } from "../adapters/codec.js";

/**
 * Encode a single message value/key using the same policy everywhere (§8.2).
 *
 *   - Buffer remains bytes.
 *   - Uint8Array is normalized to Buffer.
 *   - Strings are encoded to UTF-8 bytes.
 *   - Everything else uses the configured codec.
 *
 * This is the single encoding contract shared by the publish path
 * (`normalizeOutgoing`) and transactional publishing so strings are never
 * passed through raw in one path but JSON-encoded in another.
 */
export function encodeValue(
    value: unknown,
    codec?: Codec,
): string | Buffer | null | undefined {
    const c = codec ?? resolveCodec();
    if (value === undefined || value === null || Buffer.isBuffer(value)) {
        return value;
    }
    if (value instanceof Uint8Array) {
        return Buffer.from(value);
    }
    if (typeof value === "string") {
        return Buffer.from(value);
    }
    return c.encode(value);
}

/**
 * Normalize an outgoing message into a KafkaJS record shape using the shared
 * encoding contract (§8.2, §8.3).
 */
export function normalizeOutgoing(
    message: unknown,
    codec?: Codec,
): { key?: string | Buffer | null | undefined; value: string | Buffer | null | undefined } {
    const c = codec ?? resolveCodec();

    if (Buffer.isBuffer(message)) {
        return { value: message };
    }
    if (typeof message === "string") {
        return { value: Buffer.from(message) };
    }
    // Only treat as a KafkaJS record shape if it has `topic` or `messages`.
    // Plain domain objects that happen to have a `value` or `key` property
    // must be encoded whole, not destructured (§8.3).
    if (message && typeof message === "object" && ("topic" in message || "messages" in message)) {
        const msg = message as Record<string, unknown>;
        return {
            key: msg.key === undefined || msg.key === null ? undefined : encodeValue(msg.key, c),
            value: msg.value === undefined || msg.value === null ? undefined : encodeValue(msg.value, c),
        };
    }

    return { value: encodeValue(message, c) };
}
