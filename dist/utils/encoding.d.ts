import { type Codec } from "../adapters/codec.js";
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
export declare function encodeValue(value: unknown, codec?: Codec): string | Buffer | null | undefined;
/**
 * Normalize an outgoing message into a KafkaJS record shape using the shared
 * encoding contract (§8.2, §8.3).
 */
export declare function normalizeOutgoing(message: unknown, codec?: Codec): {
    key?: string | Buffer | null | undefined;
    value: string | Buffer | null | undefined;
};
