import { KafkaConfigError } from "../errors/index.js";

export interface Codec {
    name: string;
    encode(value: unknown): Buffer | string | null | undefined;
    decode(bytes: unknown): unknown;
}

/**
 * This Codec contract is synchronous. If an asynchronous codec is needed,
 * wrap it so `encode`/`decode` resolve immediately (§12.1). The package does
 * not await codec results, so async codecs would silently return promises
 * instead of bytes.
 */

const namedCodecs = new Map<string, Codec>();

function toBuffer(value: unknown): Buffer | null | undefined {
    if (value === undefined || value === null) {
        return value;
    }
    if (Buffer.isBuffer(value)) {
        return value;
    }
    if (value instanceof Uint8Array) {
        return Buffer.from(value);
    }
    if (typeof value === "string") {
        return Buffer.from(value);
    }
    return Buffer.from(JSON.stringify(value));
}

export const jsonCodec: Codec = {
    name: "json",
    encode(value) {
        if (value === undefined || value === null) {
            return value;
        }
        if (Buffer.isBuffer(value) || value instanceof Uint8Array) {
            return Buffer.from(value);
        }
    if (typeof value === "string") {
        return Buffer.from(value);
    }
    return Buffer.from(JSON.stringify(value));
    },
    decode(bytes) {
        if (bytes === undefined || bytes === null) {
            return bytes;
        }

        const text = Buffer.isBuffer(bytes) || bytes instanceof Uint8Array
            ? Buffer.from(bytes).toString()
            : String(bytes);

        try {
            return JSON.parse(text);
        } catch {
            return text;
        }
    },
};

export const bytesCodec: Codec = {
    name: "bytes",
    encode(value) {
        return toBuffer(value);
    },
    decode(bytes) {
        if (bytes === undefined || bytes === null) {
            return bytes;
        }
        return Buffer.isBuffer(bytes) ? bytes : Buffer.from(bytes as string | Uint8Array);
    },
};

namedCodecs.set("json", jsonCodec);
namedCodecs.set("bytes", bytesCodec);

export function createCodecAdapter(codec: Codec | string | { name?: string; encode: Codec["encode"]; decode: Codec["decode"] }): Codec {
    if (typeof codec === "string") {
        const named = namedCodecs.get(codec);
        if (!named) {
            throw new KafkaConfigError(`Unknown codec "${codec}"`);
        }
        return named;
    }

    if (!codec || typeof codec.encode !== "function" || typeof codec.decode !== "function") {
        throw new KafkaConfigError("Codec adapter requires encode and decode functions");
    }

    return {
        name: codec.name ?? "custom",
        encode: codec.encode.bind(codec),
        decode: codec.decode.bind(codec),
    };
}

export function resolveCodec(codec?: Codec | string | { encode: Codec["encode"]; decode: Codec["decode"]; name?: string }): Codec {
    if (codec === undefined || codec === null) {
        return jsonCodec;
    }
    return createCodecAdapter(codec);
}

export function encode(value: unknown, codec: Codec = jsonCodec): Buffer | string | null | undefined {
    return codec.encode(value);
}

export function decode(bytes: unknown, codec: Codec = jsonCodec): unknown {
    return codec.decode(bytes);
}

export function encodeToString(value: unknown, codec: Codec = jsonCodec): string | null | undefined {
    const encoded = encode(value, codec);
    if (encoded === undefined || encoded === null) {
        return encoded;
    }
    return Buffer.from(encoded).toString("base64");
}

export function decodeFromString(value: string | null | undefined, codec: Codec = jsonCodec): unknown {
    if (value === undefined || value === null) {
        return value;
    }
    return decode(Buffer.from(value, "base64"), codec);
}

export function createKafkaMessage(
    key: unknown,
    value: unknown,
    codec: Codec = jsonCodec,
): { key: Buffer | string | null | undefined; value: Buffer | string | null | undefined } {
    return {
        key: key === undefined || key === null ? key : encode(key, codec),
        value: encode(value, codec),
    };
}

export function parseKafkaMessage(
    message: { key?: unknown; value?: unknown } = {},
    codec: Codec = jsonCodec,
): { key: unknown; value: unknown } {
    return {
        key: message.key === undefined || message.key === null ? undefined : decode(message.key, codec),
        value: message.value === undefined || message.value === null ? undefined : decode(message.value, codec),
    };
}

export function createKafkaMessageString(
    key: unknown,
    value: unknown,
    codec: Codec = jsonCodec,
): { key: string | null | undefined; value: string | null | undefined } {
    return {
        key: key === undefined || key === null ? key : encodeToString(key, codec),
        value: encodeToString(value, codec),
    };
}

export function parseKafkaMessageString(
    message: { key?: unknown; value?: unknown } = {},
    codec: Codec = jsonCodec,
): { key: unknown; value: unknown } {
    return {
        key: message.key === undefined || message.key === null ? undefined : decodeFromString(String(message.key), codec),
        value: message.value === undefined || message.value === null ? undefined : decodeFromString(String(message.value), codec),
    };
}
