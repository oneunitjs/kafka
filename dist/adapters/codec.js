import { KafkaConfigError } from "../errors/index.js";
/**
 * This Codec contract is synchronous. If an asynchronous codec is needed,
 * wrap it so `encode`/`decode` resolve immediately (§12.1). The package does
 * not await codec results, so async codecs would silently return promises
 * instead of bytes.
 */
const namedCodecs = new Map();
function toBuffer(value) {
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
export const jsonCodec = {
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
        }
        catch {
            return text;
        }
    },
};
export const bytesCodec = {
    name: "bytes",
    encode(value) {
        return toBuffer(value);
    },
    decode(bytes) {
        if (bytes === undefined || bytes === null) {
            return bytes;
        }
        return Buffer.isBuffer(bytes) ? bytes : Buffer.from(bytes);
    },
};
namedCodecs.set("json", jsonCodec);
namedCodecs.set("bytes", bytesCodec);
export function createCodecAdapter(codec) {
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
export function resolveCodec(codec) {
    if (codec === undefined || codec === null) {
        return jsonCodec;
    }
    return createCodecAdapter(codec);
}
export function encode(value, codec = jsonCodec) {
    return codec.encode(value);
}
export function decode(bytes, codec = jsonCodec) {
    return codec.decode(bytes);
}
export function encodeToString(value, codec = jsonCodec) {
    const encoded = encode(value, codec);
    if (encoded === undefined || encoded === null) {
        return encoded;
    }
    return Buffer.from(encoded).toString("base64");
}
export function decodeFromString(value, codec = jsonCodec) {
    if (value === undefined || value === null) {
        return value;
    }
    return decode(Buffer.from(value, "base64"), codec);
}
export function createKafkaMessage(key, value, codec = jsonCodec) {
    return {
        key: key === undefined || key === null ? key : encode(key, codec),
        value: encode(value, codec),
    };
}
export function parseKafkaMessage(message = {}, codec = jsonCodec) {
    return {
        key: message.key === undefined || message.key === null ? undefined : decode(message.key, codec),
        value: message.value === undefined || message.value === null ? undefined : decode(message.value, codec),
    };
}
export function createKafkaMessageString(key, value, codec = jsonCodec) {
    return {
        key: key === undefined || key === null ? key : encodeToString(key, codec),
        value: encodeToString(value, codec),
    };
}
export function parseKafkaMessageString(message = {}, codec = jsonCodec) {
    return {
        key: message.key === undefined || message.key === null ? undefined : decodeFromString(String(message.key), codec),
        value: message.value === undefined || message.value === null ? undefined : decodeFromString(String(message.value), codec),
    };
}
