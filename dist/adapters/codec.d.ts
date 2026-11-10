export interface Codec {
    name: string;
    encode(value: unknown): Buffer | string | null | undefined;
    decode(bytes: unknown): unknown;
}
export declare const jsonCodec: Codec;
export declare const bytesCodec: Codec;
export declare function createCodecAdapter(codec: Codec | string | {
    name?: string;
    encode: Codec["encode"];
    decode: Codec["decode"];
}): Codec;
export declare function resolveCodec(codec?: Codec | string | {
    encode: Codec["encode"];
    decode: Codec["decode"];
    name?: string;
}): Codec;
export declare function encode(value: unknown, codec?: Codec): Buffer | string | null | undefined;
export declare function decode(bytes: unknown, codec?: Codec): unknown;
export declare function encodeToString(value: unknown, codec?: Codec): string | null | undefined;
export declare function decodeFromString(value: string | null | undefined, codec?: Codec): unknown;
export declare function createKafkaMessage(key: unknown, value: unknown, codec?: Codec): {
    key: Buffer | string | null | undefined;
    value: Buffer | string | null | undefined;
};
export declare function parseKafkaMessage(message?: {
    key?: unknown;
    value?: unknown;
}, codec?: Codec): {
    key: unknown;
    value: unknown;
};
export declare function createKafkaMessageString(key: unknown, value: unknown, codec?: Codec): {
    key: string | null | undefined;
    value: string | null | undefined;
};
export declare function parseKafkaMessageString(message?: {
    key?: unknown;
    value?: unknown;
}, codec?: Codec): {
    key: unknown;
    value: unknown;
};
