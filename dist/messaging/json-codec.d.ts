import { jsonCodec, type Codec } from "../adapters/codec.js";
export declare class JsonCodec implements Codec {
    name: string;
    encode(value: unknown): string | Buffer<ArrayBufferLike> | null | undefined;
    decode(bytes: unknown): unknown;
}
export declare const json: JsonCodec;
export { jsonCodec };
