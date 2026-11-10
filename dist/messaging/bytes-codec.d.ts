import { bytesCodec, type Codec } from "../adapters/codec.js";
export declare class BytesCodec implements Codec {
    name: string;
    encode(value: unknown): string | Buffer<ArrayBufferLike> | null | undefined;
    decode(bytes: unknown): unknown;
}
export declare const bytes: BytesCodec;
export { bytesCodec };
