import { bytesCodec, type Codec } from "../adapters/codec.js";

export class BytesCodec implements Codec {
    name = "bytes";

    encode(value: unknown) {
        return bytesCodec.encode(value);
    }

    decode(bytes: unknown) {
        return bytesCodec.decode(bytes);
    }
}

export const bytes: BytesCodec = new BytesCodec();
export { bytesCodec };
