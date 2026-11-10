import { bytesCodec } from "../adapters/codec.js";
export class BytesCodec {
    name = "bytes";
    encode(value) {
        return bytesCodec.encode(value);
    }
    decode(bytes) {
        return bytesCodec.decode(bytes);
    }
}
export const bytes = new BytesCodec();
export { bytesCodec };
