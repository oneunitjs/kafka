import { jsonCodec } from "../adapters/codec.js";
export class JsonCodec {
    name = "json";
    encode(value) {
        return jsonCodec.encode(value);
    }
    decode(bytes) {
        return jsonCodec.decode(bytes);
    }
}
export const json = new JsonCodec();
export { jsonCodec };
