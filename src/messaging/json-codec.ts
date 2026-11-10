import { jsonCodec, type Codec } from "../adapters/codec.js";

export class JsonCodec implements Codec {
    name = "json";

    encode(value: unknown) {
        return jsonCodec.encode(value);
    }

    decode(bytes: unknown) {
        return jsonCodec.decode(bytes);
    }
}

export const json: JsonCodec = new JsonCodec();
export { jsonCodec };
