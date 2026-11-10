import fs from "node:fs";
import { envString, parseBoolean } from "../core/context.js";
import { KafkaConfigError } from "../errors/index.js";
import { readConfigBoolean, readConfigString } from "../adapters/config.js";
function readCert(value) {
    if (value === undefined || value === null || value === false) {
        return undefined;
    }
    if (Buffer.isBuffer(value) || Array.isArray(value)) {
        return value;
    }
    if (typeof value !== "string") {
        return undefined;
    }
    const trimmed = value.trim();
    if (trimmed === "") {
        return undefined;
    }
    if (trimmed.includes("-----BEGIN") || trimmed.includes("\n")) {
        return trimmed;
    }
    if (fs.existsSync(trimmed)) {
        if (!fs.statSync(trimmed).isFile()) {
            throw new KafkaConfigError(`TLS certificate path is not a file`);
        }
        return fs.readFileSync(trimmed, "utf8");
    }
    return trimmed;
}
export function getSslConfig(options = {}) {
    if (options.ssl === false) {
        return undefined;
    }
    if (options.ssl && typeof options.ssl === "object") {
        return options.ssl;
    }
    const ca = options.ca ?? readConfigString(options, "KAFKA_CA") ?? envString("KAFKA_CA");
    const cert = options.cert ?? readConfigString(options, "KAFKA_CERT") ?? envString("KAFKA_CERT");
    const key = options.key ?? readConfigString(options, "KAFKA_KEY") ?? envString("KAFKA_KEY");
    const enabled = options.ssl === true || readConfigBoolean(options, "KAFKA_SSL", false);
    let rejectUnauthorized = options.rejectUnauthorized;
    if (rejectUnauthorized === undefined) {
        // readConfigString also covers process.env when no config adapter is set.
        rejectUnauthorized = readConfigString(options, "KAFKA_SSL_REJECT_UNAUTHORIZED");
    }
    if (rejectUnauthorized !== undefined) {
        // A string "false" is truthy in Node's TLS options, so normalise it. Anything
        // unrecognised falls back to verifying, which is the safe direction.
        rejectUnauthorized = parseBoolean(rejectUnauthorized, true);
    }
    const caValue = readCert(ca);
    const certValue = readCert(cert);
    const keyValue = readCert(key);
    const hasMaterial = caValue !== undefined || certValue !== undefined || keyValue !== undefined;
    if (!enabled && !hasMaterial) {
        return undefined;
    }
    const ssl = {};
    if (caValue !== undefined) {
        ssl.ca = Array.isArray(caValue) ? caValue : [caValue];
    }
    if (certValue !== undefined) {
        ssl.cert = certValue;
    }
    if (keyValue !== undefined) {
        ssl.key = keyValue;
    }
    if (rejectUnauthorized !== undefined) {
        ssl.rejectUnauthorized = rejectUnauthorized;
    }
    if (Object.keys(ssl).length === 0) {
        return true;
    }
    return ssl;
}
