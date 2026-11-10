import { envBoolean, envNumber, envString } from "../core/context.js";
function isConfigAdapter(value) {
    return Boolean(value &&
        typeof value === "object" &&
        typeof value.string === "function");
}
export function createConfigAdapter(source = process.env) {
    const readString = (name, fallback) => {
        const raw = source[name];
        if (raw === undefined || raw === null) {
            return fallback;
        }
        const value = String(raw).trim();
        return value === "" ? fallback : value;
    };
    const readNumber = (name, fallback) => {
        const raw = readString(name);
        if (raw === undefined) {
            return fallback;
        }
        const value = Number(raw);
        return Number.isFinite(value) ? value : fallback;
    };
    const readBoolean = (name, fallback = false) => {
        const raw = readString(name);
        if (raw === undefined) {
            return fallback;
        }
        const normalized = raw.toLowerCase();
        if (["1", "true", "yes", "on"].includes(normalized)) {
            return true;
        }
        if (["0", "false", "no", "off"].includes(normalized)) {
            return false;
        }
        return fallback;
    };
    return {
        get(name) {
            return source[name];
        },
        string: readString,
        number: readNumber,
        boolean: readBoolean,
    };
}
export function readConfigString(options = {}, name, fallback) {
    if (isConfigAdapter(options.config)) {
        return options.config.string(name, fallback);
    }
    return envString(name, fallback);
}
export function readConfigNumber(options = {}, name, fallback) {
    if (isConfigAdapter(options.config)) {
        return options.config.number(name, fallback);
    }
    return envNumber(name, fallback);
}
export function readConfigBoolean(options = {}, name, fallback = false) {
    if (isConfigAdapter(options.config)) {
        return options.config.boolean(name, fallback);
    }
    return envBoolean(name, fallback);
}
