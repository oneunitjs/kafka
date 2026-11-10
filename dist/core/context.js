export function envString(name, fallback) {
    const raw = process.env[name];
    if (raw === undefined || raw === null) {
        return fallback;
    }
    const value = String(raw).trim();
    return value === "" ? fallback : value;
}
export function envNumber(name, fallback) {
    const raw = envString(name);
    if (raw === undefined || raw === null) {
        return fallback;
    }
    const value = Number(raw);
    return Number.isFinite(value) ? value : fallback;
}
export function parseBoolean(value, fallback = false) {
    if (typeof value === "boolean") {
        return value;
    }
    if (value === undefined || value === null) {
        return fallback;
    }
    const normalized = String(value).trim().toLowerCase();
    if (["1", "true", "yes", "on"].includes(normalized)) {
        return true;
    }
    if (["0", "false", "no", "off"].includes(normalized)) {
        return false;
    }
    return fallback;
}
export function envBoolean(name, fallback = false) {
    return parseBoolean(process.env[name], fallback);
}
