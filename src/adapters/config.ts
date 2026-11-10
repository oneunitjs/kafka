import { envBoolean, envNumber, envString } from "../core/context.js";

export interface ConfigAdapter {
    string(name: string, fallback?: string): string | undefined;
    number(name: string, fallback?: number): number | undefined;
    boolean(name: string, fallback?: boolean): boolean;
    get(name: string): unknown;
}

function isConfigAdapter(value: unknown): value is ConfigAdapter {
    return Boolean(
        value &&
            typeof value === "object" &&
            typeof (value as ConfigAdapter).string === "function",
    );
}

export function createConfigAdapter(
    source: Record<string, unknown> | NodeJS.ProcessEnv = process.env,
): ConfigAdapter {
    const readString = (name: string, fallback?: string): string | undefined => {
        const raw = source[name];
        if (raw === undefined || raw === null) {
            return fallback;
        }

        const value = String(raw).trim();
        return value === "" ? fallback : value;
    };

    const readNumber = (name: string, fallback?: number): number | undefined => {
        const raw = readString(name);
        if (raw === undefined) {
            return fallback;
        }

        const value = Number(raw);
        return Number.isFinite(value) ? value : fallback;
    };

    const readBoolean = (name: string, fallback = false): boolean => {
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

export function readConfigString(
    options: Record<string, unknown> = {},
    name: string,
    fallback?: string,
): string | undefined {
    if (isConfigAdapter(options.config)) {
        return options.config.string(name, fallback);
    }
    return envString(name, fallback);
}

export function readConfigNumber(
    options: Record<string, unknown> = {},
    name: string,
    fallback?: number,
): number | undefined {
    if (isConfigAdapter(options.config)) {
        return options.config.number(name, fallback);
    }
    return envNumber(name, fallback);
}

export function readConfigBoolean(
    options: Record<string, unknown> = {},
    name: string,
    fallback = false,
): boolean {
    if (isConfigAdapter(options.config)) {
        return options.config.boolean(name, fallback);
    }
    return envBoolean(name, fallback);
}
