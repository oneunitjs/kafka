const LOGGER_KEYS = ["error", "warn", "info", "debug", "log"] as const;

const FALLBACKS: Record<string, string[]> = {
    error: ["warn", "info", "debug", "log"],
    warn: ["info", "debug", "log"],
    info: ["debug", "log"],
    debug: ["log"],
    log: [],
};

export interface Logger {
    error(message: unknown, extra?: unknown): void;
    warn(message: unknown, extra?: unknown): void;
    info(message: unknown, extra?: unknown): void;
    debug(message: unknown, extra?: unknown): void;
    child?(bindings?: Record<string, unknown>): Logger;
}

export const silentLogger: Logger = Object.freeze({
    error() {},
    warn() {},
    info() {},
    debug() {},
});

export const consoleLogger: Logger = Object.freeze({
    error(message: unknown, extra?: unknown) {
        write(console.error, message, extra);
    },
    warn(message: unknown, extra?: unknown) {
        write(console.warn, message, extra);
    },
    info(message: unknown, extra?: unknown) {
        write(console.info, message, extra);
    },
    debug(message: unknown, extra?: unknown) {
        write(console.debug, message, extra);
    },
});

export function isLogger(value: unknown): value is Logger {
    if (!value || typeof value !== "object" || Array.isArray(value)) {
        return false;
    }

    const obj = value as Record<string, unknown>;
    return LOGGER_KEYS.some((key) => typeof obj[key] === "function");
}

function isBindingsFirst(value: unknown): boolean {
    const obj = value as Record<string, unknown>;
    return (
        typeof obj.bindings === "function" &&
        typeof obj.levels === "object" &&
        obj.levels !== null
    );
}

function findMethod(obj: Record<string, unknown>, level: string): ((...args: unknown[]) => void) | undefined {
    const direct = obj[level];
    if (typeof direct === "function") {
        return direct as (...args: unknown[]) => void;
    }

    for (const fallback of FALLBACKS[level] ?? []) {
        const candidate = obj[fallback];
        if (typeof candidate === "function") {
            return candidate as (...args: unknown[]) => void;
        }
    }

    return undefined;
}

export function createLogger(input?: unknown): Logger {
    if (!input) {
        return consoleLogger;
    }

    if (input === silentLogger || input === consoleLogger) {
        return input as Logger;
    }

    if (typeof input === "function") {
        const fn = input as (level: string, message: unknown, extra?: unknown) => void;
        return {
            error(message, extra) {
                fn("error", message, extra);
            },
            warn(message, extra) {
                fn("warn", message, extra);
            },
            info(message, extra) {
                fn("info", message, extra);
            },
            debug(message, extra) {
                fn("debug", message, extra);
            },
        };
    }

    if (isLogger(input)) {
        const obj = input as unknown as Record<string, unknown>;
        const bindingsFirst = isBindingsFirst(input);
        const child = typeof obj.child === "function" ? obj.child.bind(obj) : undefined;

        const make = (level: string) => (message: unknown, extra?: unknown): void => {
            const fn = findMethod(obj, level);
            if (!fn) {
                return;
            }
            if (bindingsFirst) {
                fn.call(obj, extra, message);
                return;
            }
            if (extra === undefined) {
                fn.call(obj, message);
                return;
            }
            fn.call(obj, message, extra);
        };

        return {
            error: make("error"),
            warn: make("warn"),
            info: make("info"),
            debug: make("debug"),
            ...(child ? { child } : {}),
        };
    }

    return consoleLogger;
}

export function resolveLoggerAndOptions(
    loggerOrOptions: unknown,
    maybeOptions: Record<string, unknown> = {},
): { logger: Logger; options: Record<string, unknown> } {
    if (loggerOrOptions === undefined || loggerOrOptions === null) {
        return {
            logger: createLogger(),
            options: { ...maybeOptions },
        };
    }

    if (isLogger(loggerOrOptions)) {
        return {
            logger: createLogger(loggerOrOptions),
            options: { ...maybeOptions },
        };
    }

    const options = loggerOrOptions as Record<string, unknown>;
    return {
        logger: createLogger(options.logger),
        options: { ...options, ...maybeOptions },
    };
}

function write(fn: (...args: unknown[]) => void, message: unknown, extra?: unknown): void {
    if (extra === undefined) {
        fn(message);
        return;
    }
    fn(message, extra);
}
