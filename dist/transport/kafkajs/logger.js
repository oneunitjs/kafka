const LOGGER_KEYS = ["error", "warn", "info", "debug", "log"];
const FALLBACKS = {
    error: ["warn", "info", "debug", "log"],
    warn: ["info", "debug", "log"],
    info: ["debug", "log"],
    debug: ["log"],
    log: [],
};
export const silentLogger = Object.freeze({
    error() { },
    warn() { },
    info() { },
    debug() { },
});
export const consoleLogger = Object.freeze({
    error(message, extra) {
        write(console.error, message, extra);
    },
    warn(message, extra) {
        write(console.warn, message, extra);
    },
    info(message, extra) {
        write(console.info, message, extra);
    },
    debug(message, extra) {
        write(console.debug, message, extra);
    },
});
export function isLogger(value) {
    if (!value || typeof value !== "object" || Array.isArray(value)) {
        return false;
    }
    const obj = value;
    return LOGGER_KEYS.some((key) => typeof obj[key] === "function");
}
function isBindingsFirst(value) {
    const obj = value;
    return (typeof obj.bindings === "function" &&
        typeof obj.levels === "object" &&
        obj.levels !== null);
}
function findMethod(obj, level) {
    const direct = obj[level];
    if (typeof direct === "function") {
        return direct;
    }
    for (const fallback of FALLBACKS[level] ?? []) {
        const candidate = obj[fallback];
        if (typeof candidate === "function") {
            return candidate;
        }
    }
    return undefined;
}
export function createLogger(input) {
    if (!input) {
        return consoleLogger;
    }
    if (input === silentLogger || input === consoleLogger) {
        return input;
    }
    if (typeof input === "function") {
        const fn = input;
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
        const obj = input;
        const bindingsFirst = isBindingsFirst(input);
        const child = typeof obj.child === "function" ? obj.child.bind(obj) : undefined;
        const make = (level) => (message, extra) => {
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
export function resolveLoggerAndOptions(loggerOrOptions, maybeOptions = {}) {
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
    const options = loggerOrOptions;
    return {
        logger: createLogger(options.logger),
        options: { ...options, ...maybeOptions },
    };
}
function write(fn, message, extra) {
    if (extra === undefined) {
        fn(message);
        return;
    }
    fn(message, extra);
}
