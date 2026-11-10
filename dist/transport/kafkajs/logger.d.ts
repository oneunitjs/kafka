export interface Logger {
    error(message: unknown, extra?: unknown): void;
    warn(message: unknown, extra?: unknown): void;
    info(message: unknown, extra?: unknown): void;
    debug(message: unknown, extra?: unknown): void;
    child?(bindings?: Record<string, unknown>): Logger;
}
export declare const silentLogger: Logger;
export declare const consoleLogger: Logger;
export declare function isLogger(value: unknown): value is Logger;
export declare function createLogger(input?: unknown): Logger;
export declare function resolveLoggerAndOptions(loggerOrOptions: unknown, maybeOptions?: Record<string, unknown>): {
    logger: Logger;
    options: Record<string, unknown>;
};
