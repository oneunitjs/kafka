import { createLogger, isLogger, resolveLoggerAndOptions, type Logger, silentLogger, consoleLogger } from "../transport/kafkajs/logger.js";
/**
 * Alias for {@link createLogger} kept for backwards compatibility.
 */
export declare const createLoggerAdapter: typeof createLogger;
export { createLogger, isLogger, resolveLoggerAndOptions, type Logger, silentLogger, consoleLogger };
