import { createLogger, isLogger, resolveLoggerAndOptions, silentLogger, consoleLogger, } from "../transport/kafkajs/logger.js";
/**
 * Alias for {@link createLogger} kept for backwards compatibility.
 */
export const createLoggerAdapter = createLogger;
export { createLogger, isLogger, resolveLoggerAndOptions, silentLogger, consoleLogger };
