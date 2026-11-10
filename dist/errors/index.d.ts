export interface KafkaErrorOptions {
    cause?: unknown;
    code?: string;
    operation?: string;
    topic?: string;
    resource?: string;
    path?: string;
    retryable?: boolean;
}
export declare class KafkaError extends Error {
    readonly code?: string;
    readonly operation?: string;
    readonly topic?: string;
    readonly resource?: string;
    readonly path?: string;
    readonly retryable?: boolean;
    constructor(message: string, options?: KafkaErrorOptions);
    /**
     * Redact a potentially sensitive value for safe inclusion in error
     * messages and diagnostics (§17).
     */
    static redact(value: unknown): string;
    /**
     * Redact an Error and its entire `cause` chain into a single safe string
     * for logging or diagnostics (§17). Each level's message is passed through
     * `redact()` so raw secrets embedded in any cause never leak.
     *
     * Example output:
     *   [KafkaConnectionError] connection lost caused by: [Error] nested secret sk***
     */
    static redactError(error: Error): string;
}
export declare class KafkaConfigError extends KafkaError {
    constructor(message: string, options?: KafkaErrorOptions);
}
export declare class KafkaConnectionError extends KafkaError {
    constructor(message: string, options?: KafkaErrorOptions);
}
export declare class KafkaDecodeError extends KafkaError {
    constructor(message: string, options?: KafkaErrorOptions);
}
export declare class KafkaLifecycleError extends KafkaError {
    constructor(message: string, options?: KafkaErrorOptions);
}
export declare class KafkaProducerError extends KafkaError {
    constructor(message: string, options?: KafkaErrorOptions);
}
export declare class KafkaConsumerError extends KafkaError {
    constructor(message: string, options?: KafkaErrorOptions);
}
export declare class KafkaValidationError extends KafkaError {
    constructor(message: string, options?: KafkaErrorOptions);
}
export declare class KafkaTransactionError extends KafkaError {
    constructor(message: string, options?: KafkaErrorOptions);
}
export declare class KafkaTimeoutError extends KafkaError {
    constructor(message: string, options?: KafkaErrorOptions);
}
export declare class KafkaPluginError extends KafkaError {
    constructor(message: string, options?: KafkaErrorOptions);
}
