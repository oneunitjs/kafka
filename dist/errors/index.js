export class KafkaError extends Error {
    code;
    operation;
    topic;
    resource;
    path;
    retryable;
    constructor(message, options = {}) {
        super(message, options.cause ? { cause: options.cause } : undefined);
        this.name = "KafkaError";
        this.code = options.code;
        this.operation = options.operation;
        this.topic = options.topic;
        this.resource = options.resource;
        this.path = options.path;
        this.retryable = options.retryable;
    }
    /**
     * Redact a potentially sensitive value for safe inclusion in error
     * messages and diagnostics (§17).
     */
    static redact(value) {
        if (value === undefined || value === null) {
            return "[redacted]";
        }
        if (typeof value === "string") {
            return value.length > 4 ? `${value.slice(0, 2)}***` : "[redacted]";
        }
        return "[redacted]";
    }
    /**
     * Redact an Error and its entire `cause` chain into a single safe string
     * for logging or diagnostics (§17). Each level's message is passed through
     * `redact()` so raw secrets embedded in any cause never leak.
     *
     * Example output:
     *   [KafkaConnectionError] connection lost caused by: [Error] nested secret sk***
     */
    static redactError(error) {
        const parts = [];
        let current = error;
        let depth = 0;
        while (current && depth < 10) {
            parts.push(`[${current.constructor.name}] ${KafkaError.redact(current.message)}`);
            current = current.cause;
            depth += 1;
            if (current) {
                parts.push(" caused by:");
            }
        }
        return parts.join(" ");
    }
}
export class KafkaConfigError extends KafkaError {
    constructor(message, options = {}) {
        super(message, { ...options, code: options.code ?? "KAFKA_CONFIG_ERROR", retryable: options.retryable ?? false });
        this.name = "KafkaConfigError";
    }
}
export class KafkaConnectionError extends KafkaError {
    constructor(message, options = {}) {
        super(message, { ...options, code: options.code ?? "KAFKA_CONNECTION_ERROR", retryable: options.retryable ?? true });
        this.name = "KafkaConnectionError";
    }
}
export class KafkaDecodeError extends KafkaError {
    constructor(message, options = {}) {
        super(message, { ...options, code: options.code ?? "KAFKA_DECODE_ERROR", retryable: options.retryable ?? false });
        this.name = "KafkaDecodeError";
    }
}
export class KafkaLifecycleError extends KafkaError {
    constructor(message, options = {}) {
        super(message, { ...options, code: options.code ?? "KAFKA_LIFECYCLE_ERROR", retryable: options.retryable ?? false });
        this.name = "KafkaLifecycleError";
    }
}
export class KafkaProducerError extends KafkaError {
    constructor(message, options = {}) {
        super(message, { ...options, code: options.code ?? "KAFKA_PRODUCER_ERROR", retryable: options.retryable ?? true });
        this.name = "KafkaProducerError";
    }
}
export class KafkaConsumerError extends KafkaError {
    constructor(message, options = {}) {
        super(message, { ...options, code: options.code ?? "KAFKA_CONSUMER_ERROR", retryable: options.retryable ?? false });
        this.name = "KafkaConsumerError";
    }
}
export class KafkaValidationError extends KafkaError {
    constructor(message, options = {}) {
        super(message, { ...options, code: options.code ?? "KAFKA_VALIDATION_ERROR", retryable: options.retryable ?? false });
        this.name = "KafkaValidationError";
    }
}
export class KafkaTransactionError extends KafkaError {
    constructor(message, options = {}) {
        super(message, { ...options, code: options.code ?? "KAFKA_TRANSACTION_ERROR", retryable: options.retryable ?? false });
        this.name = "KafkaTransactionError";
    }
}
export class KafkaTimeoutError extends KafkaError {
    constructor(message, options = {}) {
        super(message, { ...options, code: options.code ?? "KAFKA_TIMEOUT_ERROR", retryable: options.retryable ?? true });
        this.name = "KafkaTimeoutError";
    }
}
export class KafkaPluginError extends KafkaError {
    constructor(message, options = {}) {
        super(message, { ...options, code: options.code ?? "KAFKA_PLUGIN_ERROR", retryable: options.retryable ?? false });
        this.name = "KafkaPluginError";
    }
}
