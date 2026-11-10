export function createMessageEnvelope(topic, value, options = {}) {
    return {
        topic,
        value,
        key: options.key,
        headers: options.headers,
        metadata: options.metadata ?? {},
        correlationId: options.correlationId,
        messageId: options.messageId,
        timestamp: options.timestamp,
    };
}
