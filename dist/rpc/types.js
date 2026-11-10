export function generateCorrelationId() {
    return crypto.randomUUID();
}
export function responseTopicFor(requestTopic, responseTopic) {
    return responseTopic ?? `${requestTopic}.responses`;
}
