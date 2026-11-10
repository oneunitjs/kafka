import { Kafka } from "kafkajs";
import { createProducer } from "../transport/kafkajs/factory.js";
import { KafkaConsumer } from "../consumer/consumer.js";
import { generateCorrelationId, responseTopicFor, } from "./types.js";
import { KafkaConnectionError, KafkaLifecycleError } from "../errors/index.js";
const DEFAULT_TIMEOUT_MS = 30000;
export class RpcClient {
    kafka;
    defaultTimeoutMs;
    producer = null;
    responseConsumer = null;
    responseTopic = null;
    pending = new Map();
    closed = false;
    metrics = {
        requestsSent: 0,
        requestsResolved: 0,
        requestsRejected: 0,
        requestsTimedOut: 0,
        responsesReceived: 0,
        responsesUnmatched: 0,
        pendingRequests: 0,
    };
    constructor(kafka, options = {}) {
        this.kafka = kafka;
        this.defaultTimeoutMs = options.defaultTimeoutMs ?? DEFAULT_TIMEOUT_MS;
    }
    /**
     * Send a request and wait for the response (§14.1).
     *
     * A correlation ID is generated (or supplied) and forwarded in message
     * headers. The response consumer listens on the response topic and
     * resolves/rejects the pending promise when a matching response arrives.
     * A per-request timeout rejects the promise if no response arrives.
     */
    async request(topic, payload, options = {}) {
        this.ensureOpen();
        const timeoutMs = options.timeoutMs ?? this.defaultTimeoutMs;
        const correlationId = options.correlationId ?? generateCorrelationId();
        const topicResponse = responseTopicFor(topic, options.responseTopic);
        return new Promise((resolve, reject) => {
            const timer = setTimeout(() => {
                this.pending.delete(correlationId);
                this.metrics.pendingRequests = this.pending.size;
                this.metrics.requestsTimedOut += 1;
                reject(new KafkaConnectionError(`RPC request to "${topic}" timed out after ${timeoutMs}ms`, { code: "KAFKA_CONNECTION_ERROR", topic, retryable: true }));
            }, timeoutMs);
            const pending = { resolve, reject, timer };
            this.pending.set(correlationId, pending);
            this.metrics.pendingRequests = this.pending.size;
            this.metrics.requestsSent += 1;
            this.executeRequest(topic, payload, correlationId, topicResponse, options)
                .then(() => {
                // Request sent successfully. The response arrives
                // asynchronously via handleResponse, which resolves the
                // pending entry. Do not resolve here.
            })
                .catch((error) => {
                clearTimeout(timer);
                this.pending.delete(correlationId);
                this.metrics.pendingRequests = this.pending.size;
                this.metrics.requestsRejected += 1;
                reject(error);
            });
        });
    }
    /**
     * Executes the RPC request: acquires producer and response consumer,
     * sends the request envelope, and waits for the response consumer to
     * resolve the pending entry via handleResponse.
     */
    async executeRequest(topic, payload, correlationId, topicResponse, options) {
        if (this.closed) {
            throw new KafkaConnectionError("RpcClient closed before response arrived", {
                code: "KAFKA_CONNECTION_ERROR",
                retryable: false,
            });
        }
        const producer = await this.getProducer();
        if (this.closed) {
            throw new KafkaConnectionError("RpcClient closed before response arrived", {
                code: "KAFKA_CONNECTION_ERROR",
                retryable: false,
            });
        }
        await this.getResponseConsumer(topicResponse);
        if (this.closed) {
            throw new KafkaConnectionError("RpcClient closed before response arrived", {
                code: "KAFKA_CONNECTION_ERROR",
                retryable: false,
            });
        }
        const envelope = {
            correlationId,
            requestTopic: topic,
            payload,
            timestamp: new Date().toISOString(),
        };
        await producer.send({
            topic,
            messages: [
                {
                    key: Buffer.from(correlationId),
                    value: Buffer.from(JSON.stringify(envelope)),
                    headers: {
                        correlationId,
                        replyTo: topicResponse,
                        requestTopic: topic,
                    },
                },
            ],
            ...(options.send ?? {}),
        });
    }
    async getProducer() {
        if (!this.producer) {
            this.producer = await createProducer(this.kafka, {});
        }
        return this.producer;
    }
    async getResponseConsumer(topicResponse) {
        if (this.responseConsumer && this.responseTopic === topicResponse) {
            return this.responseConsumer;
        }
        // Close any existing response consumer if the topic changed.
        if (this.responseConsumer) {
            try {
                await this.responseConsumer.close();
            }
            catch {
                // non-fatal
            }
        }
        const groupId = `rpc-client-${crypto.randomUUID()}`;
        const consumer = new KafkaConsumer(groupId, this.kafka, {
            fromBeginning: false,
        });
        await consumer.subscribe(topicResponse, async (message) => {
            await this.handleResponse(message.value);
        }, { fromBeginning: false });
        await consumer.start();
        this.responseConsumer = consumer;
        this.responseTopic = topicResponse;
        return consumer;
    }
    async handleResponse(value) {
        let envelope;
        try {
            envelope = JSON.parse(String(value ?? ""));
        }
        catch {
            this.metrics.responsesUnmatched += 1;
            return;
        }
        this.metrics.responsesReceived += 1;
        const pending = this.pending.get(envelope.correlationId);
        if (!pending) {
            // Late or unmatched response (§14.2).
            this.metrics.responsesUnmatched += 1;
            return;
        }
        this.pending.delete(envelope.correlationId);
        this.metrics.pendingRequests = this.pending.size;
        clearTimeout(pending.timer);
        if (envelope.error) {
            pending.reject(new KafkaConnectionError(`RPC response error for "${envelope.requestTopic}": ${envelope.error.message}`, {
                code: "KAFKA_CONNECTION_ERROR",
                topic: envelope.requestTopic,
                retryable: false,
            }));
            this.metrics.requestsRejected += 1;
        }
        else {
            pending.resolve(envelope.payload);
            this.metrics.requestsResolved += 1;
        }
    }
    get isClosed() {
        return this.closed;
    }
    get pendingRequests() {
        return this.pending.size;
    }
    get metricsSnapshot() {
        return { ...this.metrics };
    }
    ensureOpen() {
        if (this.closed) {
            throw new KafkaLifecycleError("RpcClient is closed");
        }
    }
    async close() {
        if (this.closed) {
            return;
        }
        this.closed = true;
        // Reject all pending requests.
        for (const [, pending] of this.pending) {
            clearTimeout(pending.timer);
            pending.reject(new KafkaConnectionError("RpcClient closed before response arrived", {
                code: "KAFKA_CONNECTION_ERROR",
                retryable: false,
            }));
        }
        this.pending.clear();
        this.metrics.pendingRequests = 0;
        if (this.responseConsumer) {
            try {
                await this.responseConsumer.close();
            }
            catch {
                // non-fatal
            }
            this.responseConsumer = null;
        }
        if (this.producer) {
            try {
                await this.producer.disconnect();
            }
            catch {
                // non-fatal
            }
            this.producer = null;
        }
    }
}
