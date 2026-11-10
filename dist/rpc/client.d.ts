import { Kafka } from "kafkajs";
import { type RpcRequestOptions, type RpcMetrics } from "./types.js";
export declare class RpcClient {
    private readonly kafka;
    private readonly defaultTimeoutMs;
    private producer;
    private responseConsumer;
    private responseTopic;
    private readonly pending;
    private closed;
    private readonly metrics;
    constructor(kafka: Kafka, options?: {
        defaultTimeoutMs?: number;
        responseGroupId?: string;
    });
    /**
     * Send a request and wait for the response (§14.1).
     *
     * A correlation ID is generated (or supplied) and forwarded in message
     * headers. The response consumer listens on the response topic and
     * resolves/rejects the pending promise when a matching response arrives.
     * A per-request timeout rejects the promise if no response arrives.
     */
    request<T = unknown, R = unknown>(topic: string, payload: T, options?: RpcRequestOptions): Promise<R>;
    /**
     * Executes the RPC request: acquires producer and response consumer,
     * sends the request envelope, and waits for the response consumer to
     * resolve the pending entry via handleResponse.
     */
    private executeRequest;
    private getProducer;
    private getResponseConsumer;
    private handleResponse;
    get isClosed(): boolean;
    get pendingRequests(): number;
    get metricsSnapshot(): RpcMetrics;
    private ensureOpen;
    close(): Promise<void>;
}
