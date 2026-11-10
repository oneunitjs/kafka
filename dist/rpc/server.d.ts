import { Kafka } from "kafkajs";
import { type RpcServerOptions } from "./types.js";
export type RpcHandler<T = unknown, R = unknown> = (request: T, context: {
    correlationId: string;
    requestTopic: string;
    headers: Record<string, unknown>;
}) => R | Promise<R>;
export declare class RpcServer<T = unknown, R = unknown> {
    private readonly kafka;
    private producer;
    private consumer;
    private closed;
    private handler;
    constructor(kafka: Kafka, _options?: RpcServerOptions);
    /**
     * Register a handler for requests on `topic` and start consumption.
     * One runner per consumer resource (§9.2).
     */
    respond(topic: string, handler: RpcHandler<T, R>, options?: RpcServerOptions): Promise<void>;
    private handleRequest;
    private getProducer;
    get isClosed(): boolean;
    private ensureOpen;
    close(): Promise<void>;
}
