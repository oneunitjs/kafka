import { Kafka, type Producer } from "kafkajs";
import { createProducer } from "../transport/kafkajs/factory.js";
import { KafkaConsumer } from "../consumer/consumer.js";
import {
    responseTopicFor,
    type RpcResponseEnvelope,
    type RpcServerOptions,
} from "./types.js";
import { KafkaLifecycleError } from "../errors/index.js";

export type RpcHandler<T = unknown, R = unknown> = (
    request: T,
    context: { correlationId: string; requestTopic: string; headers: Record<string, unknown> },
) => R | Promise<R>;

export class RpcServer<T = unknown, R = unknown> {
    private readonly kafka: Kafka;
    private producer: Producer | null = null;
    private consumer: KafkaConsumer | null = null;
    private closed = false;
    private handler: RpcHandler<T, R> | null = null;

    constructor(
        kafka: Kafka,
        _options: RpcServerOptions = {},
    ) {
        this.kafka = kafka;
    }

    /**
     * Register a handler for requests on `topic` and start consumption.
     * One runner per consumer resource (§9.2).
     */
    async respond(
        topic: string,
        handler: RpcHandler<T, R>,
        options: RpcServerOptions = {},
    ): Promise<void> {
        this.ensureOpen();
        if (this.consumer) {
            throw new KafkaLifecycleError(
                `RpcServer already responding on "${topic}"`,
                { code: "KAFKA_LIFECYCLE_ERROR", retryable: false },
            );
        }

        this.handler = handler;
        const groupId = options.groupId ?? `rpc-server-${crypto.randomUUID()}`;

        const consumer = new KafkaConsumer(groupId, this.kafka, {
            fromBeginning: options.fromBeginning ?? false,
        });

        await consumer.subscribe(topic, async (message) => {
            await this.handleRequest(message.value as unknown, topic);
        }, { fromBeginning: options.fromBeginning ?? false });

        await consumer.start();
        this.consumer = consumer;
    }

    private async handleRequest(
        value: unknown,
        requestTopic: string,
    ): Promise<void> {
        let envelope: RpcResponseEnvelope;
        try {
            envelope = JSON.parse(String(value ?? ""));
        } catch {
            // Malformed request; cannot correlate, skip.
            return;
        }

        const correlationId = envelope.correlationId;
        const replyTo = responseTopicFor(envelope.requestTopic, undefined);

        let payload: unknown;
        let error: { name: string; message: string } | undefined;

        if (this.handler) {
            try {
                payload = await this.handler(envelope.payload as unknown as T, {
                    correlationId,
                    requestTopic,
                    headers: {},
                });
            } catch (handlerError) {
                error = {
                    name: handlerError instanceof Error ? handlerError.name : "Error",
                    message: handlerError instanceof Error ? handlerError.message : String(handlerError),
                };
            }
        }

        const response: RpcResponseEnvelope = {
            correlationId,
            requestTopic,
            payload,
            error,
            timestamp: new Date().toISOString(),
        };

        const producer = await this.getProducer();
        await producer.send({
            topic: replyTo,
            messages: [
                {
                    key: Buffer.from(correlationId),
                    value: Buffer.from(JSON.stringify(response)),
                    headers: {
                        correlationId,
                        replyTo,
                        requestTopic,
                    },
                },
            ],
        });
    }

    private async getProducer(): Promise<Producer> {
        if (!this.producer) {
            this.producer = await createProducer(this.kafka, {});
        }
        return this.producer;
    }

    get isClosed(): boolean {
        return this.closed;
    }

    private ensureOpen(): void {
        if (this.closed) {
            throw new KafkaLifecycleError("RpcServer is closed");
        }
    }

    async close(): Promise<void> {
        if (this.closed) {
            return;
        }
        this.closed = true;
        this.handler = null;

        if (this.consumer) {
            try {
                await this.consumer.close();
            } catch {
                // non-fatal
            }
            this.consumer = null;
        }
        if (this.producer) {
            try {
                await this.producer.disconnect();
            } catch {
                // non-fatal
            }
            this.producer = null;
        }
    }
}
