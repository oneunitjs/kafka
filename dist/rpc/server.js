import { Kafka } from "kafkajs";
import { createProducer } from "../transport/kafkajs/factory.js";
import { KafkaConsumer } from "../consumer/consumer.js";
import { responseTopicFor, } from "./types.js";
import { KafkaLifecycleError } from "../errors/index.js";
export class RpcServer {
    kafka;
    producer = null;
    consumer = null;
    closed = false;
    handler = null;
    constructor(kafka, _options = {}) {
        this.kafka = kafka;
    }
    /**
     * Register a handler for requests on `topic` and start consumption.
     * One runner per consumer resource (§9.2).
     */
    async respond(topic, handler, options = {}) {
        this.ensureOpen();
        if (this.consumer) {
            throw new KafkaLifecycleError(`RpcServer already responding on "${topic}"`, { code: "KAFKA_LIFECYCLE_ERROR", retryable: false });
        }
        this.handler = handler;
        const groupId = options.groupId ?? `rpc-server-${crypto.randomUUID()}`;
        const consumer = new KafkaConsumer(groupId, this.kafka, {
            fromBeginning: options.fromBeginning ?? false,
        });
        await consumer.subscribe(topic, async (message) => {
            await this.handleRequest(message.value, topic);
        }, { fromBeginning: options.fromBeginning ?? false });
        await consumer.start();
        this.consumer = consumer;
    }
    async handleRequest(value, requestTopic) {
        let envelope;
        try {
            envelope = JSON.parse(String(value ?? ""));
        }
        catch {
            // Malformed request; cannot correlate, skip.
            return;
        }
        const correlationId = envelope.correlationId;
        const replyTo = responseTopicFor(envelope.requestTopic, undefined);
        let payload;
        let error;
        if (this.handler) {
            try {
                payload = await this.handler(envelope.payload, {
                    correlationId,
                    requestTopic,
                    headers: {},
                });
            }
            catch (handlerError) {
                error = {
                    name: handlerError instanceof Error ? handlerError.name : "Error",
                    message: handlerError instanceof Error ? handlerError.message : String(handlerError),
                };
            }
        }
        const response = {
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
    async getProducer() {
        if (!this.producer) {
            this.producer = await createProducer(this.kafka, {});
        }
        return this.producer;
    }
    get isClosed() {
        return this.closed;
    }
    ensureOpen() {
        if (this.closed) {
            throw new KafkaLifecycleError("RpcServer is closed");
        }
    }
    async close() {
        if (this.closed) {
            return;
        }
        this.closed = true;
        this.handler = null;
        if (this.consumer) {
            try {
                await this.consumer.close();
            }
            catch {
                // non-fatal
            }
            this.consumer = null;
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
