import { type KafkaClientOptions } from "../transport/kafkajs/adapter.js";
import { type Producer } from "../transport/kafkajs/adapter.js";
import { type Consumer } from "../transport/kafkajs/adapter.js";
import { type Admin } from "../transport/kafkajs/adapter.js";
import { type Logger } from "../transport/kafkajs/logger.js";
import { resolveCodec, type Codec } from "../adapters/codec.js";
import { Kafka, type RecordMetadata, type EachMessagePayload, type ProducerRecord } from "kafkajs";
export declare class KafkaClient {
    logger: Logger;
    options: KafkaClientOptions;
    kafka: Kafka;
    producer: Producer | null;
    consumer: Consumer | null;
    admin: Admin | null;
    connected: {
        producer: boolean;
        consumer: boolean;
        admin: boolean;
    };
    codec: Codec;
    private subscriptions;
    private generation;
    private producerPending;
    private consumerPending;
    private adminPending;
    private readonly failedResources;
    private closePromise;
    private closed;
    constructor(options?: KafkaClientOptions);
    static isConfigured(options: Pick<KafkaClientOptions, "brokers">): boolean;
    getProducer(options?: KafkaClientOptions["producer"]): Promise<Producer>;
    getConsumer(groupIdOrOptions: string | KafkaClientOptions["consumer"] | undefined, maybeOptions?: KafkaClientOptions["consumer"]): Promise<Consumer>;
    getAdmin(options?: KafkaClientOptions["admin"]): Promise<Admin>;
    /**
     * Generic resource acquisition with generation-based close safety (§7.4).
     * Used by getProducer/getConsumer/getAdmin to eliminate duplicated
     * closed/failed/cache/try-catch boilerplate.
     */
    private getResource;
    send(topic: string, messages: unknown | unknown[], options?: SendOptions): Promise<RecordMetadata[]>;
    private resolveCodecOption;
    /**
     * Guards against a client being connected after `disconnect()` already ran.
     * `disconnect()` bumps the generation, so a connection that was in flight at that
     * moment is torn down instead of being cached into a client that looks connected.
     */
    private adoptIfCurrent;
    private clientLevelProducerOptions;
    subscribe(topic: string, options?: ConsumeOptions): Promise<Consumer>;
    private subscribeOnce;
    consume(topicOrHandler: string | MessageHandler, handlerOrOptions: MessageHandler | ConsumeOptions | undefined, maybeOptions?: ConsumeOptions): Promise<Consumer>;
    registerShutdown(extra?: ShutdownClients): (signal: string) => Promise<void>;
    disconnect(): Promise<void>;
    /**
     * Clears the FAILED lifecycle state for all resources so reconnection can
     * be attempted. A closed client cannot be retried; CLOSED is terminal
     * (§7.2).
     */
    retry(): Promise<void>;
    /**
     * Returns the set of resources currently in FAILED state.
     */
    get failed(): ReadonlySet<"producer" | "consumer" | "admin">;
}
export declare function createKafkaClient(options?: KafkaClientOptions): KafkaClient;
interface SendOptions {
    producer?: object;
    send?: ProducerSendOptions;
    codec?: CodecInput;
}
interface ConsumeOptions {
    parseJson?: boolean;
    fromBeginning?: boolean;
    groupId?: string;
    codec?: CodecInput;
}
type CodecInput = Parameters<typeof resolveCodec>[0];
type ProducerSendOptions = Omit<ProducerRecord, "topic" | "messages">;
type MessageHandler = (payload: EachMessagePayload) => unknown | Promise<unknown>;
interface ShutdownClients {
    producer?: Producer | null;
    consumer?: Consumer | null;
    admin?: Admin | null;
    clients?: Array<{
        disconnect(): Promise<void>;
    }>;
    exit?: boolean;
}
export {};
