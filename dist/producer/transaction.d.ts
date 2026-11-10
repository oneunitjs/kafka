import { Kafka, type Transaction } from "kafkajs";
import { type Codec } from "../adapters/codec.js";
export interface TransactionPublishOptions {
    codec?: Codec | undefined;
    send?: Record<string, unknown>;
}
export interface KafkaTransaction {
    publish<T = unknown>(topic: string, value: T, options?: TransactionPublishOptions): Promise<void>;
    publishMany<T = unknown>(topic: string, values: readonly T[], options?: TransactionPublishOptions): Promise<void>;
    commit(): Promise<void>;
    abort(): Promise<void>;
    isActive(): boolean;
}
export declare class KafkaTransactionImpl implements KafkaTransaction {
    private readonly transaction;
    private readonly codec;
    private committed;
    private aborted;
    constructor(transaction: Transaction, codec?: Codec | undefined);
    private ensureActive;
    publish<T = unknown>(topic: string, value: T, options?: TransactionPublishOptions): Promise<void>;
    publishMany<T = unknown>(topic: string, values: readonly T[], options?: TransactionPublishOptions): Promise<void>;
    commit(): Promise<void>;
    abort(): Promise<void>;
    isActive(): boolean;
}
export declare class TransactionalProducer {
    private readonly kafka;
    private readonly codec?;
    private readonly transactionalId?;
    private producer;
    private transaction;
    private txImpl;
    constructor(kafka: Kafka, codec?: Codec | undefined, transactionalId?: string | undefined);
    begin(): Promise<KafkaTransaction>;
    commit(): Promise<void>;
    abort(): Promise<void>;
    close(): Promise<void>;
    private cleanup;
}
export declare function runTransaction<T>(kafka: Kafka, callback: (tx: KafkaTransaction) => Promise<T>, options?: {
    codec?: Codec | undefined;
    transactionalId?: string;
}): Promise<T>;
