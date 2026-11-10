import { Kafka, type Producer, type Transaction } from "kafkajs";
import { KafkaTransactionError } from "../errors/index.js";
import { resolveCodec, type Codec } from "../adapters/codec.js";
import { encodeValue } from "../utils/encoding.js";

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

export class KafkaTransactionImpl implements KafkaTransaction {
    private readonly transaction: Transaction;
    private readonly codec: Codec | undefined;
    private committed = false;
    private aborted = false;

    constructor(transaction: Transaction, codec?: Codec | undefined) {
        this.transaction = transaction;
        this.codec = codec;
    }

    private ensureActive(): void {
        if (this.committed || this.aborted) {
            throw new KafkaTransactionError("Transaction is already completed", {
                code: "KAFKA_TRANSACTION_ERROR",
                retryable: false,
            });
        }
        if (!this.transaction.isActive()) {
            throw new KafkaTransactionError("Transaction is no longer active", {
                code: "KAFKA_TRANSACTION_ERROR",
                retryable: false,
            });
        }
    }

    async publish<T = unknown>(topic: string, value: T, options: TransactionPublishOptions = {}): Promise<void> {
        this.ensureActive();
        const resolvedCodec = options.codec ?? this.codec ?? resolveCodec();
        const encoded = encodeValue(value, resolvedCodec);

        await this.transaction.send({
            topic,
            messages: [
                {
                    value: encoded ?? null,
                },
            ],
            ...(options.send ?? {}),
        });
    }

    async publishMany<T = unknown>(topic: string, values: readonly T[], options: TransactionPublishOptions = {}): Promise<void> {
        this.ensureActive();
        const resolvedCodec = options.codec ?? this.codec ?? resolveCodec();
        const messages = values.map((value) => {
            const encoded = encodeValue(value, resolvedCodec);
            return { value: encoded ?? null };
        });

        await this.transaction.send({
            topic,
            messages,
            ...(options.send ?? {}),
        });
    }

    async commit(): Promise<void> {
        this.ensureActive();
        await this.transaction.commit();
        this.committed = true;
    }

    async abort(): Promise<void> {
        if (this.committed) {
            throw new KafkaTransactionError("Cannot abort a committed transaction", {
                code: "KAFKA_TRANSACTION_ERROR",
                retryable: false,
            });
        }
        if (this.aborted) {
            return;
        }
        await this.transaction.abort();
        this.aborted = true;
    }

    isActive(): boolean {
        return !this.committed && !this.aborted && this.transaction.isActive();
    }
}

export class TransactionalProducer {
    private producer: Producer | null = null;
    private transaction: Transaction | null = null;
    private txImpl: KafkaTransactionImpl | null = null;

    constructor(private readonly kafka: Kafka, private readonly codec?: Codec | undefined, private readonly transactionalId?: string) {}

    async begin(): Promise<KafkaTransaction> {
        if (this.txImpl) {
            throw new KafkaTransactionError("Transaction already in progress", {
                code: "KAFKA_TRANSACTION_ERROR",
                retryable: false,
            });
        }

        this.producer = this.kafka.producer({
            transactionalId: this.transactionalId ?? `oneunit-tx-${crypto.randomUUID()}`,
            idempotent: true,
        });
        await this.producer.connect();
        this.transaction = await this.producer.transaction();
        this.txImpl = new KafkaTransactionImpl(this.transaction, this.codec);
        return this.txImpl;
    }

    async commit(): Promise<void> {
        if (!this.txImpl) {
            throw new KafkaTransactionError("No active transaction", {
                code: "KAFKA_TRANSACTION_ERROR",
                retryable: false,
            });
        }
        await this.txImpl.commit();
        await this.cleanup();
    }

    async abort(): Promise<void> {
        if (!this.txImpl) {
            return;
        }
        try {
            await this.txImpl.abort();
        } catch {
            // non-fatal
        }
        await this.cleanup();
    }

    async close(): Promise<void> {
        await this.cleanup();
    }

    private async cleanup(): Promise<void> {
        if (this.producer) {
            try {
                await this.producer.disconnect();
            } catch {
                // non-fatal
            }
            this.producer = null;
        }
        this.transaction = null;
        this.txImpl = null;
    }
}

export async function runTransaction<T>(
    kafka: Kafka,
    callback: (tx: KafkaTransaction) => Promise<T>,
    options: { codec?: Codec | undefined; transactionalId?: string } = {},
): Promise<T> {
    const producer = new TransactionalProducer(kafka, options.codec, options.transactionalId);
    const tx = await producer.begin();
    let result: T;
    try {
        result = await callback(tx);
        if (tx.isActive()) {
            await tx.commit();
        }
        await producer.close();
        return result;
    } catch (error) {
        await producer.abort();
        throw error;
    }
}
