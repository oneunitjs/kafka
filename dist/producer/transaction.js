import { Kafka } from "kafkajs";
import { KafkaTransactionError } from "../errors/index.js";
import { resolveCodec } from "../adapters/codec.js";
import { encodeValue } from "../utils/encoding.js";
export class KafkaTransactionImpl {
    transaction;
    codec;
    committed = false;
    aborted = false;
    constructor(transaction, codec) {
        this.transaction = transaction;
        this.codec = codec;
    }
    ensureActive() {
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
    async publish(topic, value, options = {}) {
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
    async publishMany(topic, values, options = {}) {
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
    async commit() {
        this.ensureActive();
        await this.transaction.commit();
        this.committed = true;
    }
    async abort() {
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
    isActive() {
        return !this.committed && !this.aborted && this.transaction.isActive();
    }
}
export class TransactionalProducer {
    kafka;
    codec;
    transactionalId;
    producer = null;
    transaction = null;
    txImpl = null;
    constructor(kafka, codec, transactionalId) {
        this.kafka = kafka;
        this.codec = codec;
        this.transactionalId = transactionalId;
    }
    async begin() {
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
    async commit() {
        if (!this.txImpl) {
            throw new KafkaTransactionError("No active transaction", {
                code: "KAFKA_TRANSACTION_ERROR",
                retryable: false,
            });
        }
        await this.txImpl.commit();
        await this.cleanup();
    }
    async abort() {
        if (!this.txImpl) {
            return;
        }
        try {
            await this.txImpl.abort();
        }
        catch {
            // non-fatal
        }
        await this.cleanup();
    }
    async close() {
        await this.cleanup();
    }
    async cleanup() {
        if (this.producer) {
            try {
                await this.producer.disconnect();
            }
            catch {
                // non-fatal
            }
            this.producer = null;
        }
        this.transaction = null;
        this.txImpl = null;
    }
}
export async function runTransaction(kafka, callback, options = {}) {
    const producer = new TransactionalProducer(kafka, options.codec, options.transactionalId);
    const tx = await producer.begin();
    let result;
    try {
        result = await callback(tx);
        if (tx.isActive()) {
            await tx.commit();
        }
        await producer.close();
        return result;
    }
    catch (error) {
        await producer.abort();
        throw error;
    }
}
