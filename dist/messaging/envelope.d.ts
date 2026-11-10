export interface KafkaMessageEnvelope<T = unknown> {
    readonly topic: string;
    readonly value: T;
    readonly key?: unknown;
    readonly headers?: Record<string, string | Buffer | undefined>;
    readonly metadata?: Record<string, unknown>;
    readonly correlationId?: string;
    readonly messageId?: string;
    readonly timestamp?: string;
}
export declare function createMessageEnvelope<T>(topic: string, value: T, options?: Partial<KafkaMessageEnvelope<T>>): KafkaMessageEnvelope<T>;
