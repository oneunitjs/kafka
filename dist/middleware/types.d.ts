export interface PublishContext<T = unknown> {
    topic: string;
    value: T;
    headers: Record<string, string | Buffer>;
    metadata: Record<string, unknown>;
}
export type PublishMiddleware<T = unknown> = (context: PublishContext<T>, next: () => Promise<unknown>) => Promise<unknown>;
export interface ConsumeContext<T = unknown> {
    topic: string;
    value: T;
    key: unknown;
    partition: number;
    offset: string;
    timestamp: string;
    headers: Record<string, string | Buffer | undefined>;
    signal?: AbortSignal;
    metadata: {
        correlationId?: string;
        messageId?: string;
        contentType?: string;
        schemaVersion?: string;
    };
}
export type ConsumeMiddleware<T = unknown> = (context: ConsumeContext<T>, next: () => Promise<void>) => Promise<void>;
export type ErrorMiddleware = (error: unknown, context: Record<string, unknown>) => unknown | Promise<unknown>;
export interface AdminContext {
    operation: "createTopics" | "deleteTopics" | "fetchTopicMetadata" | "fetchOffsets" | "describeConfigs" | "alterConfigs" | "createPartitions" | "deleteRecords" | "fetchConsumerGroupOffsets" | "alterConsumerGroupOffsets" | "describeConsumerGroups" | "deleteConsumerGroups";
    args: Record<string, unknown>;
}
export type AdminMiddleware = (context: AdminContext, next: () => Promise<unknown>) => Promise<unknown>;
