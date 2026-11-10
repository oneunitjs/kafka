export interface RpcRequestOptions {
    timeoutMs?: number;
    responseTopic?: string;
    correlationId?: string;
    codec?: unknown;
    send?: Record<string, unknown>;
}

export interface RpcServerOptions {
    groupId?: string;
    fromBeginning?: boolean;
    codec?: unknown;
}

export interface RpcResponseEnvelope {
    correlationId: string;
    requestTopic: string;
    payload: unknown;
    error?: { name: string; message: string };
    timestamp: string;
}

export interface PendingRequest<R = unknown> {
    resolve: (value: R) => void;
    reject: (error: unknown) => void;
    timer: ReturnType<typeof setTimeout>;
}

export function generateCorrelationId(): string {
    return crypto.randomUUID();
}

export function responseTopicFor(requestTopic: string, responseTopic?: string): string {
    return responseTopic ?? `${requestTopic}.responses`;
}

export interface RpcMetrics {
    requestsSent: number;
    requestsResolved: number;
    requestsRejected: number;
    requestsTimedOut: number;
    responsesReceived: number;
    responsesUnmatched: number;
    pendingRequests: number;
}
