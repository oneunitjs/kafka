export type ErrorDisposition = {
    action: "retry";
    delayMs?: number;
} | {
    action: "dead-letter";
} | {
    action: "stop";
};
export interface RetryPolicy {
    readonly maxAttempts: number;
    readonly maxElapsedTimeMs: number;
    readonly baseDelayMs: number;
    readonly maxDelayMs: number;
    readonly classify: (error: unknown) => ErrorDisposition;
}
export interface RetryOptions {
    maxAttempts?: number;
    maxElapsedTimeMs?: number;
    baseDelayMs?: number;
    maxDelayMs?: number;
    classify?: (error: unknown) => ErrorDisposition;
}
export declare function defaultClassifier(error: unknown): ErrorDisposition;
export declare function createRetryPolicy(options?: RetryOptions): RetryPolicy;
/** Exponential backoff with jitter (§10.2). */
export declare function exponentialBackoff(attempt: number, baseDelayMs: number, maxDelayMs: number, random?: () => number): number;
/** Retry an async function according to the policy (§10.1, §10.2). */
export declare function withRetry<T>(fn: () => Promise<T>, policy: RetryPolicy): Promise<T>;
