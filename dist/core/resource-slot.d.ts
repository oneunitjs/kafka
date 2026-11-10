/**
 * Shared in-flight promise caching for managed resources (§7.4, §7.5).
 *
 * Caches the pending promise rather than resolving into a bare field so that
 * concurrent callers share a single creation attempt. Close races are tracked
 * through a generation counter; stale callers are rejected rather than
 * adopting a transport created after disconnect.
 */
export declare class ResourceSlot<T> {
    private value;
    private pending;
    private closePromise;
    private closed;
    private generation;
    get isClosed(): boolean;
    get current(): T | null;
    get currentGeneration(): number;
    get(create: (generation: number) => Promise<T>): Promise<T>;
    close(closeValue: (value: T | null) => Promise<void> | void): Promise<void>;
    reset(): void;
    private closeTransport;
}
