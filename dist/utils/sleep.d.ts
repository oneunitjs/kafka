/**
 * Returns a promise that resolves after the given delay.
 * Centralized sleep utility used across the codebase (§10.2).
 */
export declare function sleep(ms: number): Promise<void>;
