/**
 * Lifecycle state constants and transition policy (§6, §7.2-7.3).
 *
 * Not every resource uses every state; the producer and admin resources
 * never take a `running` step.
 */
export type LifecycleState = "new" | "connecting" | "connected" | "running" | "stopping" | "closing" | "closed" | "failed";
export declare const LIFECYCLE_TRANSITIONS: Record<LifecycleState, readonly LifecycleState[]>;
export declare function isValidTransition(from: LifecycleState, to: LifecycleState): boolean;
export declare function assertTransition(from: LifecycleState, to: LifecycleState): void;
