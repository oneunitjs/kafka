/**
 * Lifecycle state constants and transition policy (§6, §7.2-7.3).
 *
 * Not every resource uses every state; the producer and admin resources
 * never take a `running` step.
 */

export type LifecycleState =
    | "new"
    | "connecting"
    | "connected"
    | "running"
    | "stopping"
    | "closing"
    | "closed"
    | "failed";

export const LIFECYCLE_TRANSITIONS: Record<LifecycleState, readonly LifecycleState[]> = {
    new: ["connecting", "closed"],
    connecting: ["connected", "failed", "closed"],
    connected: ["running", "stopping", "closing", "closed", "failed"],
    running: ["connected", "stopping", "closing", "closed", "failed"],
    stopping: ["connected", "closing", "closed", "failed"],
    closing: ["closed", "failed"],
    closed: ["new"],
    failed: ["connecting", "closed"],
};

export function isValidTransition(
    from: LifecycleState,
    to: LifecycleState,
): boolean {
    if (from === to) {
        return true;
    }
    return LIFECYCLE_TRANSITIONS[from].includes(to);
}

export function assertTransition(
    from: LifecycleState,
    to: LifecycleState,
): void {
    if (!isValidTransition(from, to)) {
        throw new Error(
            `Illegal lifecycle transition from "${from}" to "${to}"`,
        );
    }
}
