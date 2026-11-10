/**
 * Lifecycle state constants and transition policy (§6, §7.2-7.3).
 *
 * Not every resource uses every state; the producer and admin resources
 * never take a `running` step.
 */
export const LIFECYCLE_TRANSITIONS = {
    new: ["connecting", "closed"],
    connecting: ["connected", "failed", "closed"],
    connected: ["running", "stopping", "closing", "closed", "failed"],
    running: ["connected", "stopping", "closing", "closed", "failed"],
    stopping: ["connected", "closing", "closed", "failed"],
    closing: ["closed", "failed"],
    closed: ["new"],
    failed: ["connecting", "closed"],
};
export function isValidTransition(from, to) {
    if (from === to) {
        return true;
    }
    return LIFECYCLE_TRANSITIONS[from].includes(to);
}
export function assertTransition(from, to) {
    if (!isValidTransition(from, to)) {
        throw new Error(`Illegal lifecycle transition from "${from}" to "${to}"`);
    }
}
