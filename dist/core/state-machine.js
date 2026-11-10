/**
 * Generic resource lifecycle state machine (§6, §7.2).
 *
 * Resources opt-in to a subset of lifecycle states; the guard checks that a
 * declared transition is legal before applying it. Failed states are not a
 * forced member of every state set, so producers/admin never inherit the
 * consumer `running` state.
 */
export class StateMachine {
    current;
    allowed;
    history = [];
    now;
    constructor(initial, allowed, now = () => new Date().toISOString()) {
        this.current = initial;
        this.allowed = new Map();
        for (const [state, nextStates] of Object.entries(allowed)) {
            this.allowed.set(state, new Set(nextStates));
        }
        this.now = now;
    }
    get state() {
        return this.current;
    }
    get historyEntries() {
        return this.history;
    }
    can(next) {
        return (next === this.current ||
            Boolean(this.allowed.get(this.current)?.has(next)));
    }
    transition(next) {
        if (!this.can(next)) {
            throw new Error(`Illegal state transition from "${String(this.current)}" to "${String(next)}"`);
        }
        if (next === this.current) {
            return;
        }
        this.history.push({
            from: this.current,
            to: next,
            at: this.now(),
        });
        this.current = next;
    }
    reset(initial) {
        this.current = initial;
        this.history.length = 0;
    }
}
