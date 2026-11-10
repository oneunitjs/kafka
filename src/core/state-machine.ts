/**
 * Generic resource lifecycle state machine (§6, §7.2).
 *
 * Resources opt-in to a subset of lifecycle states; the guard checks that a
 * declared transition is legal before applying it. Failed states are not a
 * forced member of every state set, so producers/admin never inherit the
 * consumer `running` state.
 */

export interface StateTransition<State> {
    readonly from: State;
    readonly to: State;
    readonly at?: string;
}

export class StateMachine<State extends string | number | symbol> {
    private current: State;
    private readonly allowed: Map<State, Set<State>>;
    private readonly history: StateTransition<State>[] = [];
    private readonly now: () => string;

    constructor(
        initial: State,
        allowed: Record<State, readonly State[]>,
        now: () => string = () => new Date().toISOString(),
    ) {
        this.current = initial;
        this.allowed = new Map();
        for (const [state, nextStates] of Object.entries(
            allowed as Record<string, readonly State[]>,
        )) {
            this.allowed.set(state as State, new Set(nextStates as readonly State[]));
        }
        this.now = now;
    }

    get state(): State {
        return this.current;
    }

    get historyEntries(): readonly StateTransition<State>[] {
        return this.history;
    }

    can(next: State): boolean {
        return (
            next === this.current ||
            Boolean(this.allowed.get(this.current)?.has(next))
        );
    }

    transition(next: State): void {
        if (!this.can(next)) {
            throw new Error(
                `Illegal state transition from "${String(this.current)}" to "${String(next)}"`,
            );
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

    reset(initial: State): void {
        this.current = initial;
        this.history.length = 0;
    }
}
