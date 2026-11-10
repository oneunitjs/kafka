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
export declare class StateMachine<State extends string | number | symbol> {
    private current;
    private readonly allowed;
    private readonly history;
    private readonly now;
    constructor(initial: State, allowed: Record<State, readonly State[]>, now?: () => string);
    get state(): State;
    get historyEntries(): readonly StateTransition<State>[];
    can(next: State): boolean;
    transition(next: State): void;
    reset(initial: State): void;
}
