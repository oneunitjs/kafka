export interface Validator<T = unknown> {
    parse(value: unknown): T | Promise<T>;
}

export function composeValidators<T>(validators: readonly Validator<unknown>[]): Validator<T> {
    return {
        async parse(value: unknown): Promise<T> {
            let current: unknown = value;
            for (const v of validators) {
                current = await v.parse(current);
            }
            return current as T;
        },
    };
}

export function isValidator(value: unknown): value is Validator {
    return Boolean(
        value &&
            typeof value === "object" &&
            typeof (value as Record<string, unknown>).parse === "function",
    );
}
