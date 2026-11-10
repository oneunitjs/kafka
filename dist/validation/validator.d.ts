export interface Validator<T = unknown> {
    parse(value: unknown): T | Promise<T>;
}
export declare function composeValidators<T>(validators: readonly Validator<unknown>[]): Validator<T>;
export declare function isValidator(value: unknown): value is Validator;
