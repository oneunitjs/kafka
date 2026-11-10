export function composeValidators(validators) {
    return {
        async parse(value) {
            let current = value;
            for (const v of validators) {
                current = await v.parse(current);
            }
            return current;
        },
    };
}
export function isValidator(value) {
    return Boolean(value &&
        typeof value === "object" &&
        typeof value.parse === "function");
}
