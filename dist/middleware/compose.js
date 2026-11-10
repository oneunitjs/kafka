/**
 * Composes middleware into an onion pipeline (§11.3):
 *
 *   A before
 *     B before
 *       C before
 *         transport operation
 *       C after
 *     B after
 *   A after
 *
 * Registration order is preserved for the inbound path; unwind runs in
 * reverse order. `next()` may be called at most once per middleware.
 */
export function composePublish(middleware, transport) {
    return async (context) => {
        let index = -1;
        async function dispatch(i) {
            if (i <= index) {
                throw new Error("publish middleware next() called multiple times");
            }
            index = i;
            const fn = middleware[i];
            if (!fn) {
                return transport(context);
            }
            return fn(context, () => dispatch(i + 1));
        }
        return dispatch(0);
    };
}
export function composeConsume(middleware, transport) {
    return async (context) => {
        let index = -1;
        async function dispatch(i) {
            if (i <= index) {
                throw new Error("consume middleware next() called multiple times");
            }
            index = i;
            const fn = middleware[i];
            if (!fn) {
                return transport(context);
            }
            return fn(context, () => dispatch(i + 1));
        }
        return dispatch(0);
    };
}
/**
 * Composes admin middleware into an onion pipeline (§11.1).
 */
export function composeAdmin(middleware, transport) {
    return async (context) => {
        let index = -1;
        async function dispatch(i) {
            if (i <= index) {
                throw new Error("admin middleware next() called multiple times");
            }
            index = i;
            const fn = middleware[i];
            if (!fn) {
                return transport(context);
            }
            return fn(context, () => dispatch(i + 1));
        }
        return dispatch(0);
    };
}
