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

import type {
    PublishContext,
    PublishMiddleware,
    ConsumeContext,
    ConsumeMiddleware,
    AdminContext,
    AdminMiddleware,
} from "./types.js";

export function composePublish<T>(
    middleware: ReadonlyArray<PublishMiddleware<T>>,
    transport: (context: PublishContext<T>) => Promise<unknown>,
): (context: PublishContext<T>) => Promise<unknown> {
    return async (context) => {
        let index = -1;

        async function dispatch(i: number): Promise<unknown> {
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

export function composeConsume<T>(
    middleware: ReadonlyArray<ConsumeMiddleware<T>>,
    transport: (context: ConsumeContext<T>) => Promise<void>,
): (context: ConsumeContext<T>) => Promise<void> {
    return async (context) => {
        let index = -1;

        async function dispatch(i: number): Promise<void> {
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

export type { PublishContext, PublishMiddleware, ConsumeContext, ConsumeMiddleware } from "./types.js";

/**
 * Composes admin middleware into an onion pipeline (§11.1).
 */
export function composeAdmin(
    middleware: ReadonlyArray<AdminMiddleware>,
    transport: (context: AdminContext) => Promise<unknown>,
): (context: AdminContext) => Promise<unknown> {
    return async (context) => {
        let index = -1;

        async function dispatch(i: number): Promise<unknown> {
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
    }
}

export type { AdminContext, AdminMiddleware } from "./types.js";
