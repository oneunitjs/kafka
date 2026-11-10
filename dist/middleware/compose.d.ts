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
import type { PublishContext, PublishMiddleware, ConsumeContext, ConsumeMiddleware, AdminContext, AdminMiddleware } from "./types.js";
export declare function composePublish<T>(middleware: ReadonlyArray<PublishMiddleware<T>>, transport: (context: PublishContext<T>) => Promise<unknown>): (context: PublishContext<T>) => Promise<unknown>;
export declare function composeConsume<T>(middleware: ReadonlyArray<ConsumeMiddleware<T>>, transport: (context: ConsumeContext<T>) => Promise<void>): (context: ConsumeContext<T>) => Promise<void>;
export type { PublishContext, PublishMiddleware, ConsumeContext, ConsumeMiddleware } from "./types.js";
/**
 * Composes admin middleware into an onion pipeline (§11.1).
 */
export declare function composeAdmin(middleware: ReadonlyArray<AdminMiddleware>, transport: (context: AdminContext) => Promise<unknown>): (context: AdminContext) => Promise<unknown>;
export type { AdminContext, AdminMiddleware } from "./types.js";
