/**
 * Registry of managed resource instances by stable id or name (§6, §7.1).
 */
import type { LifecycleState } from "./lifecycle.js";
export interface RegisteredResource {
    readonly id: string;
    readonly name: string;
    readonly type: "producer" | "consumer" | "admin" | "custom";
    readonly state: LifecycleState;
}
export declare class ResourceRegistry {
    private readonly resources;
    register(resource: RegisteredResource): void;
    unregister(id: string): void;
    get(id: string): RegisteredResource | undefined;
    list(): readonly RegisteredResource[];
    has(id: string): boolean;
    clear(): void;
}
