/**
 * Minimal resource manager coordinating registry/slot lifecycle (§6, §7).
 */
import { type RegisteredResource } from "./resource-registry.js";
import { ResourceSlot } from "./resource-slot.js";
export declare class ResourceManager {
    private readonly registry;
    private readonly slots;
    private closed;
    get isClosed(): boolean;
    registerSlot<T>(id: string, resource: RegisteredResource): ResourceSlot<T>;
    slot<T>(id: string): ResourceSlot<T> | null;
    registrySnapshot(): readonly RegisteredResource[];
    close(id?: string): Promise<void>;
    ensureOpen(operation?: string): void;
}
