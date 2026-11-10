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

export class ResourceRegistry {
    private readonly resources = new Map<string, RegisteredResource>();

    register(resource: RegisteredResource): void {
        this.resources.set(resource.id, resource);
    }

    unregister(id: string): void {
        this.resources.delete(id);
    }

    get(id: string): RegisteredResource | undefined {
        return this.resources.get(id);
    }

    list(): readonly RegisteredResource[] {
        return [...this.resources.values()];
    }

    has(id: string): boolean {
        return this.resources.has(id);
    }

    clear(): void {
        this.resources.clear();
    }
}
