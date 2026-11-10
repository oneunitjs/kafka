/**
 * Minimal resource manager coordinating registry/slot lifecycle (§6, §7).
 */
import { ResourceRegistry } from "./resource-registry.js";
import { ResourceSlot } from "./resource-slot.js";
import { KafkaLifecycleError } from "../errors/index.js";
export class ResourceManager {
    registry = new ResourceRegistry();
    slots = new Map();
    closed = false;
    get isClosed() {
        return this.closed;
    }
    registerSlot(id, resource) {
        this.registry.register(resource);
        const slot = new ResourceSlot();
        this.slots.set(id, slot);
        return slot;
    }
    slot(id) {
        return this.slots.get(id) ?? null;
    }
    registrySnapshot() {
        return this.registry.list();
    }
    async close(id) {
        if (id) {
            const slot = this.slots.get(id);
            if (!slot) {
                return;
            }
            await slot.close(async () => { });
            this.slots.delete(id);
            this.registry.unregister(id);
            return;
        }
        this.closed = true;
        await Promise.allSettled([...this.slots.values()].map((s) => s.close(async () => { })));
        this.slots.clear();
        this.registry.clear();
    }
    ensureOpen(operation = "resource operations") {
        if (this.closed) {
            throw new KafkaLifecycleError(`Resource manager is closed; ${operation} is no longer permitted`, {
                code: "KAFKA_LIFECYCLE_ERROR",
                operation,
                retryable: false,
            });
        }
    }
}
