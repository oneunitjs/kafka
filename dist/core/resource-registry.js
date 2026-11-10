/**
 * Registry of managed resource instances by stable id or name (§6, §7.1).
 */
export class ResourceRegistry {
    resources = new Map();
    register(resource) {
        this.resources.set(resource.id, resource);
    }
    unregister(id) {
        this.resources.delete(id);
    }
    get(id) {
        return this.resources.get(id);
    }
    list() {
        return [...this.resources.values()];
    }
    has(id) {
        return this.resources.has(id);
    }
    clear() {
        this.resources.clear();
    }
}
