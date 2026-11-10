/**
 * Shared in-flight promise caching for managed resources (§7.4, §7.5).
 *
 * Caches the pending promise rather than resolving into a bare field so that
 * concurrent callers share a single creation attempt. Close races are tracked
 * through a generation counter; stale callers are rejected rather than
 * adopting a transport created after disconnect.
 */
export class ResourceSlot {
    value = null;
    pending = null;
    closePromise = null;
    closed = false;
    generation = 0;
    get isClosed() {
        return this.closed;
    }
    get current() {
        return this.value;
    }
    get currentGeneration() {
        return this.generation;
    }
    async get(create) {
        if (this.closed) {
            throw new Error("Resource slot is closed");
        }
        if (this.value) {
            return this.value;
        }
        if (this.pending) {
            return this.pending;
        }
        const generation = this.generation;
        const pending = create(generation)
            .then((value) => {
            if (generation !== this.generation) {
                // Stale creation (closed or recreated); discard it rather
                // than resurrecting into the active slot.
                this.closeTransport(value);
                throw new Error("Resource creation became stale before adoption");
            }
            this.value = value;
            return value;
        })
            .catch((error) => {
            if (this.pending === pending && !this.closed) {
                this.pending = null;
            }
            throw error;
        })
            .finally(() => {
            if (this.pending === pending) {
                this.pending = null;
            }
        });
        this.pending = pending;
        return pending;
    }
    async close(closeValue) {
        if (this.closePromise) {
            return this.closePromise;
        }
        this.generation += 1;
        const value = this.value;
        this.value = null;
        this.closed = true;
        this.closePromise = (async () => {
            if (value) {
                try {
                    await Promise.resolve(closeValue(value));
                }
                catch {
                    // Close failures are reported by callers; the slot
                    // remains terminal regardless.
                }
            }
            this.pending = null;
        })();
        return this.closePromise;
    }
    reset() {
        this.value = null;
        this.pending = null;
        this.closePromise = null;
        this.closed = false;
        this.generation = 0;
    }
    closeTransport(value) {
        const candidate = value;
        if (typeof candidate.disconnect === "function") {
            try {
                void Promise.resolve(candidate.disconnect());
            }
            catch {
                // Best-effort cleanup for stale resources.
            }
        }
    }
}
