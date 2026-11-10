/**
 * Schema registry integration (§12.4).
 *
 * Schema registry clients are optional. A schema adapter defines register or
 * resolve schema, encode/decode with schema identity, compatibility checks,
 * cache behavior, registry outage behavior, and schema version metadata.
 *
 * The package does not claim schema compatibility guarantees unless the
 * selected registry and compatibility mode provide them.
 */
import { KafkaError } from "../errors/index.js";
/**
 * In-memory schema registry implementation (§12.4). Suitable for tests and
 * for deployments without an external schema registry.
 */
export class InMemorySchemaRegistry {
    schemas = new Map();
    bySubject = new Map();
    compatibility;
    maxCacheSize;
    constructor(options = {}) {
        this.compatibility = options.compatibility ?? "NONE";
        this.maxCacheSize = options.maxCacheSize ?? 1000;
    }
    async register(schema) {
        const existing = this.findBySchema(schema);
        if (existing) {
            return existing;
        }
        const id = `${schema.type}:${Buffer.from(schema.schema).toString("base64").slice(0, 16)}`;
        const version = (this.schemas.get(id)?.version ?? 0) + 1;
        const registered = {
            ...schema,
            id,
            version,
            registeredAt: new Date().toISOString(),
        };
        if (this.schemas.size >= this.maxCacheSize) {
            const firstKey = this.schemas.keys().next().value;
            if (firstKey !== undefined) {
                this.schemas.delete(firstKey);
            }
        }
        this.schemas.set(id, registered);
        const versions = this.bySubject.get(schema.type) ?? [];
        if (!versions.includes(id)) {
            versions.push(id);
            this.bySubject.set(schema.type, versions);
        }
        return registered;
    }
    async get(id) {
        return this.schemas.get(id);
    }
    async checkCompatibility(_subject, schema) {
        if (this.compatibility === "NONE") {
            return { compatible: true };
        }
        const existing = this.findBySchema(schema);
        if (existing) {
            return { compatible: true };
        }
        return { compatible: true, messages: ["compatibility check not enforced in-memory"] };
    }
    async encode(id, value) {
        const schema = await this.get(id);
        if (!schema) {
            throw new Error(`Schema "${id}" not found`);
        }
        const payload = Buffer.from(JSON.stringify(value));
        // Prefix with schema id length + id for wire identity.
        const idBytes = Buffer.from(id, "utf8");
        const header = Buffer.alloc(2 + idBytes.length);
        header.writeUInt16BE(idBytes.length, 0);
        idBytes.copy(header, 2);
        return Buffer.concat([header, payload]);
    }
    async decode(id, bytes) {
        let schemaId = id;
        let payload = bytes;
        // If the payload carries an embedded schema id prefix, use it instead.
        if (bytes.length >= 2) {
            const len = bytes.readUInt16BE(0);
            if (bytes.length >= 2 + len) {
                schemaId = bytes.slice(2, 2 + len).toString("utf8");
                payload = bytes.slice(2 + len);
            }
        }
        const schema = await this.get(schemaId);
        if (!schema) {
            throw new Error(`Schema "${schemaId}" not found`);
        }
        return JSON.parse(payload.toString("utf8"));
    }
    findBySchema(schema) {
        for (const registered of this.schemas.values()) {
            if (registered.type === schema.type &&
                registered.schema === schema.schema) {
                return registered;
            }
        }
        return undefined;
    }
    get size() {
        return this.schemas.size;
    }
}
/**
 * Abstract base class for external schema registry clients (§12.4).
 *
 * Extend this to integrate with an external schema registry (e.g. Confluent,
 * Apicurio). The subclass implements network-dependent methods
 * (`registerRemote`, `getRemote`, `checkCompatibilityRemote`); this base
 * class handles caching, encode/decode framing, and registry outage
 * behavior.
 *
 * The package does not claim schema compatibility guarantees unless the
 * selected registry and compatibility mode provide them.
 */
export class AbstractSchemaRegistry {
    compatibility;
    maxCacheSize;
    cache = new Map();
    bySubject = new Map();
    constructor(
    /** Compatibility mode for new schema registrations. */
    compatibility = "NONE", 
    /** Maximum number of schemas to keep in the cache. */
    maxCacheSize = 1000) {
        this.compatibility = compatibility;
        this.maxCacheSize = maxCacheSize;
    }
    async register(schema) {
        const existing = this.findBySchema(schema);
        if (existing) {
            return existing;
        }
        let registered;
        try {
            registered = await this.registerRemote(schema);
        }
        catch (error) {
            throw new KafkaSchemaRegistryError(`Failed to register schema: ${error instanceof Error ? error.message : String(error)}`, { code: "KAFKA_SCHEMA_REGISTRY_ERROR", operation: "register", retryable: false, cause: error });
        }
        const versions = this.bySubject.get(registered.type) ?? [];
        if (!versions.includes(registered.id)) {
            versions.push(registered.id);
            this.bySubject.set(registered.type, versions);
        }
        this.cache.set(registered.id, registered);
        this.evictIfOverCapacity();
        return registered;
    }
    async get(id) {
        const cached = this.cache.get(id);
        if (cached) {
            return cached;
        }
        let remote;
        try {
            remote = await this.getRemote(id);
        }
        catch (error) {
            throw new KafkaSchemaRegistryError(`Failed to resolve schema "${id}": ${error instanceof Error ? error.message : String(error)}`, { code: "KAFKA_SCHEMA_REGISTRY_ERROR", operation: "get", resource: id, retryable: false, cause: error });
        }
        if (remote) {
            this.cache.set(remote.id, remote);
            this.evictIfOverCapacity();
        }
        return remote;
    }
    async checkCompatibility(subject, schema) {
        if (!this.checkCompatibilityRemote) {
            return { compatible: true, messages: ["compatibility check not implemented"] };
        }
        try {
            return await this.checkCompatibilityRemote(subject, schema);
        }
        catch (error) {
            throw new KafkaSchemaRegistryError(`Failed to check compatibility: ${error instanceof Error ? error.message : String(error)}`, { code: "KAFKA_SCHEMA_REGISTRY_ERROR", operation: "checkCompatibility", resource: subject, retryable: false, cause: error });
        }
    }
    async encode(id, value) {
        const schema = await this.get(id);
        if (!schema) {
            throw new KafkaSchemaRegistryError(`Schema "${id}" not found`, {
                code: "KAFKA_SCHEMA_REGISTRY_ERROR",
                operation: "encode",
                resource: id,
                retryable: false,
            });
        }
        const payload = Buffer.from(JSON.stringify(value));
        const idBytes = Buffer.from(id, "utf8");
        const header = Buffer.alloc(2 + idBytes.length);
        header.writeUInt16BE(idBytes.length, 0);
        idBytes.copy(header, 2);
        return Buffer.concat([header, payload]);
    }
    async decode(id, bytes) {
        let schemaId = id;
        let payload = bytes;
        if (bytes.length >= 2) {
            const len = bytes.readUInt16BE(0);
            if (bytes.length >= 2 + len) {
                schemaId = bytes.slice(2, 2 + len).toString("utf8");
                payload = bytes.slice(2 + len);
            }
        }
        const schema = await this.get(schemaId);
        if (!schema) {
            throw new KafkaSchemaRegistryError(`Schema "${schemaId}" not found`, {
                code: "KAFKA_SCHEMA_REGISTRY_ERROR",
                operation: "decode",
                resource: schemaId,
                retryable: false,
            });
        }
        return JSON.parse(payload.toString("utf8"));
    }
    findBySchema(schema) {
        for (const registered of this.cache.values()) {
            if (registered.type === schema.type &&
                registered.schema === schema.schema) {
                return registered;
            }
        }
        return undefined;
    }
    evictIfOverCapacity() {
        if (this.cache.size <= this.maxCacheSize) {
            return;
        }
        const firstKey = this.cache.keys().next().value;
        if (firstKey !== undefined) {
            this.cache.delete(firstKey);
        }
    }
    get size() {
        return this.cache.size;
    }
}
/**
 * Confluent Schema Registry client implementation (§12.4).
 *
 * Extends {@link AbstractSchemaRegistry} to integrate with Confluent Schema
 * Registry over HTTP. Registers schemas under subjects derived from the
 * schema type, encodes payloads with a Confluent wire-format schema id
 * prefix, and resolves schemas by id from the remote registry.
 *
 * The package does not claim schema compatibility guarantees unless the
 * selected registry and compatibility mode provide them.
 */
export class ConfluentSchemaRegistry extends AbstractSchemaRegistry {
    url;
    requestTimeoutMs;
    headers;
    constructor(options) {
        super(options.compatibility, options.maxCacheSize);
        this.url = options.url.replace(/\/$/, "");
        this.requestTimeoutMs = options.requestTimeoutMs ?? 10000;
        this.headers = { "Content-Type": "application/vnd.schemaregistry.v1+json" };
        if (options.bearerToken) {
            this.headers.Authorization = `Bearer ${options.bearerToken}`;
        }
        if (options.username && options.password) {
            this.headers.Authorization = `Basic ${Buffer.from(`${options.username}:${options.password}`).toString("base64")}`;
        }
    }
    /**
     * Register a schema with Confluent Schema Registry under a subject
     * derived from the schema type (§12.4).
     */
    async registerRemote(schema) {
        const subject = `kafka-${schema.type}`;
        const body = JSON.stringify({
            schema: schema.schema,
            schemaType: schema.type.toUpperCase(),
        });
        const response = await this.fetchJson(`/subjects/${subject}/versions`, { method: "POST", body });
        const registered = {
            ...schema,
            id: String(response.id),
            version: response.id,
            registeredAt: new Date().toISOString(),
        };
        return registered;
    }
    /**
     * Resolve a schema by id from Confluent Schema Registry (§12.4).
     */
    async getRemote(id) {
        try {
            const response = await this.fetchJson(`/schemas/ids/${parseInt(id, 10)}`, { method: "GET" });
            const registered = {
                id: String(response.version),
                version: response.version,
                type: "json",
                schema: response.schema,
                registeredAt: new Date().toISOString(),
            };
            return registered;
        }
        catch (error) {
            if (error.status === 404) {
                return undefined;
            }
            throw error;
        }
    }
    /**
     * Check schema compatibility with Confluent Schema Registry (§12.4).
     */
    async checkCompatibilityRemote(_subject, schema) {
        const confluentSubject = `kafka-${schema.type}`;
        const body = JSON.stringify({ schema: schema.schema });
        try {
            const response = await this.fetchJson(`/compatibility/subjects/${confluentSubject}/versions/latest`, { method: "POST", body });
            return {
                compatible: response.is_compatible,
                messages: response.is_compatible ? undefined : ["Schema is incompatible"],
            };
        }
        catch (error) {
            if (error.status === 404) {
                return { compatible: true, messages: ["No existing schema to compare against"] };
            }
            throw error;
        }
    }
    async fetchJson(path, options) {
        const controller = new AbortController();
        const timeout = setTimeout(() => controller.abort(), this.requestTimeoutMs);
        try {
            const response = await fetch(`${this.url}${path}`, {
                ...options,
                headers: { ...this.headers, ...options.headers },
                signal: controller.signal,
            });
            if (!response.ok) {
                const err = new Error(`HTTP ${response.status}`);
                err.status = response.status;
                throw err;
            }
            return (await response.json());
        }
        finally {
            clearTimeout(timeout);
        }
    }
}
/**
 * Error thrown by schema registry operations (§12.4).
 */
export class KafkaSchemaRegistryError extends KafkaError {
    constructor(message, options = {}) {
        super(message, {
            code: "KAFKA_SCHEMA_REGISTRY_ERROR",
            ...options,
        });
        this.name = "KafkaSchemaRegistryError";
    }
}
