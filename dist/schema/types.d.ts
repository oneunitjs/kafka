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
import { KafkaError, type KafkaErrorOptions } from "../errors/index.js";
export interface Schema {
    readonly id: string;
    readonly version: number;
    readonly type: "avro" | "json" | "protobuf" | "string" | "bytes";
    readonly schema: string;
}
export interface RegisteredSchema extends Schema {
    readonly registeredAt: string;
}
export interface SchemaRegistry {
    /**
     * Register a schema, returning its id and version. May throw if the
     * schema is incompatible under the configured compatibility mode.
     */
    register(schema: Omit<Schema, "id" | "version">): Promise<RegisteredSchema>;
    /**
     * Resolve a schema by id.
     */
    get(id: string): Promise<RegisteredSchema | undefined>;
    /**
     * Check compatibility of a candidate schema against the registered
     * schema for a subject, without registering it.
     */
    checkCompatibility?(subject: string, schema: Omit<Schema, "id" | "version">): Promise<{
        compatible: boolean;
        messages?: string[];
    }>;
    /**
     * Encode a value using the schema identified by id, prefixing the
     * payload with schema identity where the registry wire format requires
     * it.
     */
    encode(id: string, value: unknown): Promise<Buffer>;
    /**
     * Decode bytes produced by `encode`, using schema identity embedded in
     * the payload or resolved from the provided id.
     */
    decode(id: string, bytes: Buffer): Promise<unknown>;
}
/**
 * Options for the in-memory schema registry.
 */
export interface InMemorySchemaRegistryOptions {
    /** Compatibility mode for new schema registrations. */
    compatibility?: "BACKWARD" | "FORWARD" | "FULL" | "NONE";
    /** Maximum number of schemas to keep in the cache. */
    maxCacheSize?: number;
}
/**
 * In-memory schema registry implementation (§12.4). Suitable for tests and
 * for deployments without an external schema registry.
 */
export declare class InMemorySchemaRegistry implements SchemaRegistry {
    private readonly schemas;
    private readonly bySubject;
    private readonly compatibility;
    private readonly maxCacheSize;
    constructor(options?: InMemorySchemaRegistryOptions);
    register(schema: Omit<Schema, "id" | "version">): Promise<RegisteredSchema>;
    get(id: string): Promise<RegisteredSchema | undefined>;
    checkCompatibility(_subject: string, schema: Omit<Schema, "id" | "version">): Promise<{
        compatible: boolean;
        messages?: string[];
    }>;
    encode(id: string, value: unknown): Promise<Buffer>;
    decode(id: string, bytes: Buffer): Promise<unknown>;
    private findBySchema;
    get size(): number;
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
export declare abstract class AbstractSchemaRegistry implements SchemaRegistry {
    /** Compatibility mode for new schema registrations. */
    protected readonly compatibility: "BACKWARD" | "FORWARD" | "FULL" | "NONE";
    /** Maximum number of schemas to keep in the cache. */
    protected readonly maxCacheSize: number;
    private readonly cache;
    private readonly bySubject;
    protected constructor(
    /** Compatibility mode for new schema registrations. */
    compatibility?: "BACKWARD" | "FORWARD" | "FULL" | "NONE", 
    /** Maximum number of schemas to keep in the cache. */
    maxCacheSize?: number);
    /**
     * Register a schema with the remote registry. Implemented by subclass.
     */
    protected abstract registerRemote(schema: Omit<Schema, "id" | "version">): Promise<RegisteredSchema>;
    /**
     * Resolve a schema by id from the remote registry. Implemented by
     * subclass.
     */
    protected abstract getRemote(id: string): Promise<RegisteredSchema | undefined>;
    /**
     * Check compatibility with the remote registry. Implemented by
     * subclass; optional.
     */
    protected checkCompatibilityRemote?(subject: string, schema: Omit<Schema, "id" | "version">): Promise<{
        compatible: boolean;
        messages?: string[];
    }>;
    register(schema: Omit<Schema, "id" | "version">): Promise<RegisteredSchema>;
    get(id: string): Promise<RegisteredSchema | undefined>;
    checkCompatibility(subject: string, schema: Omit<Schema, "id" | "version">): Promise<{
        compatible: boolean;
        messages?: string[];
    }>;
    encode(id: string, value: unknown): Promise<Buffer>;
    decode(id: string, bytes: Buffer): Promise<unknown>;
    private findBySchema;
    private evictIfOverCapacity;
    get size(): number;
}
/**
 * Options for the Confluent Schema Registry client.
 */
export interface ConfluentSchemaRegistryOptions {
    /** Base URL of the Confluent Schema Registry (e.g. "http://localhost:8081"). */
    readonly url: string;
    /** Compatibility mode for new schema registrations. */
    readonly compatibility?: "BACKWARD" | "FORWARD" | "FULL" | "NONE";
    /** Maximum number of schemas to keep in the cache. */
    readonly maxCacheSize?: number;
    /** Request timeout in milliseconds. */
    readonly requestTimeoutMs?: number;
    /** Optional bearer token for authentication. */
    readonly bearerToken?: string;
    /** Optional basic auth username. */
    readonly username?: string;
    /** Optional basic auth password. */
    readonly password?: string;
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
export declare class ConfluentSchemaRegistry extends AbstractSchemaRegistry {
    private readonly url;
    private readonly requestTimeoutMs;
    private readonly headers;
    constructor(options: ConfluentSchemaRegistryOptions);
    /**
     * Register a schema with Confluent Schema Registry under a subject
     * derived from the schema type (§12.4).
     */
    protected registerRemote(schema: Omit<Schema, "id" | "version">): Promise<RegisteredSchema>;
    /**
     * Resolve a schema by id from Confluent Schema Registry (§12.4).
     */
    protected getRemote(id: string): Promise<RegisteredSchema | undefined>;
    /**
     * Check schema compatibility with Confluent Schema Registry (§12.4).
     */
    protected checkCompatibilityRemote(_subject: string, schema: Omit<Schema, "id" | "version">): Promise<{
        compatible: boolean;
        messages?: string[];
    }>;
    private fetchJson;
}
/**
 * Error thrown by schema registry operations (§12.4).
 */
export declare class KafkaSchemaRegistryError extends KafkaError {
    constructor(message: string, options?: KafkaErrorOptions);
}
