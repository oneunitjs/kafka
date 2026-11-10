/**
 * Admin capability types (§3.3, §6).
 *
 * Topic and group administration operations wrapped around the KafkaJS
 * `Admin` client. The package keeps KafkaJS-specific types out of the
 * public surface by defining its own stable contracts.
 */

export interface TopicConfig {
    readonly name: string;
    readonly numPartitions?: number;
    readonly replicationFactor?: number;
    readonly configEntries?: Record<string, string>;
}

export interface TopicMetadata {
    readonly name: string;
    readonly isInternal: boolean;
    readonly partitions: Array<{
        readonly partition: number;
        readonly leader: number | null;
        readonly replicas: readonly number[];
        readonly isr: readonly number[];
        readonly offlineReplicas: readonly number[];
    }>;
}

export interface GroupInfo {
    readonly groupId: string;
    readonly protocolType: string | null;
    readonly protocol: string | null;
    readonly state: string | null;
    readonly members: Array<{
        readonly memberId: string;
        readonly clientId: string;
        readonly host: string;
        readonly memberAssignment: Buffer | null;
    }>;
}

export interface CreateTopicsOptions {
    readonly validateOnly?: boolean;
    readonly timeout?: number;
    readonly waitForLeaders?: boolean;
}

export interface DeleteTopicsOptions {
    readonly timeout?: number;
}

export interface TopicOperationResult {
    readonly topic: string;
    readonly errorCode: number;
    readonly errorMessage: string | null;
}

export interface GroupOperationResult {
    readonly groupId: string;
    readonly errorCode: number;
    readonly errorMessage: string | null;
}

/**
 * Package-level admin capability. Wraps KafkaJS `Admin` and executes each
 * operation through the configured admin middleware pipeline (§11.1).
 */
export interface KafkaAdmin {
    readonly topics: TopicAdmin;
    readonly groups: GroupAdmin;
    /** Disconnect the underlying KafkaJS admin client. */
    disconnect(): Promise<void>;
    /** Alias for {@link disconnect}. */
    close(): Promise<void>;
    readonly isClosed: boolean;
    /** List all topics. */
    listTopics(): Promise<string[]>;
    /** Create topics. */
    createTopics(
        topics: readonly TopicConfig[],
        options?: CreateTopicsOptions,
    ): Promise<TopicOperationResult[]>;
    /** Delete topics. */
    deleteTopics(
        topics: readonly string[],
        options?: DeleteTopicsOptions,
    ): Promise<TopicOperationResult[]>;
    /** Fetch topic metadata. */
    fetchTopicMetadata(topics?: readonly string[]): Promise<TopicMetadata[]>;
    /** List consumer groups. */
    listGroups(): Promise<GroupInfo[]>;
    /** Describe consumer groups. */
    describeGroups(groupIds: readonly string[]): Promise<GroupInfo[]>;
    /** Delete consumer groups. */
    deleteGroups(groupIds: readonly string[]): Promise<GroupOperationResult[]>;
}

/**
 * Topic administration operations.
 */
export interface TopicAdmin {
    createTopics(
        topics: readonly TopicConfig[],
        options?: CreateTopicsOptions,
    ): Promise<TopicOperationResult[]>;
    deleteTopics(
        topics: readonly string[],
        options?: DeleteTopicsOptions,
    ): Promise<TopicOperationResult[]>;
    listTopics(): Promise<string[]>;
    fetchTopicMetadata(topics?: readonly string[]): Promise<TopicMetadata[]>;
}

/**
 * Consumer group administration operations.
 */
export interface GroupAdmin {
    listGroups(): Promise<GroupInfo[]>;
    describeGroups(groupIds: readonly string[]): Promise<GroupInfo[]>;
    deleteGroups(groupIds: readonly string[]): Promise<GroupOperationResult[]>;
}
