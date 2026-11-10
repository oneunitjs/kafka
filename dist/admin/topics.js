export function createTopicAdmin(admin) {
    return {
        async createTopics(topics, options = {}) {
            const sent = await admin.createTopics({
                topics: topics.map((t) => ({
                    topic: t.name,
                    numPartitions: t.numPartitions,
                    replicationFactor: t.replicationFactor,
                    configEntries: t.configEntries,
                })),
                validateOnly: options.validateOnly,
                timeout: options.timeout,
                waitForLeaders: options.waitForLeaders,
            });
            return topics.map((t) => ({
                topic: t.name,
                errorCode: sent ? 0 : 1,
                errorMessage: null,
            }));
        },
        async deleteTopics(topics, options = {}) {
            await admin.deleteTopics({
                topics: [...topics],
                timeout: options.timeout,
            });
            return topics.map((t) => ({
                topic: t,
                errorCode: 0,
                errorMessage: null,
            }));
        },
        async listTopics() {
            return admin.listTopics();
        },
        async fetchTopicMetadata(topics) {
            const result = (await admin.fetchTopicMetadata(topics ? { topics: [...topics] } : { topics: [] }));
            return result.topics.map((t) => ({
                name: t.name,
                isInternal: Boolean(t.isInternal),
                partitions: t.partitions.map((p) => ({
                    partition: p.partition,
                    leader: p.leader == null ? null : Number(p.leader),
                    replicas: Array.isArray(p.replicas)
                        ? p.replicas.map(Number)
                        : [],
                    isr: Array.isArray(p.isr) ? p.isr.map(Number) : [],
                    offlineReplicas: Array.isArray(p.offlineReplicas)
                        ? p.offlineReplicas.map(Number)
                        : [],
                })),
            }));
        },
    };
}
