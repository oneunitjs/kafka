import { createTopicAdmin } from "./topics.js";
import { createGroupAdmin } from "./groups.js";
export function createKafkaAdmin(admin) {
    const topics = createTopicAdmin(admin);
    const groups = createGroupAdmin(admin);
    let closed = false;
    const disconnect = async () => {
        closed = true;
        try {
            await admin.disconnect();
        }
        catch {
            // Disconnect failures are non-fatal during close.
        }
    };
    return {
        topics,
        groups,
        disconnect,
        close: disconnect,
        get isClosed() {
            return closed;
        },
        listTopics: () => topics.listTopics(),
        createTopics: (t, o) => topics.createTopics(t, o),
        deleteTopics: (t, o) => topics.deleteTopics(t, o),
        fetchTopicMetadata: (t) => topics.fetchTopicMetadata(t),
        listGroups: () => groups.listGroups(),
        describeGroups: (g) => groups.describeGroups(g),
        deleteGroups: (g) => groups.deleteGroups(g),
    };
}
