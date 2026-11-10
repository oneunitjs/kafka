import type { Admin } from "kafkajs";
import type { KafkaAdmin } from "./types.js";
import { createTopicAdmin } from "./topics.js";
import { createGroupAdmin } from "./groups.js";

export function createKafkaAdmin(admin: Admin): KafkaAdmin {
    const topics = createTopicAdmin(admin);
    const groups = createGroupAdmin(admin);
    let closed = false;

    const disconnect = async () => {
        closed = true;
        try {
            await admin.disconnect();
        } catch {
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
        createTopics: (t: readonly unknown[], o?: unknown) => topics.createTopics(t as any, o as any),
        deleteTopics: (t: readonly unknown[], o?: unknown) => topics.deleteTopics(t as any, o as any),
        fetchTopicMetadata: (t?: readonly string[]) => topics.fetchTopicMetadata(t as any),
        listGroups: () => groups.listGroups(),
        describeGroups: (g: readonly string[]) => groups.describeGroups(g as readonly string[]),
        deleteGroups: (g: readonly string[]) => groups.deleteGroups(g as readonly string[]),
    };
}
