import {
    KafkaApplication,
    createApplication,
    createApplicationFromLegacyOptions,
    type KafkaApplicationOptions,
} from "./application.js";

export namespace Kafka {
export async function create<Topics = Record<string, unknown>>(
    options: KafkaApplicationOptions = {},
): Promise<KafkaApplication<Topics>> {
        return new KafkaApplication<Topics>(options);
    }
}

export { KafkaApplication, createApplication, createApplicationFromLegacyOptions };
export type { KafkaApplicationOptions };
