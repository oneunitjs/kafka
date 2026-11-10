import { KafkaApplication, createApplication, createApplicationFromLegacyOptions, type KafkaApplicationOptions } from "./application.js";
export declare namespace Kafka {
    function create<Topics = Record<string, unknown>>(options?: KafkaApplicationOptions): Promise<KafkaApplication<Topics>>;
}
export { KafkaApplication, createApplication, createApplicationFromLegacyOptions };
export type { KafkaApplicationOptions };
