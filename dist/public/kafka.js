import { KafkaApplication, createApplication, createApplicationFromLegacyOptions, } from "./application.js";
export var Kafka;
(function (Kafka) {
    async function create(options = {}) {
        return new KafkaApplication(options);
    }
    Kafka.create = create;
})(Kafka || (Kafka = {}));
export { KafkaApplication, createApplication, createApplicationFromLegacyOptions };
