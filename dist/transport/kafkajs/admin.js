import { createLogger } from "./logger.js";
import { connectKafkaResource } from "./factory.js";
import { Kafka } from "kafkajs";
export async function createAdmin(kafka, options = {}) {
    const logger = createLogger(options.logger);
    const { logger: _logger, ...adminOptions } = options;
    return connectKafkaResource(() => kafka.admin(adminOptions), (admin) => admin.connect(), (admin) => admin.disconnect(), logger, "admin");
}
