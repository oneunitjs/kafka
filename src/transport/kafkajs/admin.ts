import { createLogger, type Logger } from "./logger.js";
import { connectKafkaResource } from "./factory.js";
import { Kafka, type Admin } from "kafkajs";

export type { Admin } from "kafkajs";

export interface CreateAdminOptions {
    logger?: Logger;
    [key: string]: unknown;
}

export async function createAdmin(
    kafka: Kafka,
    options: CreateAdminOptions = {},
): Promise<Admin> {
    const logger = createLogger(options.logger);
    const { logger: _logger, ...adminOptions } = options;

    return connectKafkaResource(
        () => kafka.admin(adminOptions),
        (admin) => admin.connect(),
        (admin) => admin.disconnect(),
        logger,
        "admin",
    );
}
