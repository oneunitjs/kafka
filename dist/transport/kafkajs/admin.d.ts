import { type Logger } from "./logger.js";
import { Kafka, type Admin } from "kafkajs";
export type { Admin } from "kafkajs";
export interface CreateAdminOptions {
    logger?: Logger;
    [key: string]: unknown;
}
export declare function createAdmin(kafka: Kafka, options?: CreateAdminOptions): Promise<Admin>;
