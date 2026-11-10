import { type Logger } from "./logger.js";
import { Kafka, type Consumer, type EachMessagePayload } from "kafkajs";
export type { Consumer, EachMessagePayload } from "kafkajs";
export interface CreateConsumerOptions {
    logger?: Logger;
    topic?: string;
    fromBeginning?: boolean;
    [key: string]: unknown;
}
export declare function createConsumer(kafka: Kafka, groupId: string, options?: CreateConsumerOptions): Promise<Consumer>;
export declare function subscribeToTopic(consumer: Consumer, topic: string, options?: CreateConsumerOptions): Promise<void>;
export declare function consumeMessages(consumer: Consumer, handler?: (payload: EachMessagePayload) => Promise<void>, options?: {
    logger?: Logger;
}): Promise<void>;
