import { type Logger } from "../transport/kafkajs/logger.js";
type Disconnectable = {
    disconnect: () => Promise<void>;
};
export declare function shutdownClient(logger: Logger, client: Disconnectable | undefined, name: string): Promise<void>;
export declare function registerShutdown(logger: Logger, clients?: Record<string, unknown>): (signal: string) => Promise<void>;
export {};
