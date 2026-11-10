import { type Logger } from "../transport/kafkajs/logger.js";

type Disconnectable = { disconnect: () => Promise<void> };
type ShutdownTarget = Disconnectable | (() => Disconnectable | null | undefined);

export async function shutdownClient(
    logger: Logger,
    client: Disconnectable | undefined,
    name: string,
): Promise<void> {
    if (!client) {
        return;
    }

    try {
        await client.disconnect();
        logger.info(`${name} disconnected`);
    } catch (error) {
        logger.error(`Failed to disconnect ${name}`, error);
        throw error;
    }
}

export function registerShutdown(
    logger: Logger,
    clients: Record<string, unknown> = {},
): (signal: string) => Promise<void> {
    const { producer, consumer, admin, clients: extraClients = [], exit = true } = clients as {
        producer?: ShutdownTarget;
        consumer?: ShutdownTarget;
        admin?: ShutdownTarget;
        clients?: ShutdownTarget[];
        exit?: boolean;
    };

    let shuttingDown = false;

    async function handleShutdown(signal: string): Promise<void> {
        if (shuttingDown) {
            return;
        }
        shuttingDown = true;

        logger.info(`${signal} received`);
        const baseTargets: Array<[ShutdownTarget | undefined, string]> = [
            [producer, "Kafka producer"],
            [consumer, "Kafka consumer"],
            [admin, "Kafka admin"],
        ];
        const extraTargets: Array<[ShutdownTarget | undefined, string]> = extraClients.map(
            (client: ShutdownTarget, index: number) => [client, `Kafka client ${index + 1}`] as [ShutdownTarget | undefined, string],
        );
        const targets = [...baseTargets, ...extraTargets];
        await Promise.allSettled(
            targets.map(([target, name]) => shutdownClient(logger, resolveTarget(target) as Disconnectable, name)),
        );
        if (exit) {
            process.exit(0);
        }
    }

    process.once("SIGTERM", () => {
        handleShutdown("SIGTERM");
    });
    process.once("SIGINT", () => {
        handleShutdown("SIGINT");
    });

    return handleShutdown;
}

function resolveTarget(target: ShutdownTarget | undefined): Disconnectable | undefined {
    const resolved = typeof target === "function" ? target() : target;
    return isDisconnectable(resolved) ? resolved : undefined;
}

function isDisconnectable(value: unknown): value is Disconnectable {
    return Boolean(value && typeof value === "object" && typeof (value as Record<string, unknown>).disconnect === "function");
}
