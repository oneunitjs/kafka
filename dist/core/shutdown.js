import {} from "../transport/kafkajs/logger.js";
export async function shutdownClient(logger, client, name) {
    if (!client) {
        return;
    }
    try {
        await client.disconnect();
        logger.info(`${name} disconnected`);
    }
    catch (error) {
        logger.error(`Failed to disconnect ${name}`, error);
        throw error;
    }
}
export function registerShutdown(logger, clients = {}) {
    const { producer, consumer, admin, clients: extraClients = [], exit = true } = clients;
    let shuttingDown = false;
    async function handleShutdown(signal) {
        if (shuttingDown) {
            return;
        }
        shuttingDown = true;
        logger.info(`${signal} received`);
        const baseTargets = [
            [producer, "Kafka producer"],
            [consumer, "Kafka consumer"],
            [admin, "Kafka admin"],
        ];
        const extraTargets = extraClients.map((client, index) => [client, `Kafka client ${index + 1}`]);
        const targets = [...baseTargets, ...extraTargets];
        await Promise.allSettled(targets.map(([target, name]) => shutdownClient(logger, resolveTarget(target), name)));
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
function resolveTarget(target) {
    const resolved = typeof target === "function" ? target() : target;
    return isDisconnectable(resolved) ? resolved : undefined;
}
function isDisconnectable(value) {
    return Boolean(value && typeof value === "object" && typeof value.disconnect === "function");
}
