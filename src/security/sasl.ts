import { envString } from "../core/context.js";
import { KafkaConfigError } from "../errors/index.js";
import { readConfigString } from "../adapters/config.js";

// Mirrors SASLMechanism in kafkajs. Keep this in sync with the KafkaJS version.
const SASL_MECHANISMS = new Set([
    "plain",
    "scram-sha-256",
    "scram-sha-512",
    "aws",
    "oauthbearer",
]);

type SaslSource = Record<string, unknown> & {
    mechanism?: string;
    username?: string;
    password?: string;
    authorizationIdentity?: string;
    accessKeyId?: string;
    secretAccessKey?: string;
    sessionToken?: string;
    oauthClientId?: string;
    oauthClientSecret?: string;
    oauthTokenProvider?: () => Promise<{ access_token: string; expires_in?: number }>;
    oauthBearerProvider?: () => Promise<{ value: string } | { token: string }>;
    oauthAccessToken?: string;
    oauthExtensions?: Record<string, unknown>;
};

/**
 * OAuth token provider function type (§17). Returns an access token and
 * optional expiry in seconds. KafkaJS calls this on each connection and on
 * token refresh.
 */
export type OAuthTokenProvider = () => Promise<{ access_token: string; expires_in?: number }>;

function readOAuthConfig(options: Record<string, unknown>): SaslSource {
    return {
        ...options,
        oauthClientId:
            (options.oauthClientId as string | undefined) ??
            readConfigString(options, "KAFKA_SASL_OAUTH_CLIENT_ID") ??
            envString("KAFKA_SASL_OAUTH_CLIENT_ID"),
        oauthClientSecret:
            (options.oauthClientSecret as string | undefined) ??
            readConfigString(options, "KAFKA_SASL_OAUTH_CLIENT_SECRET") ??
            envString("KAFKA_SASL_OAUTH_CLIENT_SECRET"),
        oauthAccessToken:
            (options.oauthAccessToken as string | undefined) ??
            readConfigString(options, "KAFKA_SASL_OAUTH_ACCESS_TOKEN") ??
            envString("KAFKA_SASL_OAUTH_ACCESS_TOKEN"),
        oauthExtensions:
            (options.oauthExtensions as Record<string, unknown> | undefined) ??
            readConfigString(options, "KAFKA_SASL_OAUTH_EXTENSIONS")
                ?.split(",")
                .reduce<Record<string, unknown>>((acc: Record<string, unknown>, pair: string) => {
                    const [key, value] = pair.split("=", 2);
                    if (key && value !== undefined) {
                        acc[key.trim()] = value.trim();
                    }
                    return acc;
                }, {}),
    };
}

export function getSaslConfig(options: Record<string, unknown> = {}): object | undefined {
    if (options.sasl === false) {
        return undefined;
    }

    const source = (options.sasl && typeof options.sasl === "object" ? options.sasl : options) as SaslSource;

    const mechanism = String(
        source.mechanism ?? readConfigString(options, "KAFKA_SASL_MECHANISM", "") ?? envString("KAFKA_SASL_MECHANISM", ""),
    )
        .trim()
        .toLowerCase();

    if (!mechanism) {
        return undefined;
    }

    if (!SASL_MECHANISMS.has(mechanism)) {
        throw new KafkaConfigError(
            `Unknown SASL mechanism "${mechanism}". Supported: ${[...SASL_MECHANISMS].join(", ")}`,
        );
    }

    if (mechanism === "oauthbearer") {
        return getOAuthSaslConfig(options);
    }

    if (mechanism === "aws") {
        const authorizationIdentity =
            source.authorizationIdentity ??
            (options as SaslSource).authorizationIdentity ??
            readConfigString(options, "KAFKA_SASL_AWS_AUTHORIZATION_IDENTITY") ??
            envString("KAFKA_SASL_AWS_AUTHORIZATION_IDENTITY");
        const accessKeyId =
            source.accessKeyId ??
            (options as SaslSource).accessKeyId ??
            readConfigString(options, "KAFKA_SASL_AWS_ACCESS_KEY_ID") ??
            envString("KAFKA_SASL_AWS_ACCESS_KEY_ID");
        const secretAccessKey =
            source.secretAccessKey ??
            (options as SaslSource).secretAccessKey ??
            readConfigString(options, "KAFKA_SASL_AWS_SECRET_ACCESS_KEY") ??
            envString("KAFKA_SASL_AWS_SECRET_ACCESS_KEY");
        const sessionToken =
            source.sessionToken ??
            (options as SaslSource).sessionToken ??
            readConfigString(options, "KAFKA_SASL_AWS_SESSION_TOKEN") ??
            envString("KAFKA_SASL_AWS_SESSION_TOKEN");

        if (!accessKeyId || !secretAccessKey) {
            return undefined;
        }

        return {
            mechanism,
            authorizationIdentity,
            accessKeyId,
            secretAccessKey,
            ...(sessionToken ? { sessionToken } : {}),
        };
    }

    const username =
        source.username ??
        (options as SaslSource).username ??
        readConfigString(options, "KAFKA_SASL_USERNAME") ??
        envString("KAFKA_SASL_USERNAME");
    const password =
        source.password ??
        (options as SaslSource).password ??
        readConfigString(options, "KAFKA_SASL_PASSWORD") ??
        envString("KAFKA_SASL_PASSWORD");

    if (!username || !password) {
        return undefined;
    }

    return {
        mechanism,
        username,
        password,
    };
}

function getOAuthSaslConfig(options: Record<string, unknown>): object | undefined {
    const source = readOAuthConfig(options);

    const { oauthClientId, oauthClientSecret, oauthTokenProvider, oauthAccessToken } = source;

    if (!oauthClientId || !oauthClientSecret) {
        return undefined;
    }

    const config: Record<string, unknown> = {
        mechanism: "oauthbearer",
        oauthClientId,
        oauthClientSecret,
    };

    const provider =
        oauthTokenProvider ??
        (typeof source.oauthBearerProvider === "function"
            ? source.oauthBearerProvider
            : undefined);

    // KafkaJS requires a provider returning its Oauthbearer response shape.
    if (typeof provider === "function") {
        config.oauthBearerProvider = async () => {
            const issued = await provider();
            if (issued && typeof issued === "object" && "access_token" in issued) {
                return { value: (issued as { access_token: string }).access_token };
            }
            return issued;
        };
    } else if (oauthAccessToken) {
        config.oauthBearerProvider = async () => ({ value: oauthAccessToken as string });
    } else {
        return undefined;
    }

    if (source.oauthExtensions) {
        config.oauthExtensions = source.oauthExtensions;
    }

    return config;
}
