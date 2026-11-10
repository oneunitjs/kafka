/**
 * OAuth token provider function type (§17). Returns an access token and
 * optional expiry in seconds. KafkaJS calls this on each connection and on
 * token refresh.
 */
export type OAuthTokenProvider = () => Promise<{
    access_token: string;
    expires_in?: number;
}>;
export declare function getSaslConfig(options?: Record<string, unknown>): object | undefined;
