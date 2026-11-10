export interface ConfigAdapter {
    string(name: string, fallback?: string): string | undefined;
    number(name: string, fallback?: number): number | undefined;
    boolean(name: string, fallback?: boolean): boolean;
    get(name: string): unknown;
}
export declare function createConfigAdapter(source?: Record<string, unknown> | NodeJS.ProcessEnv): ConfigAdapter;
export declare function readConfigString(options: Record<string, unknown> | undefined, name: string, fallback?: string): string | undefined;
export declare function readConfigNumber(options: Record<string, unknown> | undefined, name: string, fallback?: number): number | undefined;
export declare function readConfigBoolean(options: Record<string, unknown> | undefined, name: string, fallback?: boolean): boolean;
