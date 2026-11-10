export default [
    {
        files: ["src/**/*.js", "test/**/*.js", "examples/**/*.js"],
        languageOptions: {
            ecmaVersion: 2022,
            sourceType: "module",
            globals: {
                AbortController: "readonly",
                Buffer: "readonly",
                URL: "readonly",
                console: "readonly",
                process: "readonly",
                setTimeout: "readonly",
                clearTimeout: "readonly",
            },
        },
        rules: {
            "no-undef": "error",
            "no-unused-vars": ["error", { argsIgnorePattern: "^_", varsIgnorePattern: "^_", caughtErrors: "none" }],
            "no-var": "error",
            eqeqeq: ["error", "always"],
        },
    },
];
