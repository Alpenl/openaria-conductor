import eslint from "@eslint/js";
import globals from "globals";

export default [
  {
    ignores: ["node_modules/", "playwright-report/", "test-results/"],
  },
  eslint.configs.recommended,
  {
    files: ["**/*.js"],
    languageOptions: {
      ecmaVersion: 2022,
      globals: {
        ...globals.browser,
        ...globals.node,
      },
      sourceType: "module",
    },
  },
];
