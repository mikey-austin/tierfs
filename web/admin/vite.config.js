import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

export default defineConfig({
  plugins: [react()],
  build: {
    outDir: "dist",
    // Embed assets inline for single-file serving from Go
    assetsInlineLimit: 1024 * 1024,
  },
  server: {
    port: 3000,
    // Proxy API calls to the tierfs metrics/API server during development
    proxy: {
      "/api":     "http://localhost:9100",
      "/metrics": "http://localhost:9100",
      "/healthz": "http://localhost:9100",
    },
  },
});
