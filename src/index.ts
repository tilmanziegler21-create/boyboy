import 'dotenv/config';
import { logger } from "./infra/logger";
import { startHttpServer } from "./server/http";
import { startBot } from "./bot/Bot";
import { initDb } from "./infra/db/sqlite";
import { registerCron } from "./infra/cron/scheduler";
import { getDefaultCity } from "./infra/backend";
import { validateSheetsSchemaOrThrow } from "./infra/sheets/SchemaValidator";
import { useSheets } from "./infra/config";
import { loadState } from "./infra/storage/InMemoryStorage";

async function main() {
  logger.info("Runtime", { node: process.version, openssl: process.versions.openssl, node_options: process.env.NODE_OPTIONS || "" });
  loadState();
  logger.info("[BOT] State loaded from persistent storage");
  try { const b = (await import("./infra/backend")).getBackend() as any; if (typeof b.preloadData === "function") { await b.preloadData(); logger.info("[BOT] Sheets data preloaded"); } } catch {}
  try { const b2 = (await import("./infra/backend")).getBackend() as any; if (typeof b2.init === "function") { await b2.init(); logger.info("[BOT] Backend initialized"); } } catch {}
  await initDb();
  if (useSheets) {
    const city = getDefaultCity();
    try { await validateSheetsSchemaOrThrow(city); }
    catch (e) { logger.warn("Sheets schema validation warning", { error: String(e) }); }
  }
  await startHttpServer();
  await startBot();
  await registerCron();
  logger.info("App started");
}

main().catch((e) => {
  logger.error("Fatal", { error: String(e) });
  process.exit(1);
});
