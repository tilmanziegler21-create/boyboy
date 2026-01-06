import express from "express";
import { logger } from "../infra/logger";
import { getQtyReservedSnapshot } from "../domain/inventory/InventoryService";
import { env, useSheets } from "../infra/config";
import { testSheetsAuth } from "../infra/sheets/SheetsClient";

export async function startHttpServer() {
  const app = express();
  app.get("/health", (_req, res) => {
    const sheetsStatus = useSheets ? (testSheetsAuth() ? "OK" : "FAIL") : "DISABLED";
    res.json({ ok: true, backend: env.DATA_BACKEND, sheets_auth: sheetsStatus });
  });
  app.get("/metrics", (req, res) => {
    const auth = req.headers.authorization || "";
    const token = String(auth).startsWith("Bearer ") ? String(auth).slice(7) : "";
    if ((env.METRICS_TOKEN || "").length > 0 && token !== env.METRICS_TOKEN) {
      return res.status(401).json({ error: "Unauthorized" });
    }
    res.json({ qty_reserved: getQtyReservedSnapshot() });
  });
  const port = process.env.PORT ? Number(process.env.PORT) : 3000;
  app.listen(port, () => logger.info("HTTP server started", { port }));
}
