import { env, useSheets } from "../config";
import { batchGet } from "./SheetsClient";

function sheetName(base: string, city: string) {
  if (env.GOOGLE_SHEETS_MODE === "TABS_PER_CITY") return `${base}_${city}`;
  return base;
}

function hasColumns(headers: string[], required: string[]) {
  const set = new Set(headers);
  return required.filter((c) => !set.has(c));
}

async function getHeadersWithFallback(sheet: string): Promise<string[]> {
  const city = (env.CITY_CODES || "").split(",")[0]?.trim() || "";
  const candidates = [sheet, sheet.replace(/^[a-z]/, (c) => c.toUpperCase())];
  if (city) {
    candidates.unshift(`${sheet}_${city}`, `${sheet.replace(/^[a-z]/, (c) => c.toUpperCase())}_${city}`);
  }
  for (const s of candidates) {
    try {
      const vr = await batchGet([`${s}!A:Z`]);
      return (vr[0]?.values?.[0] || []).map(String);
    } catch {}
  }
  return [];
}

export async function validateSheetsSchemaOrThrow(defaultCity: string) {
  if (!useSheets) return;
  const productsSheet = sheetName("products", defaultCity);
  const couriersSheet = sheetName("couriers", defaultCity);
  const ordersSheet = sheetName("orders", defaultCity);
  const metricsSheet = sheetName("metrics", defaultCity);
  const headers = [
    await getHeadersWithFallback(productsSheet),
    await getHeadersWithFallback(couriersSheet),
    await getHeadersWithFallback(ordersSheet),
    await getHeadersWithFallback(metricsSheet)
  ];
  const [prodH, courH, ordH, metH] = headers;
  if (!prodH.length || !courH.length || !ordH.length || !metH.length) {
    const missTabs: string[] = [];
    if (!prodH.length) missTabs.push(productsSheet);
    if (!courH.length) missTabs.push(couriersSheet);
    if (!ordH.length) missTabs.push(ordersSheet);
    if (!metH.length) missTabs.push(metricsSheet);
    throw new Error(`Sheets tabs missing or empty: ${missTabs.join(", ")}`);
  }
}
