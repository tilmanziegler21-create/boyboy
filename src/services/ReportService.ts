import { batchGet } from "../infra/sheets/SheetsClient";
import { env } from "../infra/config";
import { getDefaultCity } from "../infra/backend";

function sheetName(base: string, city: string) {
  if (env.GOOGLE_SHEETS_MODE === "TABS_PER_CITY") return `${base}_${city}`;
  return base;
}

export class ReportService {
  async getTodayReport(city?: string): Promise<any> {
    const today = new Date().toISOString().split("T")[0];
    return await this.getReportForDate(today, city);
  }

  async getReportForDate(date: string, city?: string): Promise<any> {
    const c = (city || getDefaultCity()).trim();
    const s = sheetName("orders", c);
    try {
      const vr = await batchGet([`${s}!A:Z`]);
      const values = vr[0]?.values || [];
      const headers = values[0] || [];
      const rows = values.slice(1);
      const idx = (name: string) => headers.indexOf(name);
      const createdIdx = idx("created_at");
      const statusIdx = idx("status");
      const totalIdx = idx("total_amount") >= 0 ? idx("total_amount") : idx("total");
      const itemsIdx = idx("items_json");
      let totalOrders = 0;
      let totalRevenue = 0;
      const itemsSold: Record<string, number> = {};
      for (const r of rows) {
        const created = String(createdIdx >= 0 ? r[createdIdx] || "" : "").split("T")[0];
        const status = String(statusIdx >= 0 ? r[statusIdx] || "" : "").toLowerCase();
        if (created === date && status === "delivered") {
          totalOrders++;
          const amt = Number(totalIdx >= 0 ? r[totalIdx] || 0 : 0);
          totalRevenue += isNaN(amt) ? 0 : amt;
          const itemsJson = String(itemsIdx >= 0 ? r[itemsIdx] || "[]" : "[]");
          try {
            const items = JSON.parse(itemsJson) as Array<{ product_id: number; qty: number; price: number }>;
            for (const it of items) {
              const name = `#${it.product_id}`;
              itemsSold[name] = (itemsSold[name] || 0) + Number(it.qty || 0);
            }
          } catch {}
        }
      }
      let topItem = { name: "-", count: 0 };
      for (const [name, count] of Object.entries(itemsSold)) {
        if (count > topItem.count) topItem = { name, count };
      }
      return {
        date,
        city: c,
        orders: totalOrders,
        revenue: Math.round(totalRevenue * 100) / 100,
        yourShare: Math.round(totalRevenue * 0.05 * 100) / 100,
        topItem: topItem.name !== "-" ? `${topItem.name} (${topItem.count} шт)` : "-",
        itemsSold
      };
    } catch (error) {
      return {
        date,
        city: c,
        orders: 0,
        revenue: 0,
        yourShare: 0,
        topItem: "-",
        itemsSold: {}
      };
    }
  }
}
