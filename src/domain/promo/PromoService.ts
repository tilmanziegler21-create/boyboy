import { getDb } from "../../infra/db/sqlite";

export function startPromo15(): void {
  const db = getDb();
  const start = new Date();
  const end = new Date(start.getTime() + 15 * 60 * 1000);
  db.prepare("INSERT INTO events(date, type, payload) VALUES (?,?,?)").run(start.toISOString(), "promo15_start", JSON.stringify({ start: start.toISOString(), end: end.toISOString() }));
}

export function getLastPromoWindow(): { start: string; end: string } | null {
  const db = getDb();
  const row = db.prepare("SELECT payload FROM events WHERE type='promo15_start' ORDER BY id DESC LIMIT 1").get() as any;
  if (!row) return null;
  try {
    const p = JSON.parse(String(row.payload || "{}"));
    if (p.start && p.end) return { start: p.start, end: p.end };
  } catch {}
  return null;
}

export function isOrderInPromo(orderReserveIso: string): boolean {
  const w = getLastPromoWindow();
  if (!w) return false;
  const t = new Date(orderReserveIso).getTime();
  const a = new Date(w.start).getTime();
  const b = new Date(w.end).getTime();
  return t >= a && t <= b;
}
