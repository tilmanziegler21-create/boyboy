import { getDb } from "../../infra/db/sqlite";
import { RESERVATION_TTL_MS } from "../../core/constants";
import { OrderItem, Product } from "../../core/types";
import { getProducts, updateProductQty } from "../../infra/data";
import { useSheets } from "../../infra/config";
import { logger } from "../../infra/logger";
import { addMinutes } from "../../core/time";

const qtyReserved: Map<number, number> = new Map();
const deductLocks: Map<number, Promise<void>> = new Map();
async function runWithLock(pid: number, fn: () => Promise<void>) {
  const prev = deductLocks.get(pid) || Promise.resolve();
  const next = prev.then(fn).catch(() => {});
  deductLocks.set(pid, next);
  await next;
}

export async function restoreReservations() {
  const db = getDb();
  const nowIso = new Date().toISOString();
  const rows = db
    .prepare(
      "SELECT product_id, SUM(qty) AS s FROM reservations WHERE released=0 AND expiry_timestamp > ? GROUP BY product_id"
    )
    .all(nowIso) as { product_id: number; s: number }[];
  qtyReserved.clear();
  for (const r of rows) qtyReserved.set(r.product_id, r.s || 0);
  logger.info("Reservations restored", { count: rows.length });
}

function getReserved(product_id: number) {
  return qtyReserved.get(product_id) || 0;
}

export async function validateStock(product_id: number, qty: number): Promise<boolean> {
  const products = await getProducts();
  const p = products.find((x) => x.product_id === product_id);
  if (!p) return false;
  return p.qty_available - getReserved(product_id) >= qty;
}

export async function reserveItems(items: OrderItem[], order_id?: number): Promise<void> {
  const db = getDb();
  const now = new Date();
  const expiry = addMinutes(now, RESERVATION_TTL_MS / 60000);
  const nowIso = now.toISOString();
  const expIso = expiry.toISOString();
  const products = await getProducts();
  for (const it of items) {
    const p = products.find((x) => x.product_id === it.product_id);
    if (!p) throw new Error("Product not found");
    const reserved = getReserved(it.product_id);
    if (p.qty_available - reserved < it.qty) throw new Error("Insufficient stock");
  }
  const tx = db.transaction(() => {
    for (const it of items) {
      db.prepare(
        "INSERT INTO reservations(order_id, product_id, qty, reserve_timestamp, expiry_timestamp, released) VALUES (?, ?, ?, ?, ?, 0)"
      ).run(order_id ?? 0, it.product_id, it.qty, nowIso, expIso);
      qtyReserved.set(it.product_id, getReserved(it.product_id) + it.qty);
    }
  });
  tx();
}

export async function releaseReservation(items: OrderItem[], order_id?: number): Promise<void> {
  const db = getDb();
  const tx = db.transaction(() => {
    for (const it of items) {
      db.prepare(
        "UPDATE reservations SET released=1 WHERE order_id = ? AND product_id = ? AND released = 0"
      ).run(order_id ?? 0, it.product_id);
      qtyReserved.set(it.product_id, Math.max(0, getReserved(it.product_id) - it.qty));
    }
  });
  tx();
}

export async function finalDeduction(items: OrderItem[]): Promise<void> {
  for (const it of items) {
    await runWithLock(it.product_id, async () => {
      const products = await getProducts();
      const p = products.find((x) => x.product_id === it.product_id);
      if (!p) throw new Error("Product not found");
      const newQty = p.qty_available - it.qty;
      if (newQty < 0) throw new Error("Negative stock");
      await updateProductQty(it.product_id, newQty);
      try { logger.info("finalDeduction", { product_id: it.product_id, new_qty: newQty }); } catch {}
    });
  }
}

export function getQtyReservedSnapshot(): Record<number, number> {
  const out: Record<number, number> = {};
  for (const [k, v] of qtyReserved.entries()) out[k] = v;
  return out;
}
