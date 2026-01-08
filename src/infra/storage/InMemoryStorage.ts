import { OrderItem } from "../../core/types";
export const carts = new Map<number, OrderItem[]>();
export const userStates = new Map<number, any>();
export const userRerollCount = new Map<number, number>();

import fs from "fs";
import path from "path";
const STORAGE_FILE = path.join(__dirname, "../../../data/state.json");

export function loadState() {
  try {
    if (fs.existsSync(STORAGE_FILE)) {
      const data = JSON.parse(fs.readFileSync(STORAGE_FILE, "utf-8"));
      if (data.carts) for (const [k, v] of Object.entries<any>(data.carts)) carts.set(Number(k), v || []);
      if (data.userStates) for (const [k, v] of Object.entries<any>(data.userStates)) userStates.set(Number(k), v || {});
      if (data.userRerollCount) for (const [k, v] of Object.entries<any>(data.userRerollCount)) userRerollCount.set(Number(k), Number(v) || 0);
      console.log("[STORAGE] State loaded from disk");
    }
  } catch (e) {
    console.error("[STORAGE ERROR] Failed to load state:", e);
  }
}

export function saveState() {
  try {
    const data = {
      carts: Object.fromEntries(carts),
      userStates: Object.fromEntries(userStates),
      userRerollCount: Object.fromEntries(userRerollCount)
    };
    const dir = path.dirname(STORAGE_FILE);
    if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });
    fs.writeFileSync(STORAGE_FILE, JSON.stringify(data, null, 2));
    console.log("[STORAGE] State saved to disk");
  } catch (e) {
    console.error("[STORAGE ERROR] Failed to save state:", e);
  }
}

setInterval(saveState, 10000);
process.on("SIGINT", () => { console.log("[STORAGE] Saving state before exit..."); saveState(); process.exit(0); });
process.on("SIGTERM", () => { console.log("[STORAGE] Saving state before exit..."); saveState(); process.exit(0); });
