import TelegramBot from "node-telegram-bot-api";
import { getDb } from "../../infra/db/sqlite";
import { setDelivered, getOrderById } from "../../domain/orders/OrderService";
import { getProducts } from "../../infra/data";
import { encodeCb, decodeCb } from "../cb";
import { logger } from "../../infra/logger";
import { batchGet } from "../../infra/sheets/SheetsClient";
import { shopConfig } from "../../config/shopConfig";

export function registerCourierFlow(bot: TelegramBot) {
  bot.onText(/\/courier/, async (msg) => {
    const chatId = msg.chat.id;
    const db = getDb();
    const map = db.prepare("SELECT tg_id, courier_id FROM couriers WHERE tg_id = ? OR courier_id = ?").get(msg.from?.id, msg.from?.id) as any;
    const idA = Number(map?.tg_id || msg.from?.id);
    const idB = Number(map?.courier_id || msg.from?.id);
    const myList = db
      .prepare("SELECT o.order_id, o.user_id, o.delivery_interval, o.delivery_exact_time, u.username FROM orders o LEFT JOIN users u ON o.user_id=u.user_id WHERE o.status IN ('pending','courier_assigned') AND o.courier_id IN (?, ?) ORDER BY o.order_id DESC LIMIT 100")
      .all(idA, idB) as any[];
    let lines = myList.map((o) => `#${o.order_id} ${o.username ? "@" + o.username : "Клиент"} · ${o.delivery_exact_time || "?"}`);
    let keyboard = myList.map((o) => [
      { text: `Выдача ${o.order_id}`, callback_data: encodeCb(`courier_issue:${o.order_id}`) },
      { text: `Не выдано ${o.order_id}`, callback_data: encodeCb(`courier_not_issued:${o.order_id}`) }
    ]);
    if (myList.length === 0) {
      try {
        const s = `orders_${shopConfig.cityCode}`;
        const vr = await batchGet([`${s}!A:L`]);
        const values = vr[0]?.values || [];
        const headers = values[0] || [];
        const rows = values.slice(1);
        const idx = (name: string) => headers.indexOf(name);
        const idIdx = idx("order_id");
        const courierIdx = idx("courier_id");
        const statusIdx = idx("status");
        const timeIdx = idx("delivery_time");
        const dateIdx = idx("delivery_date");
        const userIdx = idx("user_id");
        const pending = rows.filter((r) => {
          const cid = String(courierIdx >= 0 ? r[courierIdx] || "" : "");
          const st = String(statusIdx >= 0 ? r[statusIdx] || "" : "").toLowerCase();
          return (cid === String(idA) || cid === String(idB)) && (st === "pending" || st === "courier_assigned" || st === "confirmed");
        }).map((r) => ({
          order_id: Number(idIdx >= 0 ? r[idIdx] || 0 : 0),
          user_id: Number(userIdx >= 0 ? r[userIdx] || 0 : 0),
          delivery_exact_time: `${String(dateIdx>=0?r[dateIdx]||"": "")} ${String(timeIdx>=0?r[timeIdx]||"": "")}`
        }));
        lines = pending.map((o) => `#${o.order_id} Клиент · ${o.delivery_exact_time || "?"}`);
        keyboard = pending.map((o) => [
          { text: `Выдача ${o.order_id}`, callback_data: encodeCb(`courier_issue:${o.order_id}`) },
          { text: `Не выдано ${o.order_id}`, callback_data: encodeCb(`courier_not_issued:${o.order_id}`) }
        ]);
      } catch (e) {
        try { logger.warn("courier sheets fallback error", { error: String(e) }); } catch {}
      }
    }
    keyboard.push([{ text: "🏠 Главное меню", callback_data: encodeCb("back:main") }]);
    await bot.sendMessage(chatId, lines.join("\n") || "Нет заказов", { reply_markup: { inline_keyboard: keyboard } });
  });

  bot.on("callback_query", async (q) => {
    try { await bot.answerCallbackQuery(q.id); } catch {}
    try { logger.info("COURIER_CLICK", { data: q.data, courier_id: q.from?.id }); } catch {}
    let data = q.data || "";
    data = decodeCb(data);
    if (data === "__expired__") {
      const chatId = q.message?.chat.id || 0;
      await bot.sendMessage(chatId, "Кнопка устарела. Откройте /courier для актуального списка.");
      return;
    }
    const chatId = q.message?.chat.id || 0;
    if (data.startsWith("courier_issue:")) {
      const id = Number(data.split(":")[1]);
      await setDelivered(id, q.from.id);
      try {
        await bot.deleteMessage(chatId, q.message?.message_id as number);
      } catch {
        try { await bot.editMessageText(`Заказ #${id} выдан`, { chat_id: chatId, message_id: q.message?.message_id as number }); } catch {}
      }
      try {
        const db = getDb();
        const map = db.prepare("SELECT tg_id, courier_id FROM couriers WHERE tg_id = ? OR courier_id = ?").get(q.from.id, q.from.id) as any;
        const idA = Number(map?.tg_id || q.from.id);
        const idB = Number(map?.courier_id || q.from.id);
        const myList = db
          .prepare("SELECT o.order_id, o.user_id, o.delivery_interval, o.delivery_exact_time, u.username FROM orders o LEFT JOIN users u ON o.user_id=u.user_id WHERE o.status IN ('pending','courier_assigned') AND o.courier_id IN (?, ?) ORDER BY o.order_id DESC LIMIT 100")
          .all(idA, idB) as any[];
        const lines2 = myList.map((o) => `#${o.order_id} ${o.username ? "@" + o.username : "Клиент"} · ${o.delivery_exact_time || "?"}`);
        const keyboard2 = myList.map((o) => [
          { text: `Выдача ${o.order_id}`, callback_data: encodeCb(`courier_issue:${o.order_id}`) },
          { text: `Не выдано ${o.order_id}`, callback_data: encodeCb(`courier_not_issued:${o.order_id}`) }
        ]);
        keyboard2.push([{ text: "🏠 Главное меню", callback_data: encodeCb("back:main") }]);
        await bot.sendMessage(chatId, lines2.join("\n") || "Нет заказов", { reply_markup: { inline_keyboard: keyboard2 } });
      } catch {}
      const order = await getOrderById(id);
      if (order) { try { await bot.sendMessage(order.user_id, "Спасибо за заказ! Приходите к нам ещё."); } catch {} }
    } else if (data.startsWith("courier_not_issued:")) {
      const id = Number(data.split(":")[1]);
      try {
        const { setNotIssued, getOrderById } = await import("../../domain/orders/OrderService");
        await setNotIssued(id);
        try { await bot.deleteMessage(chatId, q.message?.message_id as number); } catch {}
        const order = await getOrderById(id);
        if (order) {
          try { await bot.sendMessage(order.user_id, "❗ Заказ не выдан и удалён из очереди. Оформите новый заказ при необходимости." ); } catch {}
        }
        const db0 = getDb();
        const map0 = db0.prepare("SELECT tg_id, courier_id FROM couriers WHERE tg_id = ? OR courier_id = ?").get(q.from.id, q.from.id) as any;
        const idA0 = Number(map0?.tg_id || q.from.id);
        const idB0 = Number(map0?.courier_id || q.from.id);
        const myList = db0
          .prepare("SELECT o.order_id, o.user_id, o.delivery_interval, o.delivery_exact_time, u.username FROM orders o LEFT JOIN users u ON o.user_id=u.user_id WHERE o.status IN ('pending','courier_assigned') AND o.courier_id IN (?, ?) ORDER BY o.order_id DESC LIMIT 100")
          .all(idA0, idB0) as any[];
        const lines2 = myList.map((o) => `#${o.order_id} ${o.username ? "@" + o.username : "Клиент"} · ${o.delivery_exact_time || "?"}`);
        const keyboard2 = myList.map((o) => [
          { text: `Выдача ${o.order_id}`, callback_data: encodeCb(`courier_issue:${o.order_id}`) },
          { text: `Не выдано ${o.order_id}`, callback_data: encodeCb(`courier_not_issued:${o.order_id}`) }
        ]);
        keyboard2.push([{ text: "🏠 Главное меню", callback_data: encodeCb("back:main") }]);
        await bot.sendMessage(chatId, lines2.join("\n") || "Нет заказов", { reply_markup: { inline_keyboard: keyboard2 } });
      } catch {}
    }
  });
}
