const store = new Map<string, { payload: string; ts: number }>();
const TTL_MS = 10 * 60 * 1000;
let counter = 0;

export function encodeCb(payload: string): string {
  try {
    const b64 = Buffer.from(payload, 'utf8').toString('base64').replace(/=+$/,'');
    return `e:${b64}`;
  } catch {
    const key = `h:${(++counter).toString(36)}`;
    store.set(key, { payload, ts: Date.now() });
    return key;
  }
}

export function decodeCb(data: string): string {
  if (data.startsWith('e:')) {
    const b64 = data.slice(2);
    try { return Buffer.from(b64, 'base64').toString('utf8'); } catch { return data; }
  }
  if (data.startsWith('h:')) {
    const v = store.get(data);
    if (v) {
      if (Date.now() - v.ts > TTL_MS) {
        store.delete(data);
        return '__expired__';
      }
      return v.payload;
    }
    return '__expired__';
  }
  return data;
}
