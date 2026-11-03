// Stay realtime server: one Durable Object per world ("room").
// It relays positions and small events, and keeps the note/story boards of that world.
const WORLDS = ["nurse2", "nurse3", "cafe"];
const isMom = (w) => w === "nurse2" || w === "nurse3";   // 엄마방 and 엄마방2: separate rooms, same rules
const MAX_PER_ROOM = 120;
const TOPICS = new Set(["busk", "bow", "clap", "cheers", "chat", "pet", "firework", "log", "pour", "lantern"]);
const NOTE_COLORS = new Set(["yellow", "pink", "mint", "sky", "lilac"]);
const NOTE_MAX = 140, STORY_MAX = 300;
const BAD = ["씨발", "시발", "ㅅㅂ", "병신", "ㅂㅅ", "좆", "개새", "미친년", "미친놈", "fuck", "shit"];

const clean = (s, n) => String(s == null ? "" : s).replace(/[\u0000-\u001f<>]/g, " ").replace(/\s+/g, " ").trim().slice(0, n);
const bad = (s) => { const t = String(s).toLowerCase().replace(/\s/g, ""); return BAD.some((w) => t.includes(w)); };
const rid = () => Math.random().toString(36).slice(2, 8);
const num = (v, lo, hi, d) => (typeof v === "number" && isFinite(v) ? Math.max(lo, Math.min(hi, v)) : d);

function corsHeaders(env, req) {
  const origin = req.headers.get("Origin") || "";
  const ok = allowed(env, origin);
  return { "Access-Control-Allow-Origin": ok ? origin : "null", "Vary": "Origin" };
}
function allowed(env, origin) {
  const list = String(env.ALLOWED_ORIGINS || "").split(",").map((x) => x.trim()).filter(Boolean);
  return !list.length || list.includes(origin);
}

export default {
  async fetch(req, env, ctx) {
    const url = new URL(req.url);
    const cors = corsHeaders(env, req);
    if (req.method === "OPTIONS") return new Response(null, { headers: { ...cors, "Access-Control-Allow-Methods": "GET", "Access-Control-Max-Age": "86400" } });

    if (url.pathname === "/counts") {
      const cache = caches.default, key = new Request("https://stay.internal/counts");
      let hit = await cache.match(key);
      if (!hit) {
        const out = {};
        await Promise.all(WORLDS.map(async (w) => {
          try { const r = await env.ROOM.get(env.ROOM.idFromName(w)).fetch("https://do/count"); out[w] = (await r.json()).n; } catch (e) { out[w] = 0; }
        }));
        hit = new Response(JSON.stringify(out), { headers: { "content-type": "application/json", "cache-control": "public, max-age=15" } });
        ctx.waitUntil(cache.put(key, hit.clone()));
      }
      return new Response(hit.body, { headers: { "content-type": "application/json", ...cors } });
    }

    const m = url.pathname.match(/^\/room\/([a-z0-9]+)$/);
    if (m && WORLDS.includes(m[1])) {
      if (req.headers.get("Upgrade") !== "websocket") return new Response("websocket only", { status: 426 });
      if (!allowed(env, req.headers.get("Origin") || "")) return new Response("forbidden", { status: 403 });
      const stub = env.ROOM.get(env.ROOM.idFromName(m[1]));
      return stub.fetch(req);
    }
    return new Response("stay server", { status: 200, headers: cors });
  },
};

export class Room {
  constructor(state, env) {
    this.state = state;
    this.conns = new Map();
    this.pending = new Map();   // id -> serialized presence entry
    this.gone = [];
    this.timer = null;
    this.tickN = 0;
    this.dbTimer = null;
    this.dbDirty = new Set();
    this.lastAdd = new Map();   // uid -> time
    this.db = { notes: [], stories: [], lamps: [] };
    state.blockConcurrencyWhile(async () => {
      const d = await state.storage.get("db");
      if (d) this.db = { notes: d.notes || [], stories: d.stories || [], lamps: d.lamps || [] };
    });
  }

  async fetch(req) {
    const url = new URL(req.url);
    if (url.pathname === "/count") return Response.json({ n: this.conns.size });
    const world = url.pathname.split("/").pop();
    if (this.conns.size >= MAX_PER_ROOM) return new Response("full", { status: 503 });
    const pair = new WebSocketPair();
    const [client, server] = Object.values(pair);
    server.accept();
    const uid = clean(url.searchParams.get("u"), 24).replace(/[^a-z0-9]/gi, "") || rid();
    const id = rid() + this.tickN.toString(36).slice(-2);
    const c = { ws: server, id, uid, world, pres: null, pTok: 20, eTok: 12, last: Date.now() };
    this.conns.set(id, c);
    // everyone who is already here
    const snap = [];
    this.conns.forEach((o) => { if (o !== c && o.pres) snap.push(JSON.stringify(o.id) + ":" + o.pres); });
    this.send(c, '{"t":"snap","p":{' + snap.join(",") + "}}");
    this.sendDb(c, "notes"); if (isMom(world)) { this.sendDb(c, "stories"); this.sendDb(c, "lamps"); }
    server.addEventListener("message", (ev) => this.onMessage(c, ev.data));
    const drop = () => this.drop(c);
    server.addEventListener("close", drop);
    server.addEventListener("error", drop);
    if (!this.timer) this.timer = setInterval(() => this.tick(), 300);
    return new Response(null, { status: 101, webSocket: client });
  }

  send(c, str) { try { c.ws.send(str); } catch (e) { this.drop(c); } }

  drop(c) {
    if (!this.conns.delete(c.id)) return;
    this.pending.delete(c.id);
    if (c.pres) this.gone.push(c.id);
    try { c.ws.close(); } catch (e) {}
    if (!this.conns.size && this.timer) { clearInterval(this.timer); this.timer = null; }
  }

  refill(c) {
    const now = Date.now(), dt = (now - c.last) / 1000; c.last = now;
    c.pTok = Math.min(20, c.pTok + dt * 10);
    c.eTok = Math.min(12, c.eTok + dt * 5);
  }

  onMessage(c, raw) {
    if (typeof raw !== "string" || raw.length > 16000) return;
    let m; try { m = JSON.parse(raw); } catch (e) { return; }
    if (!m || typeof m.t !== "string") return;
    if (raw.length > 2000 && !(m.t === "add" && m.c === "lamps")) return;
    this.refill(c);
    if (m.t === "p") {
      if (c.pTok < 1) return; c.pTok--;
      const p = this.cleanPresence(m.d, c.world);
      if (!p) return;
      c.pres = JSON.stringify(p);
      this.pending.set(c.id, c.pres);
    } else if (m.t === "e") {
      if (c.eTok < 1) return; c.eTok--;
      if (!TOPICS.has(m.k)) return;
      const d = m.d == null ? {} : m.d;
      let js; try { js = JSON.stringify(d); } catch (e) { return; }
      if (js.length > 300) return;
      if (m.k === "chat") { if (typeof d.text !== "string" || bad(d.text)) return; }
      if (m.k === "lantern" && typeof d.text === "string" && bad(d.text)) return;
      const out = '{"t":"e","f":' + JSON.stringify(c.id) + ',"k":' + JSON.stringify(m.k) + ',"d":' + js + "}";
      this.conns.forEach((o) => { if (o !== c) this.send(o, out); });
    } else if (m.t === "add" || m.t === "upd" || m.t === "del") {
      this.onDb(c, m);
    } else if (m.t === "ph") {
      if (c.eTok < 1) return; c.eTok--;
      this.sendPhoto(c, String(m.id || ""));
    } else if (m.t === "rep") {
      if (c.eTok < 1) return; c.eTok--;
      this.report(c, String(m.id || ""));
    } else if (m.t === "cl" || m.t === "ch" || m.t === "chd") {
      if (!isMom(c.world) || c.eTok < 1) return; c.eTok--;
      this.onCheer(c, m);
    }
  }

  cleanPresence(d, world) {
    if (!d || typeof d !== "object") return null;
    const o = {
      x: num(d.x, -100, 100, 0), z: num(d.z, -100, 100, 0), yaw: num(d.yaw, -1000, 1000, 0),
      seated: d.seated === true, seat: Number.isInteger(d.seat) && d.seat >= 0 && d.seat < 4000 ? d.seat : null,
      world, drink: typeof d.drink === "string" ? clean(d.drink, 12) : null, snack: d.snack === true, lie: d.lie === true,
      lamp: Array.isArray(d.lamp) && d.lamp.length === 2 ? [num(d.lamp[0], -100, 100, 0), num(d.lamp[1], -100, 100, 0)] : null,
      name: clean(d.name, 12), msg: clean(d.msg, 30),
      mom: typeof d.mom === "string" ? clean(d.mom, 16) : null, face: typeof d.face === "string" ? clean(d.face, 16) : undefined,
      look: Array.isArray(d.look) && d.look.length === 3 && d.look.every((v) => Number.isInteger(v) && v >= 0 && v < 10) ? d.look : null,
      bacc: Number.isInteger(d.bacc) ? d.bacc & 7 : 0, foc: d.foc === 2 ? 2 : d.foc === 1 ? 1 : 0,
    };
    if (bad(o.name)) o.name = ""; if (bad(o.msg)) o.msg = "";
    return o;
  }

  tick() {
    this.tickN++;
    if (this.pending.size || this.gone.length) {
      const entries = [];
      this.pending.forEach((js, id) => entries.push([id, JSON.stringify(id) + ":" + js]));
      const left = JSON.stringify(this.gone);
      this.conns.forEach((o) => {
        const parts = [];
        for (const [id, s] of entries) if (id !== o.id) parts.push(s);
        if (!parts.length && left === "[]") return;
        this.send(o, '{"t":"u","u":{' + parts.join(",") + '},"l":' + left + "}");
      });
      this.pending.clear(); this.gone = [];
    }
    if (this.tickN % 80 === 0) this.conns.forEach((o) => this.send(o, '{"t":"k"}'));   // keep-alive
    if (this.tickN % 200 === 0 && this.sweepLamps()) this.markDb("lamps");
  }

  // ---- boards ----
  sendDb(c, coll) {
    const mask = (d) => ({ ...d, by: d.by === c.uid ? c.uid : "" });
    let docs;
    if (coll === "lamps") {
      const now = Date.now();
      docs = (this.db.lamps || []).filter((d) => d.until > now)
        .map((d) => ({ id: d.id, text: d.text, name: d.name, at: d.at, until: d.until, slot: d.slot, hugs: d.hugs || 0, ph: !!d.ph, by: d.by === c.uid ? c.uid : "" }));
    } else {
      docs = (this.db[coll] || []).slice().sort((a, b) => b.at - a.at).slice(0, coll === "notes" ? 400 : 100)
        .map((d) => { const o = mask(d); delete o.hb; return o; });
    }
    this.send(c, JSON.stringify({ t: "db", c: coll, docs }));
  }
  err(c, coll, m) { this.send(c, JSON.stringify({ t: "err", c: coll, m })); }
  dropLampData(d) {
    if (d.ph) this.state.storage.delete("ph:" + d.id).catch(() => {});
    this.state.storage.delete("ch:" + d.id).catch(() => {});
  }
  async onCheer(c, m) {
    const id = String(m.id || ""), l = (this.db.lamps || []).find((x) => x.id === id && x.until > Date.now());
    if (!l) return;
    const key = "ch:" + id;
    let arr = []; try { arr = (await this.state.storage.get(key)) || []; } catch (e) {}
    const view = () => JSON.stringify({ t: "cl", id, d: arr.map((x) => ({ k: x.k, t: x.t, n: x.n, at: x.at })) });
    if (m.t === "cl") { this.send(c, view()); return; }
    if (m.t === "chd") {
      if (l.by !== c.uid) return;
      const n = arr.length; arr = arr.filter((x) => x.k !== String(m.k || ""));
      if (arr.length === n) return;
      await this.state.storage.put(key, arr).catch(() => {});
      l.hugs = arr.length; this.markDb("lamps"); this.send(c, view());
      return;
    }
    // ch: leave one short cheering message on someone else's lamp
    const now = Date.now();
    if (l.by === c.uid) { this.err(c, "cheer", "own"); return; }
    if (now - (this.lastAdd.get("ch" + c.uid) || 0) < 6000) { this.err(c, "cheer", "slow"); return; }
    const text = clean(m.text, 40);
    if (!text || bad(text)) { this.err(c, "cheer", "bad"); return; }
    if (arr.some((x) => x.u === c.uid)) { this.err(c, "cheer", "dup"); return; }
    if (arr.length >= 40) { this.err(c, "cheer", "full"); return; }
    arr.push({ k: rid(), t: text, n: clean(m.name, 12) || "익명", at: now, u: c.uid });
    this.lastAdd.set("ch" + c.uid, now);
    await this.state.storage.put(key, arr).catch(() => {});
    l.hugs = arr.length; this.markDb("lamps"); this.send(c, view());
  }
  sweepLamps() {
    const now = Date.now(), keep = [], drop = [];
    (this.db.lamps || []).forEach((d) => (d.until > now ? keep : drop).push(d));
    if (!drop.length) return false;
    this.db.lamps = keep;
    drop.forEach((d) => this.dropLampData(d));
    return true;
  }
  async sendPhoto(c, id) {
    const d = (this.db.lamps || []).find((x) => x.id === id && x.ph);
    let data = null;
    if (d) { try { data = (await this.state.storage.get("ph:" + id)) || null; } catch (e) {} }
    this.send(c, JSON.stringify({ t: "ph", id, d: data }));
  }
  report(c, id) {
    const d = (this.db.lamps || []).find((x) => x.id === id);
    if (!d || !d.ph || d.by === c.uid) return;
    d.rp = d.rp || [];
    if (d.rp.includes(c.uid)) return;
    d.rp.push(c.uid);
    if (d.rp.length >= 3) { d.ph = false; this.state.storage.delete("ph:" + id).catch(() => {}); }
    this.markDb("lamps");
  }
  markDb(coll) {
    this.dbDirty.add(coll);
    if (this.dbTimer) return;
    this.dbTimer = setTimeout(async () => {
      this.dbTimer = null;
      const colls = [...this.dbDirty]; this.dbDirty.clear();
      try { await this.state.storage.put("db", this.db); } catch (e) {}
      colls.forEach((cl) => this.conns.forEach((o) => { if (cl === "notes" || isMom(o.world)) this.sendDb(o, cl); }));
    }, 500);
  }
  onLamp(c, m, now) {
    this.sweepLamps();
    const list = this.db.lamps;
    if (m.t === "add") {
      const d = m.d || {};
      if (now - (this.lastAdd.get("lamp" + c.uid) || 0) < 15000) { this.err(c, "lamps", "slow"); return; }
      const text = String(d.text == null ? "" : d.text).replace(/[\u0000-\u0009\u000b-\u001f<>]/g, " ").replace(/\n{3,}/g, "\n\n").trim().slice(0, 100);
      if (!text || bad(text)) { this.err(c, "lamps", "bad"); return; }
      const oi = list.findIndex((x) => x.by === c.uid);
      if (oi >= 0) { const o = list[oi]; list.splice(oi, 1); this.dropLampData(o); }
      const used = new Set(list.map((x) => x.slot));
      let slot = -1;
      for (let i = 0; i < 100; i++) if (!used.has(i)) { slot = i; break; }
      if (slot < 0) { this.err(c, "lamps", "full"); return; }
      const photo = typeof d.photo === "string" && d.photo.length <= 14000 && /^data:image\/jpeg;base64,[A-Za-z0-9+/=]+$/.test(d.photo) ? d.photo : null;
      const id = rid() + now.toString(36);
      if (photo) this.state.storage.put("ph:" + id, photo).catch(() => {});
      list.push({ id, text, name: clean(d.name, 12) || "익명", at: now, until: now + 7 * 86400000, slot, hugs: 0, by: c.uid, ph: !!photo, hb: [], rp: [] });
      this.lastAdd.set("lamp" + c.uid, now);
      this.markDb("lamps");
    } else if (m.t === "del") {
      const i = list.findIndex((x) => x.id === m.id);
      if (i >= 0 && list[i].by === c.uid) { const o = list[i]; list.splice(i, 1); this.dropLampData(o); this.markDb("lamps"); }
    }
  }
  onDb(c, m) {
    const coll = m.c;
    if (coll !== "notes" && !((coll === "stories" || coll === "lamps") && isMom(c.world))) return;
    const list = this.db[coll];
    const now = Date.now();
    if (coll === "lamps") { this.onLamp(c, m, now); return; }
    if (m.t === "add") {
      const d = m.d || {};
      const gap = coll === "notes" ? 12000 : 45000;
      const lk = coll + c.uid;
      if (now - (this.lastAdd.get(lk) || 0) < gap) return;
      let doc;
      if (coll === "notes") {
        const text = String(d.text || "").replace(/[\u0000-\u0009\u000b-\u001f<>]/g, " ").replace(/\n{3,}/g, "\n\n").trim().slice(0, NOTE_MAX);
        if (!text || bad(text)) return;
        doc = { id: rid() + now.toString(36), world: c.world, text, color: NOTE_COLORS.has(d.color) ? d.color : "yellow", at: now, by: c.uid };
      } else {
        const text = String(d.text || "").replace(/[\u0000-\u0009\u000b-\u001f<>]/g, " ").replace(/\n{3,}/g, "\n\n").trim().slice(0, STORY_MAX);
        if (text.length < 5 || bad(text)) return;
        doc = { id: rid() + now.toString(36), text, name: clean(d.name, 12) || "익명", at: now, hugs: 0, by: c.uid, hb: [] };
      }
      this.lastAdd.set(lk, now);
      if (this.lastAdd.size > 2000) this.lastAdd.clear();
      list.push(doc);
      const keep = coll === "notes" ? 400 : 200, maxAge = 30 * 86400000;
      this.db[coll] = list.filter((x) => now - x.at < maxAge).sort((a, b) => b.at - a.at).slice(0, keep);
      this.markDb(coll);
    } else if (m.t === "del") {
      if (coll !== "notes") return;
      const i = list.findIndex((x) => x.id === m.id);
      if (i >= 0 && list[i].by === c.uid) { list.splice(i, 1); this.markDb(coll); }
    } else if (m.t === "upd") {
      if (coll !== "stories") return;
      const st = list.find((x) => x.id === m.id);
      if (!st || !st.hb) return;
      if (st.hb.includes(c.uid) || st.hb.length >= 500) return;
      st.hb.push(c.uid); st.hugs = (st.hugs || 0) + 1;
      this.markDb(coll);
    }
  }
}
