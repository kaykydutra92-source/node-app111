import "dotenv/config";
import pino from "pino";
import fs from "fs";
import path from "path";
import express from "express";
import TelegramBot from "node-telegram-bot-api";
import { Connection, PublicKey, Connection as SolConn } from "@solana/web3.js";
import { logEvent } from "./db.js";  // NEW: SQLite logging module

const log = pino({ level: process.env.DEBUG === "true" ? "debug" : "info" });

// =========================
// PLAN PROFILES (FREE vs PAID)
// =========================
const PLAN = String(process.env.PLAN_TIER || "free").toLowerCase();

// Defaults per plan (can be overridden in .env)
if (PLAN === "free") {
  process.env.CONCURRENCY ||= "1";
  process.env.RATE_LIMIT_RPS ||= "1";          // target: ≥1 req/s
  process.env.RATE_LIMIT_REFILL_MS ||= "1000";
  process.env.AFTER_HTTP_MS ||= "0";
  process.env.CIRCUIT_429_THRESHOLD ||= "2";
  process.env.CIRCUIT_OPEN_MS ||= "15000";     // 15s
  process.env.ONLY_TODAY ||= "true";
  process.env.ONLY_TODAY_LOOKUP ||= "true";    // lookup expensive; ON by default for today-only tokens
  process.env.WS_COMMITMENT ||= "processed";
  process.env.RPC_COMMITMENT ||= "confirmed";
  process.env.COMMITMENT ||= "confirmed";
  process.env.PID_CAP ||= "1";                 // 1 PID on free (reduce 429s)
} else {
  // paid / dev / business (Atlas configurable via .env)
  process.env.CONCURRENCY ||= "3";
  process.env.RATE_LIMIT_RPS ||= "8";          // higher throughput on paid plan
  process.env.RATE_LIMIT_REFILL_MS ||= "1000";
  process.env.AFTER_HTTP_MS ||= "0";
  process.env.CIRCUIT_429_THRESHOLD ||= "6";
  process.env.CIRCUIT_OPEN_MS ||= "10000";
  process.env.ONLY_TODAY ||= "true";
  process.env.ONLY_TODAY_LOOKUP ||= "true";    // ensure token creation date lookup by default
  process.env.WS_COMMITMENT ||= "processed";
  process.env.RPC_COMMITMENT ||= "confirmed";
  process.env.COMMITMENT ||= "confirmed";
  process.env.PID_CAP ||= "999";
}

// =========================
// Utils
// =========================
function envBool(name, def = false) {
  const v = process.env[name];
  if (v == null) return def;
  return ["1", "true", "yes", "on"].includes(String(v).toLowerCase());
}
function envInt(name, def = 0) {
  const v = process.env[name];
  return v == null ? def : parseInt(v, 10);
}
function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

function safePkStr(x) {
  try { return new PublicKey(x).toBase58(); } catch { return null; }
}
function getKeyPubkey(k) {
  return k?.pubkey || k || null;
}

// =========================
// Subscription modes & PID control
// =========================
// SUB_MODE: program_logs | logs_all | program_subscribe
const SUB_MODE = String(process.env.SUB_MODE || "program_logs").toLowerCase();

// PROGRAM_LOGS_MODE: off | only | first
// - only: use ONLY the "programLogs"/"logs" list from programs.json (in order)
// - first: prioritize the "programLogs" list, then fill with meteora+extra
// - off: old behavior (meteora + extra)
const PROGRAM_LOGS_MODE = String(process.env.PROGRAM_LOGS_MODE || "off").toLowerCase();

// current target PIDs (for gating in logs_all/webhook modes)
let __TARGET_PIDS_SET = new Set();
function setTargetPids(pids) {
  __TARGET_PIDS_SET = new Set((pids || []).map(String));
}
function logsContainAnyPid(logMsgs = [], pidSet = __TARGET_PIDS_SET) {
  if (!pidSet || pidSet.size === 0) return true;  // no target => allow all
  for (const line of logMsgs || []) {
    for (const pid of pidSet) {
      if (line.includes(pid)) return true;
    }
  }
  return false;
}
function txLikeTouchesAnyPid(txLike, pidSet = __TARGET_PIDS_SET) {
  if (!pidSet || pidSet.size === 0) return true;
  const keys = txLike?.transaction?.message?.accountKeys || [];
  for (const k of keys) {
    const pk = getKeyPubkey(k);
    if (pk && pidSet.has(pk)) return true;
  }
  const logs = txLike?.meta?.logMessages || txLike?.logs || [];
  return logsContainAnyPid(logs, pidSet);
}

// =========================
// Known Program IDs (to NEVER confuse with mint addresses)
// (will be supplemented at runtime with subscribed PIDs)
// =========================
let __ALL_PROGRAM_IDS_SET = new Set([
  "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA", // SPL Token Program
  "11111111111111111111111111111111",            // System Program
  "ComputeBudget111111111111111111111111111111", // Compute Budget Program
  "Stake11111111111111111111111111111111111111", // Stake Program
  "Vote111111111111111111111111111111111111111", // Vote Program
]);

function isValidMint(mintStr) {
  const s = safePkStr(mintStr);
  if (!s) return false;
  // Do not allow known Program IDs to be misidentified as mints:
  if (__ALL_PROGRAM_IDS_SET.has(s)) return false;
  return true;
}

// ======================================================================
// DEDUP START — send only once per token (configurable scope)
// ======================================================================
const DEDUP_ENABLED = ["1", "true", "yes", "on"].includes(String(process.env.DEDUP_ENABLED ?? "true").toLowerCase());
const DEDUP_SCOPE = String(process.env.DEDUP_SCOPE || "mint_day").toLowerCase();
  // options: mint | mint_day | mint_pool_day | mint_forever
const DEDUP_TTL_SECONDS = parseInt(process.env.DEDUP_TTL_SECONDS || "86400", 10);
const DEDUP_FILE = process.env.DEDUP_FILE
  ? path.resolve(process.cwd(), process.env.DEDUP_FILE)
  : path.resolve(process.cwd(), "dedup-cache.json");

function startOfUtcDaySec(tsSec) {
  const d = new Date((tsSec ?? (Date.now() / 1000 | 0)) * 1000);
  return (Date.UTC(d.getUTCFullYear(), d.getUTCMonth(), d.getUTCDate()) / 1000) | 0;
}

class Dedup {
  constructor() {
    this.map = new Map(); // key -> expireAtSec (0 => forever)
    this.dirty = false;
    this._load();
    this._startPruner();
    process.on("SIGINT", () => { try { this._save(true); } finally { process.exit(0); } });
    process.on("SIGTERM", () => { try { this._save(true); } finally { process.exit(0); } });
  }
  _keyForEvent(ev) {
    const base = ev?.mints?.base || ev?.mints?.quote;
    if (!base) return null;
    const pool = ev?.pool_authority || "-";
    const day = startOfUtcDaySec(ev?.timestamp || (Date.now() / 1000 | 0));
    switch (DEDUP_SCOPE) {
      case "mint":
      case "mint_forever":
        return `mint:${base}`;
      case "mint_pool_day":
        return `mpd:${base}:${pool}:${day}`;
      case "mint_day":
      default:
        return `md:${base}:${day}`;
    }
  }
  _expiryForEvent(ev) {
    if (DEDUP_SCOPE === "mint" || DEDUP_SCOPE === "mint_forever") return 0;
    const baseDay = startOfUtcDaySec(ev?.timestamp || (Date.now() / 1000 | 0));
    return baseDay + (Number.isFinite(DEDUP_TTL_SECONDS) ? DEDUP_TTL_SECONDS : 86400);
  }
  shouldSkip(ev) {
    if (!DEDUP_ENABLED) return false;
    const key = this._keyForEvent(ev);
    if (!key) return false;
    const now = (Date.now() / 1000) | 0;
    const exp = this.map.get(key);
    if (exp == null) return false;
    if (exp === 0) return true;
    if (exp > now) return true;
    // expired
    this.map.delete(key);
    this.dirty = true;
    return false;
  }
  markSent(ev) {
    if (!DEDUP_ENABLED) return;
    const key = this._keyForEvent(ev);
    if (!key) return;
    const exp = this._expiryForEvent(ev);
    this.map.set(key, exp);
    this.dirty = true;
    this._save();
  }
  _load() {
    try {
      if (fs.existsSync(DEDUP_FILE)) {
        const arr = JSON.parse(fs.readFileSync(DEDUP_FILE, "utf8"));
        if (Array.isArray(arr)) {
          for (const [k, v] of arr) this.map.set(k, v);
        }
      }
    } catch (e) {
      console.warn("[dedup] load error, starting empty:", e?.message || e);
      this.map.clear();
    }
  }
  _save(force = false) {
    try {
      if (!this.dirty && !force) return;
      fs.writeFileSync(DEDUP_FILE, JSON.stringify([...this.map.entries()]));
      this.dirty = false;
    } catch (e) {
      console.warn("[dedup] save error:", e?.message || e);
    }
  }
    _startPruner() {
    // Mantém o processo vivo: NÃO usar .unref() aqui.
    // Assim, mesmo sem WS/webhook ativo, este timer referenciado
    // impede que o Node finalize imediatamente.
    setInterval(() => {
      const now = (Date.now() / 1000) | 0;
      let removed = 0;
      for (const [k, exp] of this.map.entries()) {
        if (exp && exp > 0 && exp <= now) {
          this.map.delete(k);
          removed++;
        }
      }
      if (removed > 0) this._save();
    }, 10 * 60 * 1000);
  }

}


// Depois desta linha existente:
const dedup = new Dedup();

// ADICIONE isto logo abaixo:
if (["1","true","on","yes"].includes(String(process.env.KEEPALIVE_FORCE ?? "true").toLowerCase())) {
  // Keep-alive baratinho: impede o Node de sair quando o app está “ocioso”.
  setInterval(() => {}, 30 * 1000);
}

// ======================================================================
// DEDUP END
// ======================================================================

// =========================
// Multi-Factor Liquidity Detection toggles
// =========================
const MF_ENABLED = envBool("MULTIFACTOR_ENABLED", false);
const MF_REQUIRE_TWO_VAULTS = envBool("MF_REQUIRE_TWO_VAULTS", false);
const MF_REQUIRE_LP_MINT = envBool("MF_REQUIRE_LP_MINT", false);
const MF_REQUIRE_FIRST_HISTORY = envBool("MF_REQUIRE_FIRST_HISTORY", false);
const MF_MAX_RATIO = envInt("MF_MAX_RATIO", 0);
const MF_CHECK_PROVIDER_IS_MINT_AUTH = envBool("MF_CHECK_PROVIDER_IS_MINT_AUTH", false);

// =========================
// Telegram notify
// =========================
let __tgBot = null;
function ensureBot() {
  if (!__tgBot) {
    const tok = process.env.TELEGRAM_BOT_TOKEN || process.env.BOT_TOKEN;
    if (!tok) return null;
    __tgBot = new TelegramBot(tok, { polling: false });
  }
  return __tgBot;
}
async function notify(event) {
  const bot = ensureBot();
  const chatId = process.env.TELEGRAM_CHAT_ID || process.env.CHAT_ID;
  if (!bot || !chatId) return;

  const url = `https://solscan.io/tx/${event.signature}`;
  const base = event.mints?.base;
  const quote = event.mints?.quote;
  const baseAmt = event.amounts?.base_in ?? 0;
  const quoteAmt = event.amounts?.quote_in ?? 0;
  const fmt = (mint, amt) => (mint ? `• ${amt} ${mint}` : "");

  // Simplified alert: only essential info (mint addresses, amounts, DEX, link)
  const body = `🧪 *LP Detected* — ${event.dex}\n` +
               `*Mints*: \`${base || "unknown"}\` / \`${quote || "unknown"}\`\n` +
               `*Amounts*:\n${fmt(base, baseAmt)}\n${fmt(quote, quoteAmt)}\n` +
               `[Solscan](${url})`;

  await bot.sendMessage(chatId, body, { parse_mode: "Markdown" });
}

// =========================
// HTTP endpoint pool + 429 circuit breaker (1 evt/sec target)
// =========================
const PRIMARY_HTTP = (process.env.RPC_HTTP_URL || process.env.RPC_URL || "").trim();
const ALT_HTTP = (process.env.RPC_ALT_URLS || "").split(",").map(s => s.trim()).filter(Boolean);
const HTTP_URLS = [PRIMARY_HTTP, ...ALT_HTTP].filter(Boolean);
if (HTTP_URLS.length === 0) {
  throw new Error("Configure RPC_HTTP_URL (and optionally RPC_ALT_URLS)");
}

let __httpIndex = 0;
function nextHttpUrl() {
  __httpIndex = (__httpIndex + 1) % HTTP_URLS.length;
  return HTTP_URLS[__httpIndex];
}

const CIRCUIT_429_THRESHOLD = envInt("CIRCUIT_429_THRESHOLD");
const CIRCUIT_OPEN_MS = envInt("CIRCUIT_OPEN_MS");
let __429Count = 0;
let __circuitOpenUntil = 0;
function circuitOpen() { return Date.now() < __circuitOpenUntil; }
function on429() {
  __429Count += 1;
  if (__429Count >= CIRCUIT_429_THRESHOLD) {
    __circuitOpenUntil = Date.now() + CIRCUIT_OPEN_MS;
    log.warn({ cooldown_ms: CIRCUIT_OPEN_MS }, "[429] circuit OPEN");
    __429Count = 0;
  }
}
function on429Success() { __429Count = 0; }

// Fixed-rate scheduler (guarantees ≥1 op/s if RATE_LIMIT_RPS ≥ 1)
let RATE_RPS = Math.max(0.1, parseFloat(process.env.RATE_LIMIT_RPS || "1"));
let TICK_MS = Math.max(
  Math.floor(1000 / Math.max(RATE_RPS, 0.01)),
  envInt("RATE_LIMIT_REFILL_MS")
);
const AFTER_HTTP_MS = envInt("AFTER_HTTP_MS");

const __httpQueue = [];
let __tickTimer = null;
function scheduleHttp(fn) {
  return new Promise((resolve, reject) => {
    __httpQueue.push({ fn, resolve, reject });
    if (!__tickTimer) __tickTimer = setInterval(drainHttpQueue, TICK_MS);
  });
}
async function drainHttpQueue() {
  if (circuitOpen()) return;
  const item = __httpQueue.shift();
  if (!item) { clearInterval(__tickTimer); __tickTimer = null; return; }
  const { fn, resolve, reject } = item;
  try {
    const res = await fn();
    on429Success();
    if (AFTER_HTTP_MS > 0) await sleep(AFTER_HTTP_MS);
    resolve(res);
  } catch (e) {
    reject(e);
  }
}

// Dynamic throttle: backs off on 429s, slowly increases after stability
let __stableOk = 0;
async function throttleOnResult(errMaybe) {
  if (errMaybe && String(errMaybe).toLowerCase().includes("429")) {
    on429();
    const floor = PLAN === "free" ? 1 : 3;
    RATE_RPS = Math.max(floor, Math.floor(RATE_RPS * 0.7));
    TICK_MS = Math.max(1000 / RATE_RPS, envInt("RATE_LIMIT_REFILL_MS"));
    log.warn({ RATE_RPS, TICK_MS }, "[429] throttle down");
    __stableOk = 0;
  } else {
    __stableOk++;
    if (__stableOk % 30 === 0) {
      const cap = parseFloat(process.env.RATE_LIMIT_RPS || (PLAN === "free" ? "1" : "8"));
      if (RATE_RPS < cap) {
        RATE_RPS = Math.min(cap, RATE_RPS + 1);
        TICK_MS = Math.max(1000 / RATE_RPS, envInt("RATE_LIMIT_REFILL_MS"));
        log.info({ RATE_RPS, TICK_MS }, "[ok] throttle up");
      }
    }
  }
}

// =========================
// HTTP Connection pool (round-robin on 429)
// =========================
function makeHttpConnection(url) {
  const commitment = process.env.RPC_COMMITMENT || process.env.COMMITMENT || "confirmed";
  return new SolConn(url, { commitment });
}
let __httpConns = HTTP_URLS.map(makeHttpConnection);
function getHttpConn() {
  if (__httpConns.length === 0) __httpConns = [makeHttpConnection(PRIMARY_HTTP)];
  return __httpConns[__httpIndex];
}
function rotateHttpConn() {
  if (HTTP_URLS.length <= 1) return getHttpConn();
  const url = nextHttpUrl();
  log.warn({ url }, "[429] rotating HTTP endpoint");
  __httpConns[__httpIndex] = makeHttpConnection(url);
  return __httpConns[__httpIndex];
}

// =========================
// Webhook (Helius/QuickNode Streams)
// =========================
function startWebhookAt(path, onTxLike) {
  const app = startWebhookAt.__app || express();
  startWebhookAt.__app = app;

  if (!startWebhookAt.__listening) {
    app.use(express.json({ limit: "4mb" }));
    const port = envInt("WEBHOOK_PORT", 8080);
    app.listen(port, () => console.log(`[webhook] listening on :${port}`));
    startWebhookAt.__listening = true;
  }
  const secret = (process.env.WEBHOOK_SHARED_SECRET || "").trim();
  const hdrName = process.env.WEBHOOK_SECRET_HEADER || "X-Shared-Secret";

  app.post(path, async (req, res) => {
    try {
      if (secret) {
        const headerToken = req.header(hdrName) || "";
        const queryToken = req.query.token || "";
        if (headerToken !== secret && String(queryToken) !== secret) {
          return res.status(401).json({ ok: false, error: "unauthorized" });
        }
      }
      const body = req.body;
      const txs = Array.isArray(body) ? body : (body?.transactions || body?.data || [body]);
      for (const tx of txs) {
        // Gate by target PIDs (avoid noise)
        if (!txLikeTouchesAnyPid(tx)) continue;
        const sig = tx?.transaction?.signatures?.[0] || tx?.signature;
        if (sig) enqueue(sig);
      }
      res.json({ ok: true });
    } catch (e) {
      console.error(`[webhook ${path}] error:`, e);
      res.status(500).json({ ok: false, error: String(e) });
    }
  });
}

// One canonical makeConnection (avoid duplicates)
function makeConnection() {
  const mode = String(process.env.PROVIDER_MODE || "solana_ws").toLowerCase();
  let ws = process.env.RPC_WS_URL || undefined;
  if (mode === "quicknode_ws" && process.env.QUICKNODE_WS_URL) ws = process.env.QUICKNODE_WS_URL;
  if (mode === "helius_ws" && process.env.HELIUS_ENHANCED_WS_URL) ws = process.env.HELIUS_ENHANCED_WS_URL;
  const http = PRIMARY_HTTP;
  const commitment = process.env.COMMITMENT || "confirmed";
  return new Connection(http, { commitment, wsEndpoint: ws });
}

// Multi-provider factory
function providersFactory() {
  const modes = String(process.env.PROVIDER_MODES || process.env.PROVIDER_MODE || "solana_ws")
    .split(",").map(s => s.trim().toLowerCase()).filter(Boolean);

  const list = [];
  let configError = false;

  for (const mode of modes) {
    if (mode === "solana_ws") {
      const wsUrl = process.env.RPC_WS_URL;
      if (!wsUrl) {
        log.error({ mode }, "FATAL: solana_ws requer RPC_WS_URL (endpoint WebSocket). Defina no .env.");
        configError = true;
        continue;
      }
      const conn = makeWsConnection(wsUrl, PRIMARY_HTTP);
      list.push({
        mode,
        label: providerLabel(mode),
        connection: conn,
        onProgramLogs: (pid, cb) => onProgramLogsWs(conn, pid, cb, mode),
        type: "ws"
      });
      continue;
    }

    if (mode === "helius_ws") {
      const wsUrl = process.env.HELIUS_ENHANCED_WS_URL;
      if (!wsUrl) {
        log.error({ mode }, "FATAL: helius_ws requer HELIUS_ENHANCED_WS_URL (endpoint Enhanced WS da Helius).");
        configError = true;
        continue;
      }
      const conn = makeWsConnection(wsUrl, PRIMARY_HTTP);
      list.push({
        mode,
        label: providerLabel(mode),
        connection: conn,
        onProgramLogs: (pid, cb) => onProgramLogsWs(conn, pid, cb, mode),
        type: "ws"
      });
      continue;
    }

    if (mode === "quicknode_ws") {
      const wsUrl = process.env.QUICKNODE_WS_URL;
      if (!wsUrl) {
        log.error({ mode }, "FATAL: quicknode_ws requer QUICKNODE_WS_URL (endpoint WS do QuickNode).");
        configError = true;
        continue;
      }
      const conn = makeWsConnection(wsUrl, PRIMARY_HTTP);
      list.push({
        mode,
        label: providerLabel(mode),
        connection: conn,
        onProgramLogs: (pid, cb) => onProgramLogsWs(conn, pid, cb, mode),
        type: "ws"
      });
      continue;
    }

    if (mode === "webhook_helius") {
      const path = process.env.WEBHOOK_HELIUS_PATH || "/streams/helius";
      startWebhookAt(path, () => {});
      const altPath = process.env.WEBHOOK_ALT_PATH || "/streams/webhook";
      if (altPath) startWebhookAt(altPath, () => {});
      const conn = makeConnection();
      list.push({
        mode,
        label: providerLabel(mode),
        connection: conn,
        onProgramLogs: async () => "webhook_helius",
        type: "webhook"
      });
      continue;
    }

    if (mode === "webhook_quicknode") {
      const path = process.env.WEBHOOK_QN_PATH || "/streams/qn";
      startWebhookAt(path, () => {});
      const conn = makeConnection();
      list.push({
        mode,
        label: providerLabel(mode),
        connection: conn,
        onProgramLogs: async () => "webhook_quicknode",
        type: "webhook"
      });
      continue;
    }

    log.warn({ mode }, "provider mode não reconhecido — ignorando");
  }

  // Se o usuário escolheu apenas modos WS mas esqueceu o endpoint, abortamos cedo:
  if (list.length === 0 && configError) {
    // Mantém processo vivo para evidenciar o erro no console (e evitar “silêncio”),
    // mas falha explicitamente.
    setInterval(() => {}, 60 * 1000);
    throw new Error("Configuração inválida: modo WS escolhido sem URL WS correspondente no .env");
  }

  // Fallback antigo (somente se NÃO houve erro de config e nada foi adicionado):
  if (list.length === 0) {
    const conn = makeConnection();
    list.push({
      mode: "solana_ws",
      label: "solana_ws",
      connection: conn,
      onProgramLogs: (pid, cb) => onProgramLogsWs(conn, pid, cb, "solana_ws"),
      type: "ws"
    });
    log.warn("Nenhum provider declarado — usando fallback solana_ws (verifique RPC_WS_URL para WS real).");
  }

  return list;
}

// =========================
// Balance helpers
// =========================
function computeTokenDeltas(meta) {
  const pre = meta?.preTokenBalances || [];
  const post = meta?.postTokenBalances || [];
  const index = new Map();

  for (const x of pre) {
    index.set(x.accountIndex, {
      mint: x.mint,
      owner: x.owner,
      pre: Number(x.uiTokenAmount?.uiAmountString ?? x.uiTokenAmount?.uiAmount ?? 0),
      post: 0
    });
  }
  for (const y of post) {
    const slot = index.get(y.accountIndex) || { mint: y.mint, owner: y.owner, pre: 0, post: 0 };
    slot.post = Number(y.uiTokenAmount?.uiAmountString ?? y.uiTokenAmount?.uiAmount ?? 0);
    slot.mint = y.mint;
    slot.owner = y.owner;
    index.set(y.accountIndex, slot);
  }

  const entries = [...index.values()];
  const byOwner = new Map();
  for (const e of entries) {
    const delta = e.post - e.pre;
    const ownerKey = e.owner;
    if (!byOwner.has(ownerKey)) byOwner.set(ownerKey, []);
    byOwner.get(ownerKey).push({ mint: e.mint, delta });
  }
  return { entries, byOwner };
}

function findBestOwnerForInflows(byOwner, userWallet) {
  // choose the owner with the most positive inflows (not the userWallet)
  let best = null;
  for (const [owner, list] of byOwner.entries()) {
    if (owner && userWallet && owner === userWallet) continue;
    const inflows = list.filter(x => x.delta > 0 && isValidMint(x.mint));
    if (inflows.length >= 1) {
      const score = inflows.reduce((s, x) => s + x.delta, 0);
      if (!best || inflows.length > best.inflows || (inflows.length === best.inflows && score > best.score)) {
        best = { owner, inflows: inflows.length, score };
      }
    }
  }
  return best?.owner || null;
}

function inferMintsAndAmountsFromMeta(meta, userWallet) {
  const { byOwner } = computeTokenDeltas(meta);
  const vaultOwner = findBestOwnerForInflows(byOwner, userWallet);

  // 1) First: inflows to the vaultOwner
  let inflows = (byOwner.get(vaultOwner) || [])
    .filter(x => x.delta > 0 && isValidMint(x.mint))
    .sort((a, b) => b.delta - a.delta);

  // 2) If none, consider all positive inflows (except user wallet)
  if (inflows.length === 0) {
    const allInflows = [];
    for (const [owner, list] of byOwner.entries()) {
      if (owner && userWallet && owner === userWallet) continue;
      for (const it of list) {
        if (it.delta > 0 && isValidMint(it.mint)) allInflows.push(it);
      }
    }
    inflows = allInflows.sort((a, b) => b.delta - a.delta);
  }

  let base_in = 0, quote_in = 0;
  let base_mint = null, quote_mint = null;

  if (inflows[0]) {
    base_mint = inflows[0].mint;
    base_in = inflows[0].delta;
  }
  if (inflows[1]) {
    quote_mint = inflows[1].mint;
    quote_in = inflows[1].delta;
  }

  // 3) If only one inflow found, try to find the second via userWallet outflows
  if (!quote_mint && userWallet) {
    const negs = (byOwner.get(userWallet) || [])
      .filter(x => x.delta < 0 && isValidMint(x.mint))
      .sort((a, b) => a.delta - b.delta);
    if (negs[0]) {
      quote_mint = negs[0].mint;
      quote_in = 0;
    }
  }

  return { vaultOwner, base_mint, quote_mint, base_in, quote_in };
}

// =========================
// Mint age helpers
// =========================
function isTodayFromBlockTime(blockTimeSec) {
  if (!blockTimeSec) return null;
  const now = new Date();
  const start = Math.floor(Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), now.getUTCDate()) / 1000);
  const end = start + 86400;
  return (blockTimeSec >= start && blockTimeSec < end);
}
async function mintedToday(connection, mint) {
  const onlyToday = String(process.env.ONLY_TODAY || "true").toLowerCase() === "true";
  if (!onlyToday) return true;
  const doLookup = String(process.env.ONLY_TODAY_LOOKUP || "true").toLowerCase() === "true";
  if (!doLookup) return true;

  const max = parseInt(process.env.MAX_FETCH_SIGNATURES || "250", 10);
  try {
    let before = undefined;
    let minTime = Number.MAX_SAFE_INTEGER;
    let fetched = 0;
    const httpConn = getHttpConn();
    while (fetched < max) {
      const list = await scheduleHttp(async () => {
        try {
          return await httpConn.getSignaturesForAddress(mint, {
            limit: Math.min(100, max - fetched),
            before,
            commitment: "confirmed"
          });
        } catch (e) {
          const msg = String(e?.message || e).toLowerCase();
          if (msg.includes("429") || msg.includes("too many requests")) {
            on429();
            rotateHttpConn();
          }
          await throttleOnResult(e);
          throw e;
        }
      });
      if (list.length === 0) break;
      for (const s of list) {
        if (s.blockTime) {
          minTime = Math.min(minTime, s.blockTime);
        }
      }
      fetched += list.length;
      before = list[list.length - 1].signature;
      if (minTime !== Number.MAX_SAFE_INTEGER) break;
    }
    const today = isTodayFromBlockTime(minTime);
    return today === null ? true : today;
  } catch (e) {
    console.warn("[mintedToday] error, fallback true:", e);
    return true;
  }
}

// =========================
// Parsers — Meteora DLMM
// =========================
const METEORA_DLMM = {
  label: "Meteora DLMM",
  programId: "LBUZKhRxPF3XUpBCjp4YzTKgLccjZhTSDM9YuVaPwxo",
  markers: [
    "InitializePosition",
    "InitializePositionAndAddLiquidity",
    "AddLiquidity",
    "AddLiquidityByStrategy",
    "AddLiquidityByStrategy2",
    "AddLiquidityByWeight",
    "IncreaseLiquidity"
  ]
};
function looksLikeMeteoraLiquidity(logs) {
  const L = logs || [];
  return L.some(l => METEORA_DLMM.markers.some(m => l.includes(m)));
}
function parseMeteoraLiquidity(tx) {
  const meta = tx?.meta;
  const logs = meta?.logMessages || [];
  if (!looksLikeMeteoraLiquidity(logs)) return null;

  const keys = tx?.transaction?.message?.accountKeys || [];
  const firstKey = getKeyPubkey(keys[0]);
  const userWallet = firstKey ? safePkStr(firstKey) : null;

  const { vaultOwner, base_mint, quote_mint, base_in, quote_in } = inferMintsAndAmountsFromMeta(meta, userWallet);

  const markersHit = METEORA_DLMM.markers.filter(m => logs.some(l => l.includes(m)));
  return {
    dex: METEORA_DLMM.label,
    program_id: METEORA_DLMM.programId,
    signature: tx?.transaction?.signatures?.[0] || tx?.signature || "unknown",
    slot: tx?.slot || 0,
    timestamp: tx?.blockTime || 0,
    user_wallet: userWallet,
    pool_authority: vaultOwner,
    mints: { base: base_mint, quote: quote_mint },
    amounts: { base_in, quote_in },
    single_sided: (quote_in === 0 || !quote_mint),
    markers: markersHit
  };
}

// =========================
// Parsers — Kamino→Meteora
// =========================
const KAMINO_YVAULTS = {
  name: "Kamino yVaults",
  programId: "6LtLpnUFNByNXLyCoK9wA2MykKAmQNZKBdY8s47dehDc",
  markers: ["Invest"]
};
const METEORA_DLMM_PID = METEORA_DLMM.programId;
function _hasAny(logs, needles) { return (logs || []).some(l => needles.some(n => l.includes(n))); }
function looksLikeKaminoInvest(tx) {
  const logs = tx?.meta?.logMessages || [];
  const keys = tx?.transaction?.message?.accountKeys || [];
  const hasKamino = keys.some(k => getKeyPubkey(k) === KAMINO_YVAULTS.programId);
  const mentionsInvest = _hasAny(logs, KAMINO_YVAULTS.markers);
  const touchesMeteora = keys.some(k => getKeyPubkey(k) === METEORA_DLMM_PID);

  return (hasKamino && mentionsInvest && touchesMeteora);
}
function parseKaminoInvest(tx) {
  if (!looksLikeKaminoInvest(tx)) return null;
  const meta = tx?.meta;
  const logs = meta?.logMessages || [];
  const keys = tx?.transaction?.message?.accountKeys || [];
  const firstKey = getKeyPubkey(keys[0]);
  const userWallet = firstKey ? safePkStr(firstKey) : null;

  const { vaultOwner, base_mint, quote_mint, base_in, quote_in } = inferMintsAndAmountsFromMeta(meta, userWallet);

  const markersHit = []
    .concat(KAMINO_YVAULTS.markers.filter(m => logs.some(l => l.includes(m))))
    .concat(logs.some(l => l.includes("AddLiquidity")) ? ["AddLiquidity*"] : []);
  return {
    dex: "Kamino→Meteora DLMM",
    program_id: KAMINO_YVAULTS.programId,
    routed_program: METEORA_DLMM_PID,
    signature: tx?.transaction?.signatures?.[0] || tx?.signature || "unknown",
    slot: tx?.slot || 0,
    timestamp: tx?.blockTime || 0,
    user_wallet: userWallet,
    pool_authority: vaultOwner,
    mints: { base: base_mint, quote: quote_mint },
    amounts: { base_in, quote_in },
    single_sided: (quote_in === 0 || !quote_mint),
    markers: markersHit
  };
}

// =========================
// Parsers — Orca Whirlpools
// =========================
const ORCA_WHIRLPOOLS = {
  name: "Orca Whirlpools",
  programId: "whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc",
  markers: ["initializePool", "initializeTickArray", "increaseLiquidity", "decreaseLiquidity", "openPosition"]
};
function looksLikeOrca(tx) {
  const logs = tx?.meta?.logMessages || [];
  const hasPid = tx?.transaction?.message?.accountKeys?.some(
    k => getKeyPubkey(k) === ORCA_WHIRLPOOLS.programId
  );
  const hasMarker = logs?.some(l => ORCA_WHIRLPOOLS.markers.some(m => l.includes(m)));
  return !!(hasPid && hasMarker);
}
function parseOrcaLiquidity(tx) {
  if (!looksLikeOrca(tx)) return null;
  const meta = tx?.meta;
  const logs = meta?.logMessages || [];
  const keys = tx?.transaction?.message?.accountKeys || [];
  const firstKey = getKeyPubkey(keys[0]);
  const userWallet = firstKey ? safePkStr(firstKey) : null;

  const { vaultOwner, base_mint, quote_mint, base_in, quote_in } = inferMintsAndAmountsFromMeta(meta, userWallet);

  const markersHit = ORCA_WHIRLPOOLS.markers.filter(m => logs.some(l => l.includes(m)));
  return {
    dex: "Orca Whirlpools",
    program_id: ORCA_WHIRLPOOLS.programId,
    signature: tx?.transaction?.signatures?.[0] || tx?.signature || "unknown",
    slot: tx?.slot || 0,
    timestamp: tx?.blockTime || 0,
    user_wallet: userWallet,
    pool_authority: vaultOwner,
    mints: { base: base_mint, quote: quote_mint },
    amounts: { base_in, quote_in },
    single_sided: (quote_in === 0 || !quote_mint),
    markers: markersHit
  };
}

// =========================
// Generic parser for other DEXes (from programs.json config)
// =========================
function parseGenericDex(tx, dexCfg) {
  const logs = tx?.meta?.logMessages || [];
  const keys = tx?.transaction?.message?.accountKeys || [];
  const programIds = dexCfg.programIds || [];
  const markers = (dexCfg.liquidityMarkers || []).map(s => s.toLowerCase());

  const hasProgram = keys?.some(k => programIds.includes(getKeyPubkey(k)));
  if (!hasProgram) return null;

  const requireMarker = envBool("GENERIC_REQUIRE_MARKER", true);
  const hasMarker = logs?.some(l => markers.some(m => l.toLowerCase().includes(m)));
  if (requireMarker && !hasMarker) return null;

  // Infer mints via token delta (same routine as others)
  const meta = tx?.meta;
  const firstKey = getKeyPubkey(keys[0]);
  const userWallet = firstKey ? safePkStr(firstKey) : null;
  const { vaultOwner, base_mint, quote_mint, base_in, quote_in } = inferMintsAndAmountsFromMeta(meta, userWallet);

  return {
    dex: dexCfg.name || "Unknown DEX",
    program_id: programIds[0] || "unknown",
    signature: tx?.transaction?.signatures?.[0] || tx?.signature || "unknown",
    slot: tx?.slot || 0,
    timestamp: tx?.blockTime || 0,
    user_wallet: userWallet,
    pool_authority: vaultOwner,
    mints: { base: base_mint, quote: quote_mint },
    amounts: { base_in, quote_in },
    single_sided: (quote_in === 0 || !quote_mint),
    markers: markers.filter(m => logs?.some(l => l.toLowerCase().includes(m)))
  };
}

// =========================
// programs.json loader (+ includeFiles merger) + programLogs list
// =========================
let EXTRA_DEX = [];
let PROGRAM_LOGS_LIST = [];
function loadDexPrograms() {
  try {
    const raw = fs.readFileSync("programs.json", "utf8");
    const cfg = JSON.parse(raw);
    EXTRA_DEX = cfg.dexes || [];

    // programLogs (list of PIDs for priority use)
    PROGRAM_LOGS_LIST = (cfg.programLogs || cfg.logs || []).map(String).filter(Boolean);

    const includes = (cfg.includeFiles || []).map(s => s.trim()).filter(Boolean);
    for (const inc of includes) {
      try {
        const incPath = path.resolve(inc);
        const text = fs.readFileSync(incPath, "utf8");
        const data = JSON.parse(text);

        const ids = new Set();
        const pushAddr = a => { if (a && typeof a === "string") ids.add(a); };

        if (Array.isArray(data)) {
          for (const it of data) {
            if (typeof it === "string") pushAddr(it);
            else if (it?.address) pushAddr(it.address);
            else if (it?.programId) pushAddr(it.programId);
          }
        } else if (typeof data === "object") {
          if (data.address) pushAddr(data.address);
          if (data.programId) pushAddr(data.programId);
          if (Array.isArray(data.addresses)) data.addresses.forEach(pushAddr);
          if (Array.isArray(data.programIds)) data.programIds.forEach(pushAddr);
        }

        if (ids.size > 0) {
          const baseName = path.basename(inc).replace(/\.json$/i, "");
          EXTRA_DEX.push({
            name: baseName,
            programIds: [...ids],
            liquidityMarkers: cfg.defaultMarkers || [
              "addliquidity", "initialize", "openposition",
              "increaseliquidity", "deposit", "create", "swap"
            ]
          });
        }
      } catch (e) {
        log.warn({ file: inc, err: String(e) }, "include parse failed (skipped)");
      }
    }
    return { meteora: cfg.meteora || METEORA_DLMM, extra: EXTRA_DEX };
  } catch {
    // fallback: only Meteora
    PROGRAM_LOGS_LIST = [];
    return { meteora: METEORA_DLMM, extra: [] };
  }
}

// =========================
// Router – classify transaction into a known DEX event
// =========================
function classifyLiquidity(tx) {
  // 1) Kamino→Meteora
  const k = parseKaminoInvest(tx);
  if (k) return k;
  // 2) Meteora
  const m = parseMeteoraLiquidity(tx);
  if (m) return m;
  // 3) Orca
  const o = parseOrcaLiquidity(tx);
  if (o) return o;
  // 4) Generic (e.g. Raydium, Pump, Jupiter as per programs.json)
  for (const dex of EXTRA_DEX) {
    const g = parseGenericDex(tx, dex);
    if (g) return g;
  }
  return null;
}

// =========================
// Dedup + queue (signatures) — keep ≥ 1 evt/sec flow
// =========================
const CONCURRENCY = envInt("CONCURRENCY");
const __pending = new Set();
const __queue = [];
let __active = 0;
const RESERVOIR_MAX = envInt("RESERVOIR_MAX", 3000);

function enqueue(signature) {
  if (!signature || __pending.has(signature)) return;
  __pending.add(signature);
  __queue.push(signature);
  if (__queue.length > RESERVOIR_MAX) {
    __queue.splice(0, __queue.length - RESERVOIR_MAX);
  }
  pump();
}
async function worker(connection) {
  while (true) {
    const sig = __queue.shift();
    if (!sig) return;
    try {
      await fetchAndProcessTx(connection, sig);
    } catch (e) {
      log.error({ sig, err: String(e) }, "worker error");
    } finally {
      __pending.delete(sig);
    }
  }
}
function pump(connRef) {
  const connection = connRef || global.__CONNECTION__;
  while (__active < CONCURRENCY && __queue.length > 0) {
    __active++;
    worker(connection).finally(() => {
      __active--;
      setImmediate(() => pump(connection));
    });
  }
}

// micro-jitter to smooth bursts
async function handleSignature(connection, signature) {
  if (!signature) return;
  if (__pending.has(signature)) return;
  const maxJitter = PLAN === "free" ? 90 : 40; // ms
  await sleep(20 + Math.floor(Math.random() * maxJitter));
  enqueue(signature);
}

// =========================
// Fetch & Process transaction (scheduled + handles 429 logic)
// =========================
function bothMintsValid(ev) {
  const bOk = isValidMint(ev?.mints?.base);
  const qOk = isValidMint(ev?.mints?.quote);
  // Accept if at least one valid mint (single-sided LP). To require both, use (bOk && qOk).
  return (bOk || qOk);
}

async function fetchAndProcessTx(_connForWs, signature) {
  try {
    const httpConn = getHttpConn();
    const tx = await scheduleHttp(async () => {
      try {
        return await httpConn.getTransaction(signature, {
          maxSupportedTransactionVersion: 0,
          commitment: "confirmed"
        });
      } catch (e) {
        const msg = String(e?.message || e).toLowerCase();
        if (msg.includes("429") || msg.includes("too many requests")) {
          on429();
          rotateHttpConn();
        }
        await throttleOnResult(e);
        throw e;
      }
    });
    if (!tx) return;

    const bt = tx.blockTime || tx.meta?.blockTime || 0;
    if (String(process.env.ONLY_TODAY || "true").toLowerCase() === "true") {
      const today = isTodayFromBlockTime(bt);
      if (today === false) return;  // Skip events not from today
    }

    if (!tx.slot) {
      // Ensure slot is populated (fetch if missing)
      const slot = await scheduleHttp(async () => {
        try {
          return await httpConn.getSlot({ commitment: "confirmed" });
        } catch (e) {
          const msg = String(e?.message || e).toLowerCase();
          if (msg.includes("429") || msg.includes("too many requests")) {
            on429();
            rotateHttpConn();
          }
          await throttleOnResult(e);
          throw e;
        }
      });
      tx.slot = slot;
    }
    tx.blockTime = bt;

    const event = classifyLiquidity(tx);
    if (!event) return;

    // Fix signature if missing
    if (!event.signature || event.signature === "unknown") {
      event.signature = signature;
    }

    // =============== VALIDATE MINTS (ensure no program IDs as mints) ===============
    if (!bothMintsValid(event)) {
      log.debug({
        sig: event.signature,
        base_mint: event?.mints?.base || null,
        quote_mint: event?.mints?.quote || null
      }, "skip: could not infer valid token mint(s)");
      return;
    }
    // ==============================================================================

    // =================== MULTI-FACTOR LIQUIDITY CHECKS ===================
    if (MF_ENABLED) {
      // Require two vaults (base and quote deposits)
      if (MF_REQUIRE_TWO_VAULTS && event.single_sided === true) {
        log.debug({ sig: event.signature }, "skip: liquidity event is single-sided");
        return;
      }
      // Identify LP token mint from user inflows if needed
      let lpMint = null;
      if (MF_REQUIRE_LP_MINT || MF_REQUIRE_FIRST_HISTORY || MF_CHECK_PROVIDER_IS_MINT_AUTH) {
        const { byOwner } = computeTokenDeltas(tx.meta);
        const userList = byOwner.get(event.user_wallet) || [];
        for (const rec of userList) {
          if (rec.delta > 0 && rec.mint !== event.mints.base && rec.mint !== event.mints.quote && isValidMint(rec.mint)) {
            lpMint = rec.mint;
            break;
          }
        }
      }
      // Require LP token minted to provider
      if (MF_REQUIRE_LP_MINT && !lpMint) {
        log.debug({ sig: event.signature }, "skip: no LP token minted to provider");
        return;
      }
      // Require first-time account usage (no prior transactions for pool and LP mint)
      if (MF_REQUIRE_FIRST_HISTORY) {
        if (event.pool_authority) {
          try {
            const history = await scheduleHttp(async () => {
              try {
                return await httpConn.getSignaturesForAddress(new PublicKey(event.pool_authority), { limit: 1, before: event.signature, commitment: "confirmed" });
              } catch (e) {
                const msg = String(e?.message || e).toLowerCase();
                if (msg.includes("429") || msg.includes("too many requests")) {
                  on429();
                  rotateHttpConn();
                }
                await throttleOnResult(e);
                throw e;
              }
            });
            if (history.length > 0) {
              log.debug({ sig: event.signature, pool: event.pool_authority }, "skip: pool account has prior transactions");
              return;
            }
          } catch (e) {
            console.warn("[multi-factor] pool history check failed, continuing:", e);
          }
        }
        if (lpMint) {
          try {
            const history = await scheduleHttp(async () => {
              try {
                return await httpConn.getSignaturesForAddress(new PublicKey(lpMint), { limit: 1, before: event.signature, commitment: "confirmed" });
              } catch (e) {
                const msg = String(e?.message || e).toLowerCase();
                if (msg.includes("429") || msg.includes("too many requests")) {
                  on429();
                  rotateHttpConn();
                }
                await throttleOnResult(e);
                throw e;
              }
            });
            if (history.length > 0) {
              log.debug({ sig: event.signature, lp_mint: lpMint }, "skip: LP token mint has prior transactions");
              return;
            }
          } catch (e) {
            console.warn("[multi-factor] LP mint history check failed, continuing:", e);
          }
        }
      }
      // Sanity check: deposit ratio limits
      if (MF_MAX_RATIO > 0) {
        const bAmt = event.amounts?.base_in ?? 0;
        const qAmt = event.amounts?.quote_in ?? 0;
        let ratio = Infinity;
        if (bAmt > 0 && qAmt > 0) {
          ratio = bAmt > qAmt ? (bAmt / qAmt) : (qAmt / bAmt);
        }
        if (ratio > MF_MAX_RATIO) {
          log.debug({ sig: event.signature, ratio }, "skip: deposit ratio exceeds limit");
          return;
        }
      }
      // Optional: provider is mint authority check
      if (MF_CHECK_PROVIDER_IS_MINT_AUTH && lpMint) {
        try {
          const mintInfo = await scheduleHttp(async () => {
            try {
              return await httpConn.getParsedAccountInfo(new PublicKey(lpMint));
            } catch (e) {
              const msg = String(e?.message || e).toLowerCase();
              if (msg.includes("429") || msg.includes("too many requests")) {
                on429();
                rotateHttpConn();
              }
              await throttleOnResult(e);
              throw e;
            }
          });
          const mintParsed = mintInfo?.value?.data?.parsed?.info;
          const mintAuth = mintParsed?.mintAuthority;
          if (!mintAuth || (typeof mintAuth === "string" ? mintAuth : String(mintAuth)) !== event.user_wallet) {
            log.debug({ sig: event.signature, provider: event.user_wallet, mint_auth: mintAuth || null }, "skip: provider is not LP mint authority");
            return;
          }
        } catch (e) {
          console.warn("[multi-factor] mint authority check failed, continuing:", e);
        }
      }
    }
    // ==========================================================

    // =================== DEDUPLICATION GATE ===================
    if (dedup.shouldSkip(event)) {
      log.debug({ sig: event.signature, scope: DEDUP_SCOPE }, "dedup: skipped duplicate");
      return;
    }
    dedup.markSent(event);
    // ==========================================================

    const maxAge = envInt("MAX_AGE_SECONDS", 0);
    if (maxAge > 0 && event.timestamp > 0) {
      const age = Math.floor(Date.now() / 1000) - event.timestamp;
      if (age > maxAge) return;  // too old, skip
    }

    // Only proceed if token was minted today (base or quote token)
    const onlyToday = String(process.env.ONLY_TODAY || "true").toLowerCase() === "true";
    if (onlyToday) {
      const doLookup = String(process.env.ONLY_TODAY_LOOKUP || "true").toLowerCase() === "true";
      if (doLookup) {
        const baseMintAddr = event.mints?.base;
        const quoteMintAddr = event.mints?.quote;
        let baseNew = true, quoteNew = true;
        if (baseMintAddr) {
          baseNew = await mintedToday(httpConn, new PublicKey(baseMintAddr));
        }
        if (quoteMintAddr) {
          quoteNew = await mintedToday(httpConn, new PublicKey(quoteMintAddr));
        }
        if (!baseNew && !quoteNew) {
          // Neither token was created today – ignore this event
          return;
        }
      }
    }

    // Log event to SQLite (persistent storage)
    try {
      logEvent(event);
    } catch (e) {
      log.error({ err: String(e) }, "DB logging failed");
    }

    // Send Telegram alert
    await notify(event);
    log.info({ sig: event.signature, dex: event.dex }, "alert sent");
  } catch (e) {
    log.error({ err: String(e), sig: signature }, "processTx error");
  }
}
