import Database from "better-sqlite3";
import fs from "fs";
import path from "path";

// Determine database file path (default to "liquidity.db" in current directory)
const DB_PATH = process.env.DB_FILE
  ? path.resolve(process.cwd(), process.env.DB_FILE)
  : path.resolve(process.cwd(), "liquidity.db");

// Ensure the database directory exists
const dbDir = path.dirname(DB_PATH);
if (dbDir && !fs.existsSync(dbDir)) {
  fs.mkdirSync(dbDir, { recursive: true });
}

// Open SQLite database in WAL mode for high performance writes
const db = new Database(DB_PATH);
db.pragma("journal_mode = WAL");

// Create events table if it doesn't exist
db.prepare(`CREATE TABLE IF NOT EXISTS events (
  signature     TEXT PRIMARY KEY,
  timestamp     INTEGER,
  dex           TEXT,
  base_mint     TEXT,
  quote_mint    TEXT,
  base_amount   REAL,
  quote_amount  REAL,
  user_wallet   TEXT,
  pool_authority TEXT
)`).run();
db.prepare(`CREATE INDEX IF NOT EXISTS idx_events_base ON events(base_mint)`).run();
db.prepare(`CREATE INDEX IF NOT EXISTS idx_events_quote ON events(quote_mint)`).run();

/**
 * Logs a liquidity event to the SQLite database.
 * This will insert a new row for the event if not already present.
 */
export function logEvent(event) {
  const stmt = db.prepare(`
    INSERT OR IGNORE INTO events (
      signature,
      timestamp,
      dex,
      base_mint,
      quote_mint,
      base_amount,
      quote_amount,
      user_wallet,
      pool_authority
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
  `);
  stmt.run(
    event.signature,
    event.timestamp || Math.floor(Date.now() / 1000),
    event.dex,
    event.mints?.base || null,
    event.mints?.quote || null,
    event.amounts?.base_in ?? 0,
    event.amounts?.quote_in ?? 0,
    event.user_wallet || null,
    event.pool_authority || null
  );
}
