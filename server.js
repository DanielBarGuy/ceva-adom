const express    = require('express');
const fetch      = require('node-fetch');
const path       = require('path');
const sqlite3    = require('sqlite3').verbose();
const fs         = require('fs');

const app      = express();
const PORT     = process.env.PORT || 3000;
const BASE_DIR = __dirname;
const DB_FILE  = path.join(BASE_DIR, 'alerts.db');
const CACHE_FILE = path.join(BASE_DIR, 'geocache.json');

const OREF_URL = 'https://alerts-history.oref.org.il/Shared/Ajax/GetAlarmsHistory.aspx?lang=he&mode=1';
const OREF_HEADERS = {
  'Referer'         : 'https://www.oref.org.il/',
  'X-Requested-With': 'XMLHttpRequest',
  'Accept'          : 'application/json, text/javascript, */*; q=0.01',
  'Accept-Language' : 'he',
  'User-Agent'      : 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
};

// ── SQLite ────────────────────────────────────────────────────────────────────

const db = new sqlite3.Database(DB_FILE, err => {
  if (err) console.error('[db] open error:', err.message);
});

db.serialize(() => {
  db.run(`CREATE TABLE IF NOT EXISTS alerts (
    rid           INTEGER PRIMARY KEY,
    data          TEXT    NOT NULL,
    alert_date    TEXT    NOT NULL,
    category      INTEGER,
    category_desc TEXT,
    matrix_id     INTEGER
  )`);
  db.run('CREATE INDEX IF NOT EXISTS idx_date ON alerts(alert_date)');
});

function dbAll(sql, params = []) {
  return new Promise((resolve, reject) =>
    db.all(sql, params, (err, rows) => err ? reject(err) : resolve(rows))
  );
}
function dbGet(sql, params = []) {
  return new Promise((resolve, reject) =>
    db.get(sql, params, (err, row) => err ? reject(err) : resolve(row))
  );
}

// ── Geocache ──────────────────────────────────────────────────────────────────

let geocache = {};

function loadGeoCache() {
  try {
    if (fs.existsSync(CACHE_FILE))
      geocache = JSON.parse(fs.readFileSync(CACHE_FILE, 'utf-8'));
    console.log(`[geo] loaded ${Object.keys(geocache).length} entries`);
  } catch (e) { console.error('[geo] load error:', e.message); }
}

function saveGeoCache() {
  try { fs.writeFileSync(CACHE_FILE, JSON.stringify(geocache), 'utf-8'); }
  catch (e) { console.error('[geo] save error:', e.message); }
}

let lastNominatim = 0;

async function geocodeLocation(name) {
  if (name in geocache) return geocache[name];
  const wait = 1100 - (Date.now() - lastNominatim);
  if (wait > 0) await new Promise(r => setTimeout(r, wait));
  lastNominatim = Date.now();
  try {
    const url = `https://nominatim.openstreetmap.org/search?q=${encodeURIComponent(name + ' ישראל')}&countrycodes=il&format=json&limit=1&accept-language=he`;
    const resp = await fetch(url, { headers: { 'User-Agent': 'CevaAdom/1.0' } });
    const results = await resp.json();
    geocache[name] = results.length
      ? [parseFloat(results[0].lat), parseFloat(results[0].lon)]
      : null;
  } catch { geocache[name] = null; }
  saveGeoCache();
  return geocache[name];
}

async function geocodeBatch(names, maxNew = 100) {
  const uncached = names.filter(n => !(n in geocache)).slice(0, maxNew);
  for (const loc of uncached) await geocodeLocation(loc);
  const result = {};
  for (const n of names) if (geocache[n]) result[n] = geocache[n];
  const pending = names.filter(n => !(n in geocache)).length;
  if (pending) result._pending = pending;
  return result;
}

// ── Fetch from OREF ───────────────────────────────────────────────────────────

async function fetchAndStore() {
  const resp = await fetch(OREF_URL, { headers: OREF_HEADERS });
  const text = await resp.text();
  const data = text.trim() ? JSON.parse(text) : [];
  if (!Array.isArray(data)) return 0;

  return new Promise((resolve, reject) => {
    let added = 0;
    db.serialize(() => {
      const stmt = db.prepare(
        'INSERT OR IGNORE INTO alerts (rid,data,alert_date,category,category_desc,matrix_id) VALUES (?,?,?,?,?,?)'
      );
      for (const r of data) {
        stmt.run(
          r.rid, r.data || '', r.alertDate || '',
          r.category, r.category_desc || '', r.matrix_id,
          function(err) { if (!err && this.changes > 0) added++; }
        );
      }
      stmt.finalize(err => err ? reject(err) : resolve(added));
    });
  });
}

function startFetchLoop() {
  const loop = async () => {
    try {
      const added = await fetchAndStore();
      if (added) console.log(`[fetch] +${added} new alerts`);
    } catch (e) { console.error('[fetch] error:', e.message); }
    setTimeout(loop, 30000);
  };
  loop();
}

// ── Query helpers ─────────────────────────────────────────────────────────────

async function queryEvents(fromIso, toIso, limit = 2000) {
  const where = [], params = [];
  if (fromIso) { where.push('alert_date >= ?'); params.push(fromIso); }
  if (toIso)   { where.push('alert_date <= ?'); params.push(toIso); }
  let sql = `SELECT alert_date, group_concat(data,'||') AS locs, category, category_desc
             FROM alerts`;
  if (where.length) sql += ' WHERE ' + where.join(' AND ');
  sql += ' GROUP BY alert_date ORDER BY alert_date DESC LIMIT ?';
  params.push(limit);

  const rows = await dbAll(sql, params);
  return rows.map(row => {
    const locations = (row.locs || '').split('||').map(l => l.trim()).filter(Boolean);
    return {
      alertDate    : row.alert_date,
      category     : row.category,
      category_desc: row.category_desc || '',
      locations,
      count        : locations.length,
    };
  });
}

// ── Static files ──────────────────────────────────────────────────────────────

app.use(express.static(path.join(BASE_DIR, 'public')));

// ── API routes ────────────────────────────────────────────────────────────────

app.get('/api/events', async (req, res) => {
  try {
    res.json(await queryEvents(req.query.from, req.query.to, parseInt(req.query.limit) || 2000));
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.get('/api/locations', async (req, res) => {
  try {
    const events = await queryEvents(req.query.from, req.query.to, 2000);
    const freq = {};
    for (const ev of events)
      for (const loc of ev.locations)
        freq[loc] = (freq[loc] || 0) + 1;
    const ranked = Object.keys(freq).sort((a, b) => freq[b] - freq[a]);
    res.json(await geocodeBatch(ranked, 100));
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.get('/api/geocode', async (req, res) => {
  try {
    const names = (req.query.names || '').split(',').map(n => n.trim()).filter(Boolean);
    if (!names.length) return res.status(400).json({ error: 'missing names parameter' });
    res.json(await geocodeBatch(names, 100));
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.get('/api/stats', async (req, res) => {
  try {
    const { total } = await dbGet('SELECT COUNT(*) AS total FROM alerts');
    const { oldest } = await dbGet('SELECT MIN(alert_date) AS oldest FROM alerts');
    const { newest } = await dbGet('SELECT MAX(alert_date) AS newest FROM alerts');
    res.json({ total, oldest, newest });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// ── Start ─────────────────────────────────────────────────────────────────────

loadGeoCache();
fetchAndStore()
  .then(n => console.log(`[init] fetched ${n} new alerts`))
  .catch(e => console.error('[init] fetch failed:', e.message));
startFetchLoop();

app.listen(PORT, () => console.log(`Server running on port ${PORT}`));
