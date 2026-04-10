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
const LIVE_URL = 'https://redalert.orielhaim.com/api/active';
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
  const trimmed = text.trim();
  if (!trimmed || trimmed.startsWith('<')) return [];
  const data = JSON.parse(trimmed);
  if (!Array.isArray(data)) return [];

  return new Promise((resolve, reject) => {
    const inserted = [];
    db.serialize(() => {
      const stmt = db.prepare(
        'INSERT OR IGNORE INTO alerts (rid,data,alert_date,category,category_desc,matrix_id) VALUES (?,?,?,?,?,?)'
      );
      for (const r of data) {
        stmt.run(
          r.rid, r.data || '', r.alertDate || '',
          r.category, r.category_desc || '', r.matrix_id,
          function(err) {
            if (!err && this.changes > 0) {
              inserted.push({
                rid          : r.rid,
                alertDate    : r.alertDate || '',
                locations    : r.data ? [r.data] : [],
                category     : r.category,
                category_desc: r.category_desc || '',
                count        : r.data ? 1 : 0,
              });
            }
          }
        );
      }
      stmt.finalize(err => err ? reject(err) : resolve(inserted));
    });
  });
}

function startFetchLoop() {
  const loop = async () => {
    try {
      const inserted = await fetchAndStore();
      if (inserted.length) {
        console.log(`[fetch] +${inserted.length} new alerts`);
        broadcastNewAlerts(inserted);
      }
    } catch (e) { console.error('[fetch] error:', e.message); }
    setTimeout(loop, 60000);
  };
  loop();
}

// ── Live feed (real-time) ─────────────────────────────────────────────────────

// Generate a deterministic rid from alertType + city + minute (avoids duplicates within same minute)
function simpleHash(str) {
  let h = 0;
  for (let i = 0; i < str.length; i++) h = Math.imul(31, h) + str.charCodeAt(i) | 0;
  return Math.abs(h);
}

async function fetchLiveAndStore() {
  const resp = await fetch(LIVE_URL);
  const text = await resp.text();
  const trimmed = text.trim();
  if (!trimmed || trimmed === '{}' || trimmed.startsWith('<')) return [];
  const data = JSON.parse(trimmed);
  if (typeof data !== 'object' || !Object.keys(data).length) return [];

  const now    = new Date().toISOString().replace('T', ' ').slice(0, 19);
  const minute = now.slice(0, 16); // YYYY-MM-DD HH:MM — for dedup key

  return new Promise((resolve, reject) => {
    const inserted = [];
    db.serialize(() => {
      const stmt = db.prepare(
        'INSERT OR IGNORE INTO alerts (rid,data,alert_date,category,category_desc,matrix_id) VALUES (?,?,?,?,?,?)'
      );
      for (const [alertType, cities] of Object.entries(data)) {
        for (const city of cities) {
          const rid = simpleHash(`${alertType}:${city}:${minute}`);
          stmt.run(rid, city, now, null, alertType, null, function(err) {
            if (!err && this.changes > 0) {
              inserted.push({
                rid,
                alertDate    : now,
                locations    : [city],
                category     : null,
                category_desc: alertType,
                count        : 1,
              });
            }
          });
        }
      }
      stmt.finalize(err => err ? reject(err) : resolve(inserted));
    });
  });
}

function startLiveLoop() {
  const loop = async () => {
    try {
      const inserted = await fetchLiveAndStore();
      if (inserted.length) {
        console.log(`[live] +${inserted.length} new alerts`);
        broadcastNewAlerts(inserted);
      }
    } catch (e) { console.error('[live] error:', e.message); }
    setTimeout(loop, 5000);
  };
  loop();
}

// ── Map-data cache ────────────────────────────────────────────────────────────

const mapDataCacheByHours = {}; // { [hours]: { data, time } }
const MAP_CACHE_TTL = 60 * 1000; // 60 seconds

const MAP_EXCLUDED_CATS = new Set([
  'סיום אירוע',
  'האירוע הסתיים',
  'בדקות הקרובות צפויות להתקבל התרעות באזורך',
]);

// ── Query helpers ─────────────────────────────────────────────────────────────

function rowToEvent(row) {
  const locations = (row.locs || '').split('||').map(l => l.trim()).filter(Boolean);
  return {
    alertDate    : row.alert_date,
    category     : row.category,
    category_desc: row.category_desc || '',
    locations,
    count        : locations.length,
  };
}

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
  return rows.map(rowToEvent);
}

// ── Server-side paginated events ──────────────────────────────────────────────

async function queryEventsPaged(page, pageSize, fromIso, toIso, search) {
  const conds  = [];
  const params = [];
  if (fromIso) { conds.push('alert_date >= ?'); params.push(fromIso); }
  if (toIso)   { conds.push('alert_date <= ?'); params.push(toIso); }

  let total, totalAlerts, totalLocations, rows;

  if (search) {
    const sConds  = [...conds, 'data LIKE ?'];
    const sParams = [...params, `%${search}%`];
    const sWhere  = ' WHERE ' + sConds.join(' AND ');

    ({ total }          = await dbGet(`SELECT COUNT(DISTINCT alert_date) AS total FROM alerts${sWhere}`, sParams));
    ({ totalAlerts }    = await dbGet(`SELECT COUNT(*) AS totalAlerts FROM alerts${sWhere}`, sParams));
    ({ totalLocations } = await dbGet(`SELECT COUNT(DISTINCT data) AS totalLocations FROM alerts${sWhere}`, sParams));

    const totalPages = Math.max(1, Math.ceil(total / pageSize));
    const safePage   = Math.min(Math.max(1, page), totalPages);
    const offset     = (safePage - 1) * pageSize;

    rows = await dbAll(
      `SELECT alert_date, group_concat(data,'||') AS locs, category, category_desc
       FROM alerts
       WHERE alert_date IN (SELECT DISTINCT alert_date FROM alerts${sWhere})
       GROUP BY alert_date ORDER BY alert_date DESC LIMIT ? OFFSET ?`,
      [...sParams, pageSize, offset]
    );
    return { items: rows.map(rowToEvent), total, totalAlerts, totalLocations, page: safePage, pageSize, totalPages };

  } else {
    const where = conds.length ? ' WHERE ' + conds.join(' AND ') : '';

    ({ total }          = await dbGet(
      `SELECT COUNT(*) AS total FROM (SELECT 1 FROM alerts${where} GROUP BY alert_date)`, params));
    ({ totalAlerts }    = await dbGet(`SELECT COUNT(*) AS totalAlerts FROM alerts${where}`, params));
    ({ totalLocations } = await dbGet(`SELECT COUNT(DISTINCT data) AS totalLocations FROM alerts${where}`, params));

    const totalPages = Math.max(1, Math.ceil(total / pageSize));
    const safePage   = Math.min(Math.max(1, page), totalPages);
    const offset     = (safePage - 1) * pageSize;

    rows = await dbAll(
      `SELECT alert_date, group_concat(data,'||') AS locs, category, category_desc
       FROM alerts${where}
       GROUP BY alert_date ORDER BY alert_date DESC LIMIT ? OFFSET ?`,
      [...params, pageSize, offset]
    );
    return { items: rows.map(rowToEvent), total, totalAlerts, totalLocations, page: safePage, pageSize, totalPages };
  }
}

// ── Static files ──────────────────────────────────────────────────────────────

app.use(express.static(path.join(BASE_DIR, 'public')));

// ── No-cache middleware ───────────────────────────────────────────────────────

app.use((req, res, next) => {
  res.set('Cache-Control', 'no-store, no-cache, must-revalidate, proxy-revalidate');
  res.set('Pragma', 'no-cache');
  res.set('Expires', '0');
  next();
});

// ── SSE ───────────────────────────────────────────────────────────────────────

const clients = new Set();

app.get('/api/stream', (req, res) => {
  res.setHeader('Content-Type', 'text/event-stream');
  res.setHeader('Cache-Control', 'no-cache, no-store, must-revalidate');
  res.setHeader('Connection', 'keep-alive');
  res.write('retry: 2000\n\n');
  clients.add(res);
  req.on('close', () => { clients.delete(res); });
});

function broadcastNewAlerts(alerts) {
  if (!alerts.length) return;
  Object.keys(mapDataCacheByHours).forEach(k => delete mapDataCacheByHours[k]); // invalidate all map caches
  const payload = `data: ${JSON.stringify(alerts)}\n\n`;
  for (const client of clients) client.write(payload);
}

// ── API routes ────────────────────────────────────────────────────────────────

app.get('/api/events', async (req, res) => {
  try {
    const page     = Math.max(1, parseInt(req.query.page)     || 1);
    const pageSize = Math.min(Math.max(1, parseInt(req.query.pageSize) || 100), 500);
    const from     = req.query.from   || null;
    const to       = req.query.to     || null;
    const search   = req.query.search ? req.query.search.trim() : null;
    res.json(await queryEventsPaged(page, pageSize, from, to, search));
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

app.get('/api/map-data', async (req, res) => {
  try {
    const hours = Math.min(parseInt(req.query.hours) || 24, 168);

    // Serve from per-hours cache if fresh
    const cached = mapDataCacheByHours[hours];
    if (cached && (Date.now() - cached.time) < MAP_CACHE_TTL) {
      return res.json(cached.data);
    }

    const since = new Date(Date.now() - hours * 3600 * 1000)
      .toISOString().replace('T', ' ').slice(0, 19);

    const rows = await dbAll(
      `SELECT data, alert_date, category_desc
       FROM alerts
       WHERE alert_date >= ?
         AND category_desc NOT IN (?, ?, ?)
       ORDER BY alert_date DESC`,
      [since, ...MAP_EXCLUDED_CATS]
    );

    // Aggregate by location (keep up to 8 latest events each)
    const byLoc = {};
    for (const row of rows) {
      const name = (row.data || '').trim();
      if (!name) continue;
      if (!byLoc[name]) byLoc[name] = { name, count: 0, events: [] };
      byLoc[name].count++;
      if (byLoc[name].events.length < 8)
        byLoc[name].events.push({ alertDate: row.alert_date, category_desc: row.category_desc || '' });
    }

    // Geocode any missing locations (up to 50 new ones per request)
    const names = Object.keys(byLoc);
    await geocodeBatch(names, 50);

    // Build response — only locations that have coordinates
    const locations = names
      .filter(n => geocache[n])
      .map(n => ({
        name   : n,
        lat    : geocache[n][0],
        lng    : geocache[n][1],
        count  : byLoc[n].count,
        events : byLoc[n].events,
      }));

    const result = { generatedAt: new Date().toISOString(), hours, locations };
    mapDataCacheByHours[hours] = { data: result, time: Date.now() };
    res.json(result);
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
  .then(items => console.log(`[init] fetched ${items.length} new alerts`))
  .catch(e => console.error('[init] fetch failed:', e.message));
startFetchLoop();
startLiveLoop();

app.listen(PORT, () => console.log(`Server running on port ${PORT}`));
