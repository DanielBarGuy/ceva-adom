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

const OREF_URL       = 'https://alerts-history.oref.org.il/Shared/Ajax/GetAlarmsHistory.aspx?lang=he&mode=1';
const LIVE_URL       = 'https://api.siren.co.il/active';
const CITIES_URL     = 'https://api.siren.co.il/data/cities';
const NOM_URL        = 'https://nominatim.openstreetmap.org/search';
const ISRAEL_OFFSET_MS = 3 * 60 * 60 * 1000; // UTC+3
const SIREN_API_KEY  = 'pr_LrWyqtByaKuVHoEzBRjDnpxvDThsIrJIAVzcwJFJDpvETptpDNinmStfglznvmbR';
const SIREN_HEADERS  = {
  'Authorization': `Bearer ${SIREN_API_KEY}`,
  'Accept'       : 'application/json',
  'User-Agent'   : 'CevaAdom/2.0',
};

const OREF_HEADERS = {
  'Referer'         : 'https://www.oref.org.il/',
  'X-Requested-With': 'XMLHttpRequest',
  'Accept'          : 'application/json, text/javascript, */*; q=0.01',
  'Accept-Language' : 'he',
  'User-Agent'      : 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
};

const MAP_EXCLUDED_CATS = new Set([
  'סיום אירוע',
  'האירוע הסתיים',
  'בדקות הקרובות צפויות להתקבל התרעות באזורך',
]);

const MAP_CACHE_TTL = 60 * 1000; // 60 seconds

// ── Siren city → zone lookup ─────────────────────────────────────────────────

let cityZoneMap = {};  // { cityName: zoneName }

async function loadSirenCities() {
  const limit = 200;
  let offset = 0, total = Infinity, fetched = 0;
  try {
    while (offset < total) {
      const url = `${CITIES_URL}?limit=${limit}&offset=${offset}`;
      const resp = await fetch(url, { headers: SIREN_HEADERS });
      const json = await resp.json();
      if (json.pagination) total = json.pagination.total;
      for (const c of (json.data || [])) {
        if (c.name && c.zone) cityZoneMap[c.name] = c.zone;
      }
      fetched += (json.data || []).length;
      offset += limit;
    }
    console.log(`[siren] loaded ${fetched} cities, ${Object.keys(cityZoneMap).length} with zones`);
  } catch (e) {
    console.error('[siren] cities load failed:', e.message);
  }
}

// ── Static coords — checked before Nominatim, never wrong ────────────────────
const STATIC_COORDS = {
  // Problematic moshavim Nominatim confuses with streets
  'חמד'                 : [32.0016, 34.8317],
  'גן יבנה'             : [31.7833, 34.7000],
  'בארות יצחק'          : [31.9667, 34.8833],
  'כפר ביל'             : [31.9500, 34.9000],
  // Major cities — skip Nominatim entirely
  'תל אביב - יפו'       : [32.0853, 34.7818],
  'ירושלים'             : [31.7683, 35.2137],
  'חיפה'                : [32.7940, 34.9896],
  'באר שבע'             : [31.2518, 34.7913],
  'ראשון לציון'          : [31.9730, 34.7925],
  'פתח תקווה'           : [32.0841, 34.8878],
  'אשדוד'               : [31.8040, 34.6550],
  'אשקלון'              : [31.6693, 34.5715],
  'נתניה'               : [32.3215, 34.8532],
  'בני ברק'             : [32.0840, 34.8340],
  'חולון'               : [32.0107, 34.7796],
  'רמת גן'              : [32.0707, 34.8238],
  'רחובות'              : [31.8928, 34.8113],
  'בת ים'               : [32.0233, 34.7503],
  'בית שמש'             : [31.7473, 34.9873],
  'הרצליה'              : [32.1659, 34.8439],
  'כפר סבא'             : [32.1752, 34.9078],
  'מודיעין מכבים רעות'  : [31.8969, 35.0104],
  'לוד'                 : [31.9514, 34.8953],
  'רמלה'                : [31.9290, 34.8700],
  'עכו'                 : [32.9282, 35.0714],
  'נהריה'               : [33.0043, 35.0982],
  'טבריה'               : [32.7922, 35.5312],
  'צפת'                 : [32.9641, 35.4956],
  'קריית שמונה'          : [33.2074, 35.5699],
  'אילת'                : [29.5577, 34.9519],
  'דימונה'              : [31.0657, 35.0326],
  'אופקים'              : [31.3133, 34.6215],
  'שדרות'               : [31.5242, 34.5961],
  'נתיבות'              : [31.4228, 34.5905],
  'ערד'                 : [31.2587, 35.2127],
  // Specific POIs Nominatim gets wrong
  'בית העלמין החדש עכו' : [32.9348, 35.1054],
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

// ── Israel time helpers ───────────────────────────────────────────────────────

function nowIsrael() {
  return new Date(Date.now() + ISRAEL_OFFSET_MS);
}
function israelIso(date = nowIsrael()) {
  return date.toISOString().replace('T', ' ').slice(0, 19);
}
function israelIsoAgo(ms) {
  return israelIso(new Date(Date.now() + ISRAEL_OFFSET_MS - ms));
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

function cleanCityName(name) {
  let clean = name.replace(/אזור תעשייה/g, '').replace(/פארק תעשייה/g, '');
  clean = clean.replace(/\s+-.*$/, '');   // " - צפון" → removed (\s+ requires space before hyphen)
  clean = clean.replace(/\(.*?\)/g, ''); // remove parentheses
  return clean.trim();
}

async function geocodeLocation(name) {
  // 1. Static dictionary — highest priority, never wrong
  const cleanName = cleanCityName(name);
  for (const key of [name, cleanName]) {
    if (STATIC_COORDS[key]) {
      geocache[name] = STATIC_COORDS[key];
      saveGeoCache();
      return geocache[name];
    }
  }
  // 2. In-memory cache
  if (name in geocache) return geocache[name];
  // 3. Nominatim — skip streets/buildings, keep everything else
  const wait = 1100 - (Date.now() - lastNominatim);
  if (wait > 0) await new Promise(r => setTimeout(r, wait));
  lastNominatim = Date.now();
  try {
    const url = `${NOM_URL}?q=${encodeURIComponent(cleanName + ' ישראל')}&countrycodes=il&format=json&limit=5&accept-language=he`;
    const resp = await fetch(url, { headers: { 'User-Agent': 'CevaAdom/1.2' } });
    const results = await resp.json();
    let coords = null;
    if (results.length) {
      const best = results.find(r => r.class !== 'highway' && r.class !== 'building');
      const chosen = best || results[0];
      coords = [parseFloat(chosen.lat), parseFloat(chosen.lon)];
    }
    geocache[name] = coords; // key stays as original zone name
  } catch { geocache[name] = null; }
  saveGeoCache();
  return geocache[name];
}

async function geocodeBatch(names, maxNew = 250) {
  const uncached = names.filter(n => !(n in geocache)).slice(0, maxNew);
  for (const loc of uncached) await geocodeLocation(loc);
  const result = {};
  for (const n of names) if (geocache[n]) result[n] = geocache[n];
  const pending = names.filter(n => !(n in geocache)).length;
  if (pending) result._pending = pending;
  return result;
}

// ── Fetch from OREF (history) ─────────────────────────────────────────────────

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
    setTimeout(loop, 60_000);
  };
  loop();
}

// ── Live feed (real-time, api.siren.co.il) ────────────────────────────────────

function simpleHash(str) {
  let h = 0;
  for (let i = 0; i < str.length; i++) h = Math.imul(31, h) + str.charCodeAt(i) | 0;
  return Math.abs(h);
}

// Track last active set to avoid redundant broadcasts
let lastActiveSignature = '';

async function fetchLiveAndStore() {
  const resp = await fetch(LIVE_URL, { headers: SIREN_HEADERS });
  const text = await resp.text();
  const trimmed = text.trim();
  if (!trimmed || trimmed === '{}' || trimmed.startsWith('<')) {
    lastActiveSignature = '';
    return [];
  }
  // Rate limit or non-JSON response — skip silently
  if (!trimmed.startsWith('{') && !trimmed.startsWith('[')) return [];
  const data = JSON.parse(trimmed);
  if (typeof data !== 'object' || !Object.keys(data).length) {
    lastActiveSignature = '';
    return [];
  }

  // Skip broadcast if same alerts as last poll
  const sig = JSON.stringify(data);
  if (sig === lastActiveSignature) return [];
  lastActiveSignature = sig;

  const now    = israelIso();
  const minute = now.slice(0, 16);

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
    setTimeout(loop, 10_000);
  };
  loop();
}

// ── Map-data cache ────────────────────────────────────────────────────────────

const mapDataCacheByHours = {};

// ── Query helpers ─────────────────────────────────────────────────────────────

function rowToEvent(row, search = null) {
  let locations = (row.locs || '').split('||').map(l => l.trim()).filter(Boolean);
  if (search) locations.sort(key => search && !key.includes(search) ? 1 : 0);
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
  let sql = `SELECT alert_date, group_concat(data,'||') AS locs, category, category_desc FROM alerts`;
  if (where.length) sql += ' WHERE ' + where.join(' AND ');
  sql += ' GROUP BY alert_date ORDER BY alert_date DESC LIMIT ?';
  params.push(limit);
  const rows = await dbAll(sql, params);
  return rows.map(r => rowToEvent(r));
}

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
    return {
      items: rows.map(r => rowToEvent(r, search)),
      total, totalAlerts, totalLocations, page: safePage, pageSize, totalPages
    };

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
    return {
      items: rows.map(r => rowToEvent(r)),
      total, totalAlerts, totalLocations, page: safePage, pageSize, totalPages
    };
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
  Object.keys(mapDataCacheByHours).forEach(k => delete mapDataCacheByHours[k]);
  const payload = `data: ${JSON.stringify(alerts)}\n\n`;
  for (const client of clients) client.write(payload);
}

// ── API routes ────────────────────────────────────────────────────────────────

app.get('/api/events', async (req, res) => {
  try {
    const page     = Math.max(1, parseInt(req.query.page) || 1);
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
    res.json(await geocodeBatch(ranked, 250));
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.get('/api/geocode', async (req, res) => {
  try {
    const names = (req.query.names || '').split(',').map(n => n.trim()).filter(Boolean);
    if (!names.length) return res.status(400).json({ error: 'missing names parameter' });
    res.json(await geocodeBatch(names, 250));
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.get('/api/map-data', async (req, res) => {
  try {
    const hours = Math.min(parseInt(req.query.hours) || 24, 168);

    const cached = mapDataCacheByHours[hours];
    if (cached && (Date.now() - cached.time) < MAP_CACHE_TTL)
      return res.json(cached.data);

    const since = israelIsoAgo(hours * 3600 * 1000);

    const rows = await dbAll(
      `SELECT data, alert_date, category_desc
       FROM alerts
       WHERE alert_date >= ?
         AND category_desc NOT IN (?, ?, ?)
       ORDER BY alert_date DESC`,
      [since, ...MAP_EXCLUDED_CATS]
    );

    const byLoc = {};
    for (const row of rows) {
      const name = (row.data || '').trim();
      if (!name) continue;
      if (!byLoc[name]) byLoc[name] = { name, count: 0, events: [] };
      byLoc[name].count++;
      if (byLoc[name].events.length < 8)
        byLoc[name].events.push({ alertDate: row.alert_date, category_desc: row.category_desc || '' });
    }

    const names = Object.keys(byLoc);
    await geocodeBatch(names, 250);

    const locations = names
      .filter(n => geocache[n])
      .map(n => ({
        name  : n,
        lat   : geocache[n][0],
        lng   : geocache[n][1],
        count : byLoc[n].count,
        events: byLoc[n].events,
        zone  : cityZoneMap[n] || null,
      }));

    const result = { generatedAt: new Date().toISOString(), hours, locations };
    mapDataCacheByHours[hours] = { data: result, time: Date.now() };
    res.json(result);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.get('/api/stats', async (req, res) => {
  try {
    const { total }  = await dbGet('SELECT COUNT(*) AS total FROM alerts');
    const { oldest } = await dbGet('SELECT MIN(alert_date) AS oldest FROM alerts');
    const { newest } = await dbGet('SELECT MAX(alert_date) AS newest FROM alerts');
    res.json({ total, oldest, newest });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.get('/api/live', (req, res) => {
  res.json({}); // live state is pushed via SSE; this endpoint kept for compatibility
});

// ── GeoJSON — circle polygons generated from geocache + zone data ─────────

function circlePolygon(lat, lng, radiusKm = 2.5, numPts = 24) {
  const coords = [];
  for (let i = 0; i <= numPts; i++) {
    const angle = (i / numPts) * 2 * Math.PI;
    const dLat  = (radiusKm / 111) * Math.sin(angle);
    const dLng  = (radiusKm / (111 * Math.cos(lat * Math.PI / 180))) * Math.cos(angle);
    coords.push([+(lng + dLng).toFixed(6), +(lat + dLat).toFixed(6)]);
  }
  return coords;
}

let geojsonCache = null;

app.get('/israel-cities.geojson', (req, res) => {
  if (!geojsonCache) {
    const features = [];
    for (const [name, coords] of Object.entries(geocache)) {
      if (!coords) continue;
      const [lat, lng] = coords;
      features.push({
        type      : 'Feature',
        properties: { name, zone: cityZoneMap[name] || null },
        geometry  : { type: 'Polygon', coordinates: [circlePolygon(lat, lng)] },
      });
    }
    geojsonCache = JSON.stringify({ type: 'FeatureCollection', features });
    console.log(`[geojson] generated ${features.length} city polygons`);
  }
  res.setHeader('Content-Type', 'application/geo+json');
  res.setHeader('Cache-Control', 'public, max-age=3600');
  res.send(geojsonCache);
});

app.get('/api/charts', async (req, res) => {
  try {
    const since = israelIsoAgo(7 * 24 * 3600 * 1000);
    const daily = await dbAll(
      `SELECT DATE(alert_date) AS d, COUNT(*) AS cnt
       FROM alerts WHERE alert_date >= ?
       GROUP BY d ORDER BY d`,
      [since]
    );
    const cats = await dbAll(
      `SELECT category_desc, COUNT(*) AS cnt
       FROM alerts
       WHERE alert_date >= ?
         AND (category_desc IS NULL OR category_desc NOT IN (?, ?))
       GROUP BY category_desc ORDER BY cnt DESC LIMIT 10`,
      [since, 'סיום אירוע', 'האירוע הסתיים']
    );
    res.json({
      daily     : daily.map(r => ({ date: r.d, count: r.cnt })),
      categories: cats.map(r => ({ name: r.category_desc || 'אחר', count: r.cnt })),
    });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// ── Start ─────────────────────────────────────────────────────────────────────

loadGeoCache();
loadSirenCities();
fetchAndStore()
  .then(items => console.log(`[init] fetched ${items.length} new alerts`))
  .catch(e => console.error('[init] fetch failed:', e.message));
startFetchLoop();
startLiveLoop();

app.listen(PORT, () => console.log(`Server running on port ${PORT}`));
