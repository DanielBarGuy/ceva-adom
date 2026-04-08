"""
שרת אזעקות צבע אדום
- SQLite history: accumulates every alert ever fetched
- /api/events  : returns events grouped by alertDate
- /api/locations: geocodes unique locations (cached to disk)
Run: python server.py
"""

import json, os, sys, time, sqlite3, threading
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from urllib.request import Request, urlopen
from urllib.error import HTTPError, URLError
from urllib.parse import quote, urlparse, parse_qs
from datetime import datetime, timedelta, timezone

PORT     = 3000
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PUBLIC_DIR  = os.path.join(BASE_DIR, 'public')
DB_FILE     = os.path.join(BASE_DIR, 'alerts.db')
CACHE_FILE  = os.path.join(BASE_DIR, 'geocache.json')

OREF_URL = (
    'https://alerts-history.oref.org.il/Shared/Ajax/GetAlarmsHistory.aspx'
    '?lang=he&mode=1'
)
LIVE_URL      = 'https://www.oref.org.il/WarningMessages/Alert/alerts.json'
NOMINATIM_URL = 'https://nominatim.openstreetmap.org/search'

OREF_HEADERS = {
    'Referer'         : 'https://www.oref.org.il/',
    'X-Requested-With': 'XMLHttpRequest',
    'Accept'          : 'application/json, text/javascript, */*; q=0.01',
    'Accept-Language' : 'he',
    'User-Agent'      : (
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
        'AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
    ),
}

ISRAEL_TZ = timezone(timedelta(hours=3))

# ── SQLite ────────────────────────────────────────────────────────────────────

_db_lock = threading.Lock()

def init_db():
    with sqlite3.connect(DB_FILE) as c:
        c.execute('''
            CREATE TABLE IF NOT EXISTS alerts (
                rid          INTEGER PRIMARY KEY,
                data         TEXT    NOT NULL,
                alert_date   TEXT    NOT NULL,
                category     INTEGER,
                category_desc TEXT,
                matrix_id    INTEGER
            )
        ''')
        c.execute('CREATE INDEX IF NOT EXISTS idx_date ON alerts(alert_date)')
    print('[db] initialised', flush=True)


def insert_alerts(rows: list):
    """Insert a batch of alert dicts; silently skips duplicates."""
    if not rows:
        return 0
    with _db_lock:
        with sqlite3.connect(DB_FILE) as c:
            added = 0
            for r in rows:
                try:
                    c.execute(
                        'INSERT OR IGNORE INTO alerts'
                        ' (rid, data, alert_date, category, category_desc, matrix_id)'
                        ' VALUES (?,?,?,?,?,?)',
                        (r.get('rid'), r.get('data',''), r.get('alertDate',''),
                         r.get('category'), r.get('category_desc',''), r.get('matrix_id'))
                    )
                    added += c.rowcount
                except Exception:
                    pass
    return added


def query_events(from_iso: str = None, to_iso: str = None, limit: int = 1000, grouped: bool = True):
    """
    Return events (newest first).
    grouped=True : one row per alert_date, locations[] contains all cities
    grouped=False: one row per city (raw)
    Each event: { alertDate, category, category_desc, locations[], count }
    """
    if grouped:
        sql = '''
            SELECT alert_date,
                   group_concat(data, '||'),
                   category,
                   category_desc
            FROM   alerts
        '''
    else:
        sql = '''
            SELECT alert_date,
                   data,
                   category,
                   category_desc
            FROM   alerts
        '''
    where, params = [], []
    if from_iso:
        where.append('alert_date >= ?'); params.append(from_iso)
    if to_iso:
        where.append('alert_date <= ?'); params.append(to_iso)
    if where:
        sql += ' WHERE ' + ' AND '.join(where)
    if grouped:
        sql += ' GROUP BY alert_date ORDER BY alert_date DESC LIMIT ?'
    else:
        sql += ' ORDER BY alert_date DESC LIMIT ?'
    params.append(limit)

    with _db_lock:
        with sqlite3.connect(DB_FILE) as c:
            rows = c.execute(sql, params).fetchall()

    events = []
    for alert_date, loc_raw, category, category_desc in rows:
        if grouped:
            locs = [l.strip() for l in (loc_raw or '').split('||') if l.strip()]
        else:
            locs = [loc_raw.strip()] if loc_raw and loc_raw.strip() else []
        events.append({
            'alertDate'    : alert_date,
            'category'     : category,
            'category_desc': category_desc or '',
            'locations'    : locs,
            'count'        : len(locs),
        })
    return events


def db_stats():
    with _db_lock:
        with sqlite3.connect(DB_FILE) as c:
            total  = c.execute('SELECT COUNT(*) FROM alerts').fetchone()[0]
            oldest = c.execute('SELECT MIN(alert_date) FROM alerts').fetchone()[0]
            newest = c.execute('SELECT MAX(alert_date) FROM alerts').fetchone()[0]
    return total, oldest, newest


def count_distinct_locations(from_iso: str = None, to_iso: str = None) -> int:
    where, params = [], []
    if from_iso:
        where.append('alert_date >= ?'); params.append(from_iso)
    if to_iso:
        where.append('alert_date <= ?'); params.append(to_iso)
    sql = 'SELECT COUNT(DISTINCT data) FROM alerts'
    if where:
        sql += ' WHERE ' + ' AND '.join(where)
    with _db_lock:
        with sqlite3.connect(DB_FILE) as c:
            return c.execute(sql, params).fetchone()[0]


def count_stats(from_iso: str = None, to_iso: str = None):
    """Returns (total_distinct_dates, total_alerts) for the given range."""
    where, params = [], []
    if from_iso:
        where.append('alert_date >= ?'); params.append(from_iso)
    if to_iso:
        where.append('alert_date <= ?'); params.append(to_iso)
    clause = (' WHERE ' + ' AND '.join(where)) if where else ''
    with _db_lock:
        with sqlite3.connect(DB_FILE) as c:
            total_dates  = c.execute(
                f'SELECT COUNT(DISTINCT alert_date) FROM alerts{clause}', params).fetchone()[0]
            total_alerts = c.execute(
                f'SELECT COUNT(*) FROM alerts{clause}', params).fetchone()[0]
    return total_dates, total_alerts


# ── Fetch from oref ───────────────────────────────────────────────────────────

def fetch_and_store():
    """Fetch latest alerts from oref and persist new ones to SQLite."""
    req = Request(OREF_URL, headers=OREF_HEADERS)
    with urlopen(req, timeout=12) as resp:
        raw = resp.read().decode('utf-8-sig')
    data = json.loads(raw) if raw.strip() else []
    if not isinstance(data, list):
        return 0
    added = insert_alerts(data)
    return added


# ── Live real-time alert ───────────────────────────────────────────────────────

_live_alert      = None
_live_alert_lock = threading.Lock()

def fetch_live():
    """Fetch current live alert from oref; stores result in memory."""
    global _live_alert
    req = Request(LIVE_URL, headers=OREF_HEADERS)
    with urlopen(req, timeout=8) as resp:
        raw = resp.read().decode('utf-8-sig')
    data = json.loads(raw) if raw.strip() else None
    with _live_alert_lock:
        _live_alert = data


# ── Background refresh threads ────────────────────────────────────────────────

_fetch_thread_started = False

def _bg_fetch_loop():
    """Runs forever; fetches history from oref every 15 s."""
    while True:
        try:
            added = fetch_and_store()
            if added:
                print(f'[fetch] +{added} new alerts stored', flush=True)
        except Exception as e:
            print(f'[fetch] error: {e}', flush=True)
        time.sleep(15)

def _bg_live_loop():
    """Runs forever; polls live alert every 10 s."""
    while True:
        try:
            fetch_live()
        except Exception as e:
            print(f'[live] error: {e}', flush=True)
        time.sleep(10)

def ensure_fetch_thread():
    global _fetch_thread_started
    if not _fetch_thread_started:
        _fetch_thread_started = True
        threading.Thread(target=_bg_fetch_loop, daemon=True).start()
        threading.Thread(target=_bg_live_loop,  daemon=True).start()
        print('[fetch] background threads started (history 15 s, live 10 s)', flush=True)


# ── Geocoding ─────────────────────────────────────────────────────────────────

_geocache      : dict = {}
_geocache_lock = threading.Lock()
_nominatim_lock = threading.Lock()
_last_nominatim = 0.0


def _load_geocache():
    if os.path.exists(CACHE_FILE):
        try:
            with open(CACHE_FILE, encoding='utf-8') as f:
                _geocache.update(json.load(f))
            print(f'[geo] loaded {len(_geocache)} cached entries', flush=True)
        except Exception as e:
            print(f'[geo] cache load error: {e}', flush=True)

def _save_geocache():
    try:
        with _geocache_lock:
            snap = dict(_geocache)
        with open(CACHE_FILE, 'w', encoding='utf-8') as f:
            json.dump(snap, f, ensure_ascii=False)
    except Exception as e:
        print(f'[geo] cache save error: {e}', flush=True)


def geocode_location(name: str):
    global _last_nominatim
    with _geocache_lock:
        if name in _geocache:
            return _geocache[name]
    with _nominatim_lock:
        with _geocache_lock:
            if name in _geocache:
                return _geocache[name]
        elapsed = time.time() - _last_nominatim
        if elapsed < 1.1:
            time.sleep(1.1 - elapsed)
        _last_nominatim = time.time()
        try:
            url = (f'{NOMINATIM_URL}?q={quote(name + " ישראל")}'
                   '&countrycodes=il&format=json&limit=1&accept-language=he')
            req = Request(url, headers={'User-Agent': 'CevaAdom/1.0 local'})
            with urlopen(req, timeout=8) as resp:
                results = json.loads(resp.read().decode('utf-8'))
            coords = [float(results[0]['lat']), float(results[0]['lon'])] if results else None
        except Exception:
            coords = None
    with _geocache_lock:
        _geocache[name] = coords
    _save_geocache()
    return coords


def geocode_batch(names: list, max_new: int = 100):
    with _geocache_lock:
        uncached = [n for n in names if n not in _geocache]
    if uncached:
        to_fetch = uncached[:max_new]
        print(f'[geo] fetching {len(to_fetch)} new ({len(uncached)-len(to_fetch)} deferred)',
              flush=True)
        for i, loc in enumerate(to_fetch, 1):
            geocode_location(loc)
            if len(to_fetch) > 5:
                print(f'[geo] {i}/{len(to_fetch)} {loc}', flush=True)
    with _geocache_lock:
        return {n: _geocache[n] for n in names if _geocache.get(n) is not None}


# ── HTTP handler ──────────────────────────────────────────────────────────────

class Handler(SimpleHTTPRequestHandler):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, directory=PUBLIC_DIR, **kwargs)

    def do_GET(self):
        parsed = urlparse(self.path)
        path   = parsed.path
        params = parse_qs(parsed.query)

        if path == '/api/events':
            self._handle_events(params)
        elif path == '/api/locations':
            self._handle_locations(params)
        elif path == '/api/geocode':
            self._handle_geocode(params)
        elif path == '/api/stats':
            self._handle_stats()
        elif path == '/api/geocache':
            self._handle_geocache_all()
        elif path == '/api/live':
            self._handle_live()
        else:
            super().do_GET()

    # ── /api/events ────────────────────────────────────────────────────────────
    def _resolve_from(self, params):
        hours = params.get('hours', [None])[0]
        if hours:
            return (datetime.now(ISRAEL_TZ) - timedelta(hours=int(hours))).strftime('%Y-%m-%d %H:%M:%S')
        return params.get('from', [None])[0]

    def _handle_events(self, params):
        try:
            import math
            from_iso        = self._resolve_from(params)
            to_iso          = params.get('to',    [None])[0]
            limit           = int(params.get('limit',    ['2000'])[0])
            page_size       = int(params.get('pageSize', ['100'])[0])
            page            = int(params.get('page',     ['1'])[0])
            grouped         = params.get('grouped', ['1'])[0] != '0'
            all_events      = query_events(from_iso, to_iso, limit, grouped)
            total_locs      = count_distinct_locations(from_iso, to_iso)
            total, total_alerts = count_stats(from_iso, to_iso)
            total_pages     = max(1, math.ceil(len(all_events) / page_size))
            safe_page       = min(max(1, page), total_pages)
            offset          = (safe_page - 1) * page_size
            page_items      = all_events[offset: offset + page_size]
            self._send_json(200, {
                'items'         : page_items,
                'totalLocations': total_locs,
                'total'         : total,
                'totalAlerts'   : total_alerts,
                'page'          : safe_page,
                'pageSize'      : page_size,
                'totalPages'    : total_pages,
            })
        except Exception as e:
            self._send_json(500, {'error': str(e)})

    # ── /api/locations ─────────────────────────────────────────────────────────
    def _handle_locations(self, params):
        try:
            from_iso = self._resolve_from(params)
            to_iso   = params.get('to',   [None])[0]

            events   = query_events(from_iso, to_iso, 2000)
            freq: dict = {}
            for ev in events:
                for loc in ev['locations']:
                    freq[loc] = freq.get(loc, 0) + 1
            ranked  = sorted(freq, key=freq.get, reverse=True)
            result  = geocode_batch(ranked, max_new=100)

            with _geocache_lock:
                pending = sum(1 for n in ranked if n not in _geocache)
            if pending:
                result['_pending'] = pending

            self._send_json(200, result)
        except Exception as e:
            self._send_json(500, {'error': str(e)})

    # ── /api/geocode ───────────────────────────────────────────────────────────
    def _handle_geocode(self, params):
        """
        GET /api/geocode?names=loc1,loc2,loc3
        Returns { name: [lat,lng]|null, ..., _pending: N }
        Geocodes up to 100 uncached names per call; poll until _pending==0.
        """
        try:
            raw   = params.get('names', [''])[0]
            names = [n.strip() for n in raw.split(',') if n.strip()]
            if not names:
                self._send_json(400, {'error': 'missing names parameter'})
                return
            result = geocode_batch(names, max_new=100)
            with _geocache_lock:
                pending = sum(1 for n in names if n not in _geocache)
            if pending:
                result['_pending'] = pending
            self._send_json(200, result)
        except Exception as e:
            self._send_json(500, {'error': str(e)})

    # ── /api/stats ─────────────────────────────────────────────────────────────
    def _handle_stats(self):
        try:
            total, oldest, newest = db_stats()
            self._send_json(200, {'total': total, 'oldest': oldest, 'newest': newest})
        except Exception as e:
            self._send_json(500, {'error': str(e)})

    # ── /api/geocache ──────────────────────────────────────────────────────────
    def _handle_geocache_all(self):
        with _geocache_lock:
            data = dict(_geocache)
        self._send_json(200, data)

    # ── /api/live ──────────────────────────────────────────────────────────────
    def _handle_live(self):
        with _live_alert_lock:
            alert = _live_alert
        self._send_json(200, alert if alert is not None else {})

    def _send_json(self, status, payload):
        body = json.dumps(payload, ensure_ascii=False).encode('utf-8')
        self.send_response(status)
        self.send_header('Content-Type', 'application/json; charset=utf-8')
        self.send_header('Content-Length', str(len(body)))
        self.send_header('Access-Control-Allow-Origin', '*')
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, fmt, *args):
        if '/api/' in str(args[0] if args else ''):
            print(f'[{datetime.now().strftime("%H:%M:%S")}] {fmt % args}', flush=True)


# ── Entry point ───────────────────────────────────────────────────────────────

if __name__ == '__main__':
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')
    sys.stderr.reconfigure(encoding='utf-8', errors='replace')

    init_db()
    _load_geocache()

    # Do one immediate fetch before starting the server
    try:
        added = fetch_and_store()
        total, oldest, newest = db_stats()
        print(f'[init] fetched {added} new alerts | DB: {total} total | {oldest} to {newest}',
              flush=True)
    except Exception as e:
        print(f'[init] initial fetch failed: {e}', flush=True)
    try:
        fetch_live()
        print('[init] live alert fetched', flush=True)
    except Exception as e:
        print(f'[init] live fetch failed: {e}', flush=True)

    ensure_fetch_thread()

    server = ThreadingHTTPServer(('', PORT), Handler)
    print(f'Server running: http://localhost:{PORT}', flush=True)
    print('Stop: Ctrl+C\n', flush=True)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print('\nServer stopped.')
        server.shutdown()
