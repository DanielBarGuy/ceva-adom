"""
Hostinger / Phusion Passenger entry-point for Ceva Adom.
/api/* routes are handled here; static files in ./public/ are served
directly by the web-server (LiteSpeed/Apache) without touching Python.
"""

import json, os, sys, time, sqlite3, threading, re
from urllib.request import Request, urlopen
from urllib.parse import quote, parse_qs
from datetime import datetime, timedelta, timezone

BASE_DIR   = os.path.dirname(os.path.abspath(__file__))
DB_FILE    = os.path.join(BASE_DIR, 'alerts.db')
CACHE_FILE = os.path.join(BASE_DIR, 'geocache.json')

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

def _init_db():
    with sqlite3.connect(DB_FILE) as c:
        c.execute('''
            CREATE TABLE IF NOT EXISTS alerts (
                rid           INTEGER PRIMARY KEY,
                data          TEXT    NOT NULL,
                alert_date    TEXT    NOT NULL,
                category      INTEGER,
                category_desc TEXT,
                matrix_id     INTEGER
            )
        ''')
        c.execute('CREATE INDEX IF NOT EXISTS idx_date ON alerts(alert_date)')

def _insert_alerts(rows):
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
                        (r.get('rid'), r.get('data', ''), r.get('alertDate', ''),
                         r.get('category'), r.get('category_desc', ''), r.get('matrix_id'))
                    )
                    added += c.rowcount
                except Exception:
                    pass
    return added

def _query_events(from_iso=None, to_iso=None, limit=2000, grouped=False):
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

def _db_stats():
    with _db_lock:
        with sqlite3.connect(DB_FILE) as c:
            total  = c.execute('SELECT COUNT(*) FROM alerts').fetchone()[0]
            oldest = c.execute('SELECT MIN(alert_date) FROM alerts').fetchone()[0]
            newest = c.execute('SELECT MAX(alert_date) FROM alerts').fetchone()[0]
    return total, oldest, newest

# ── Fetch from oref ───────────────────────────────────────────────────────────

def _fetch_and_store():
    req = Request(OREF_URL, headers=OREF_HEADERS)
    with urlopen(req, timeout=12) as resp:
        raw = resp.read().decode('utf-8-sig')
    data = json.loads(raw) if raw.strip() else []
    if not isinstance(data, list):
        return 0
    return _insert_alerts(data)

# ── Live real-time alert ───────────────────────────────────────────────────────

_live_alert      = None
_live_alert_lock = threading.Lock()

def _fetch_live():
    global _live_alert
    req = Request(LIVE_URL, headers=OREF_HEADERS)
    with urlopen(req, timeout=8) as resp:
        raw = resp.read().decode('utf-8-sig')
    data = json.loads(raw) if raw.strip() else None
    with _live_alert_lock:
        _live_alert = data

# ── Background refresh threads ────────────────────────────────────────────────

_fetch_started = False
_fetch_lock    = threading.Lock()

def _ensure_fetch_thread():
    global _fetch_started
    with _fetch_lock:
        if _fetch_started:
            return
        _fetch_started = True

    def _history_loop():
        while True:
            try:
                _fetch_and_store()
            except Exception as e:
                print(f'[fetch] error: {e}', flush=True)
            time.sleep(30)          # 30 s is friendlier to shared hosting

    def _live_loop():
        while True:
            try:
                _fetch_live()
            except Exception as e:
                print(f'[live] error: {e}', flush=True)
            time.sleep(10)

    threading.Thread(target=_history_loop, daemon=True).start()
    threading.Thread(target=_live_loop,    daemon=True).start()

# ── Geocoding ─────────────────────────────────────────────────────────────────

_geocache      : dict = {}
_geocache_lock  = threading.Lock()
_nominatim_lock = threading.Lock()
_last_nominatim = 0.0

def _load_geocache():
    if os.path.exists(CACHE_FILE):
        try:
            with open(CACHE_FILE, encoding='utf-8') as f:
                _geocache.update(json.load(f))
        except Exception:
            pass

def _save_geocache():
    try:
        with _geocache_lock:
            snap = dict(_geocache)
        with open(CACHE_FILE, 'w', encoding='utf-8') as f:
            json.dump(snap, f, ensure_ascii=False)
    except Exception:
        pass

def _clean_city_name(name):
    """Strip Pikud HaOref zone suffixes so Nominatim finds the actual city."""
    clean = name.replace('אזור תעשייה', '').replace('פארק תעשייה', '')
    clean = re.sub(r'\s*-.*$', '', clean)   # remove " - צפון" etc.
    clean = re.sub(r'\(.*?\)', '', clean)    # remove parentheses
    return clean.strip()


def _geocode_location(name):
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
            clean_name = _clean_city_name(name)
            url = (f'{NOMINATIM_URL}?q={quote(clean_name + " ישראל")}'
                   '&countrycodes=il&format=json&limit=1&accept-language=he')
            req = Request(url, headers={'User-Agent': 'CevaAdom/1.0'})
            with urlopen(req, timeout=8) as resp:
                results = json.loads(resp.read().decode('utf-8'))
            coords = [float(results[0]['lat']), float(results[0]['lon'])] if results else None
        except Exception:
            coords = None
    with _geocache_lock:
        _geocache[name] = coords  # key stays as original zone name
    _save_geocache()
    return coords

def _geocode_batch(names, max_new=100):
    with _geocache_lock:
        uncached = [n for n in names if n not in _geocache]
    if uncached:
        for loc in uncached[:max_new]:
            _geocode_location(loc)
    with _geocache_lock:
        return {n: _geocache[n] for n in names if _geocache.get(n) is not None}

# ── API handlers (return (status_int, payload_dict)) ─────────────────────────

def _resolve_from(params):
    """Return from_iso: prefer ?hours=N (server-side Israel time) over ?from=..."""
    hours = params.get('hours', [None])[0]
    if hours:
        return (datetime.now(ISRAEL_TZ) - timedelta(hours=int(hours))).strftime('%Y-%m-%d %H:%M:%S')
    return params.get('from', [None])[0]

def _api_events(params):
    import math
    from_iso  = _resolve_from(params)
    to_iso    = params.get('to',       [None])[0]
    search    = params.get('search',   [None])[0]
    page_size = int(params.get('pageSize', ['100'])[0])
    page      = int(params.get('page',     ['1'])[0])

    where, p = [], []
    if from_iso:
        where.append('alert_date >= ?'); p.append(from_iso)
    if to_iso:
        where.append('alert_date <= ?'); p.append(to_iso)
    if search:
        where.append('data LIKE ?'); p.append(f'%{search}%')
    w = ('WHERE ' + ' AND '.join(where)) if where else ''

    with _db_lock:
        with sqlite3.connect(DB_FILE) as c:
            total        = c.execute(f'SELECT COUNT(DISTINCT alert_date) FROM alerts {w}', p).fetchone()[0]
            total_alerts = c.execute(f'SELECT COUNT(*) FROM alerts {w}', p).fetchone()[0]
            total_locs   = c.execute(f'SELECT COUNT(DISTINCT data) FROM alerts {w}', p).fetchone()[0]

            total_pages = max(1, math.ceil(total / page_size))
            safe_page   = min(max(1, page), total_pages)
            offset      = (safe_page - 1) * page_size

            dates = [r[0] for r in c.execute(
                f'SELECT alert_date FROM alerts {w} GROUP BY alert_date ORDER BY alert_date DESC LIMIT ? OFFSET ?',
                p + [page_size, offset]
            ).fetchall()]

            items = []
            if dates:
                ph = ','.join('?' * len(dates))
                rows = c.execute(
                    f'SELECT alert_date, group_concat(data, "||"), category, category_desc '
                    f'FROM alerts WHERE alert_date IN ({ph}) '
                    f'GROUP BY alert_date ORDER BY alert_date DESC',
                    dates
                ).fetchall()
                for alert_date, loc_raw, category, category_desc in rows:
                    locs = [l.strip() for l in (loc_raw or '').split('||') if l.strip()]
                    items.append({
                        'alertDate'    : alert_date,
                        'category'     : category,
                        'category_desc': category_desc or '',
                        'locations'    : locs,
                        'count'        : len(locs),
                    })

    return 200, {
        'items'         : items,
        'totalLocations': total_locs,
        'total'         : total,
        'totalAlerts'   : total_alerts,
        'page'          : safe_page,
        'pageSize'      : page_size,
        'totalPages'    : total_pages,
    }

def _api_locations(params):
    from_iso = _resolve_from(params)
    to_iso   = params.get('to',   [None])[0]
    events   = _query_events(from_iso, to_iso, 2000)
    freq: dict = {}
    for ev in events:
        for loc in ev['locations']:
            freq[loc] = freq.get(loc, 0) + 1
    ranked = sorted(freq, key=freq.get, reverse=True)
    result = _geocode_batch(ranked, max_new=100)
    with _geocache_lock:
        pending = sum(1 for n in ranked if n not in _geocache)
    if pending:
        result['_pending'] = pending
    return 200, result

def _api_geocode(params):
    raw   = params.get('names', [''])[0]
    names = [n.strip() for n in raw.split(',') if n.strip()]
    if not names:
        return 400, {'error': 'missing names parameter'}
    result = _geocode_batch(names, max_new=100)
    with _geocache_lock:
        pending = sum(1 for n in names if n not in _geocache)
    if pending:
        result['_pending'] = pending
    return 200, result

def _api_stats(_params=None):
    total, oldest, newest = _db_stats()
    return 200, {'total': total, 'oldest': oldest, 'newest': newest}

def _api_live(_params=None):
    with _live_alert_lock:
        alert = _live_alert
    return 200, alert if alert is not None else {}

_ROUTES = {
    '/api/events'   : _api_events,
    '/api/locations': _api_locations,
    '/api/geocode'  : _api_geocode,
    '/api/stats'    : _api_stats,
    '/api/live'     : _api_live,
}

_STATUS_TEXT = {200: '200 OK', 400: '400 Bad Request', 500: '500 Internal Server Error'}

# ── WSGI application ──────────────────────────────────────────────────────────

def application(environ, start_response):
    path   = environ.get('PATH_INFO', '/')
    params = parse_qs(environ.get('QUERY_STRING', ''))

    handler = _ROUTES.get(path)
    if handler is None:
        start_response('404 Not Found', [('Content-Type', 'text/plain; charset=utf-8')])
        return [b'Not Found']

    try:
        status_code, payload = handler(params)
    except Exception as e:
        status_code, payload = 500, {'error': str(e)}

    body = json.dumps(payload, ensure_ascii=False).encode('utf-8')
    start_response(_STATUS_TEXT.get(status_code, f'{status_code} Error'), [
        ('Content-Type',   'application/json; charset=utf-8'),
        ('Content-Length', str(len(body))),
        ('Access-Control-Allow-Origin', '*'),
    ])
    return [body]

# ── Startup ───────────────────────────────────────────────────────────────────

_init_db()
_load_geocache()
try:
    _fetch_and_store()
except Exception as e:
    print(f'[init] initial fetch failed: {e}', flush=True)
try:
    _fetch_live()
except Exception as e:
    print(f'[init] live fetch failed: {e}', flush=True)
_ensure_fetch_thread()
