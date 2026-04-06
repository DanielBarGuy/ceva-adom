#!/usr/bin/env python3
"""Sync latest alerts from dleshem/israel-alerts-data (runs every minute via cron)."""
import sqlite3, csv, io, os, sys
from urllib.request import Request, urlopen
from urllib.error import URLError

DB_FILE = os.path.join(os.path.dirname(__file__), 'alerts.db')
CSV_URL = 'https://raw.githubusercontent.com/dleshem/israel-alerts-data/main/israel-alerts.csv'
TAIL_BYTES = 500_000  # 500KB — enough for ~5000 recent alerts

FIELDNAMES = ['data', 'date', 'time', 'alertDate', 'category', 'category_desc', 'matrix_id', 'rid']


def get_latest_date():
    with sqlite3.connect(DB_FILE) as c:
        row = c.execute('SELECT MAX(alert_date) FROM alerts').fetchone()
        return row[0] if row and row[0] else '2000-01-01T00:00:00'


def download_tail():
    # Get file size via HEAD
    try:
        req = Request(CSV_URL, method='HEAD',
                      headers={'User-Agent': 'Mozilla/5.0'})
        with urlopen(req, timeout=15) as r:
            size = int(r.headers.get('Content-Length', 0))
    except Exception:
        size = 0

    if size > TAIL_BYTES:
        offset = size - TAIL_BYTES
        req = Request(CSV_URL,
                      headers={'User-Agent': 'Mozilla/5.0',
                               'Range': f'bytes={offset}-'})
    else:
        req = Request(CSV_URL, headers={'User-Agent': 'Mozilla/5.0'})

    with urlopen(req, timeout=60) as r:
        return r.read().decode('utf-8', errors='replace')


def sync():
    latest = get_latest_date()

    try:
        raw = download_tail()
    except Exception as e:
        print(f'[sync] download error: {e}', flush=True)
        sys.exit(1)

    # Skip potential partial first line (Range response may start mid-line)
    first_nl = raw.find('\n')
    if first_nl == -1:
        return
    body = raw[first_nl + 1:]

    reader = csv.DictReader(io.StringIO(body), fieldnames=FIELDNAMES)

    added = 0
    with sqlite3.connect(DB_FILE) as c:
        for row in reader:
            date = row.get('alertDate', '')
            if not date or date <= latest:
                continue
            rid = row.get('rid')
            try:
                rid = int(rid) if rid else None
            except ValueError:
                rid = None
            try:
                c.execute(
                    'INSERT OR IGNORE INTO alerts'
                    ' (rid, data, alert_date, category, category_desc, matrix_id)'
                    ' VALUES (?,?,?,?,?,?)',
                    (rid, row.get('data', ''), date,
                     row.get('category'), row.get('category_desc', ''),
                     row.get('matrix_id'))
                )
                if c.rowcount:
                    added += 1
            except Exception:
                pass

    if added:
        print(f'[sync] added {added} new alerts (latest was {latest})', flush=True)


if __name__ == '__main__':
    sync()
