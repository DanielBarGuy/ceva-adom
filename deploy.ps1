# ── Ceva Adom – Deploy via SCP ───────────────────────────────────────────────

$VPS  = "root@187.124.2.41"
$DEST = "/var/www/ceva-adom"
$ROOT = $PSScriptRoot

Write-Host "`nDeploying to $VPS..." -ForegroundColor Cyan

scp "$ROOT\public\index.html"    "${VPS}:${DEST}/public/index.html"
scp "$ROOT\public\event.html"    "${VPS}:${DEST}/public/event.html"
scp "$ROOT\passenger_wsgi.py"    "${VPS}:${DEST}/passenger_wsgi.py"
scp "$ROOT\sync_github.py"       "${VPS}:${DEST}/sync_github.py"

Write-Host "Done!" -ForegroundColor Green
