$ErrorActionPreference = 'Stop'
$PSNativeCommandUseErrorActionPreference = $true

Set-Location -LiteralPath $PSScriptRoot
npm.cmd --prefix frontend run build

docker compose stop frontend backend
docker compose -f compose.yaml -f compose.local.yaml up -d --wait postgres

$env:PYTHONPATH = (Resolve-Path -LiteralPath .\backend).Path
$env:TRADER_CONTAINER_LOCAL = '/app/backend/.local'
$env:DATABASE_URL = 'host=127.0.0.1 port=55432 dbname=trader user=trader'
$env:PGPASSWORD = docker compose exec -T postgres printenv POSTGRES_PASSWORD

try {
    .\.venv\Scripts\python.exe -m news_futures operator-api .local/runtime.json --frontend frontend/dist
} finally {
    Remove-Item Env:PGPASSWORD
}
