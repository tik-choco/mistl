$ErrorActionPreference = "Stop"
Set-Location (Split-Path -Parent $PSScriptRoot)
node scripts/dev.mjs --watch
exit $LASTEXITCODE
