# ToolFB — đồng bộ code từ GitHub (Task Scheduler hoặc chạy tay).
# Chuột phải → Run with PowerShell, hoặc tạo Scheduled Task trỏ tới script này.

$ErrorActionPreference = "Stop"
$Root = if ($env:TOOLFB_ROOT) { $env:TOOLFB_ROOT } else { Split-Path -Parent $PSScriptRoot }
$Py = Join-Path $Root ".venv\Scripts\python.exe"
if (-not (Test-Path $Py)) {
    Write-Error "Không tìm thấy $Py — tạo venv và cài requirements trước."
}
& $Py (Join-Path $Root "tools\sync_from_github.py") --force
exit $LASTEXITCODE
