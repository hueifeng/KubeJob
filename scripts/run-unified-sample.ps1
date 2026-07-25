$ErrorActionPreference = "Stop"
$RepoRoot = Split-Path -Parent $PSScriptRoot
$StackScript = Join-Path $PSScriptRoot "dev-stack.ps1"
$Project = Join-Path $RepoRoot "samples/KubeJob.Sample.Unified/KubeJob.Sample.Unified.csproj"

& $StackScript -Action up
$connectionString = (& $StackScript -Action connection-string | Out-String).Trim()
$env:ConnectionStrings__KubeJob = $connectionString

Write-Host ""
Write-Host "Starting the unified sample with PostgreSQL persistence."
Write-Host "Dashboard: http://localhost:5041/admin/jobs"
Write-Host "After startup, seed real success/failure/retry/timeout scenarios with:"
Write-Host "  pwsh scripts/seed-dashboard-demo.ps1"
Write-Host ""

dotnet run --project $Project --launch-profile http @args
if ($LASTEXITCODE -ne 0) {
    exit $LASTEXITCODE
}
