param(
    [string]$BaseUrl = $(if ($env:KUBEJOB_SAMPLE_URL) { $env:KUBEJOB_SAMPLE_URL } else { "http://localhost:5041" })
)

$ErrorActionPreference = "Stop"
$normalizedBaseUrl = $BaseUrl.TrimEnd('/')
$endpoint = "$normalizedBaseUrl/demo/scenarios"

Write-Host "Submitting KubeJob Dashboard demo scenarios to $endpoint"
$response = Invoke-RestMethod -Method Post -Uri $endpoint -Headers @{ Accept = "application/json" }
$response | ConvertTo-Json -Depth 6

Write-Host ""
Write-Host "Dashboard: $normalizedBaseUrl/admin/jobs"
Write-Host "Failures:  $normalizedBaseUrl/admin/jobs/failures"
Write-Host "The cancel-me job runs for up to 60 seconds; open it in the Dashboard and request cancellation."
