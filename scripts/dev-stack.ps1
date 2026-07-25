param(
    [ValidateSet("up", "start", "down", "stop", "status", "ps", "logs", "reset", "connection-string")]
    [string]$Action = "up",
    [string]$Service,
    [switch]$Yes
)

$ErrorActionPreference = "Stop"
$RepoRoot = Split-Path -Parent $PSScriptRoot
$ComposeFile = Join-Path $RepoRoot "compose.yaml"

function Test-Command([string]$Name) {
    return $null -ne (Get-Command $Name -ErrorAction SilentlyContinue)
}

function Test-ComposeCandidate([string]$Exe, [string[]]$Prefix) {
    & $Exe @Prefix version *> $null
    return $LASTEXITCODE -eq 0
}

$requested = $env:KUBEJOB_CONTAINER_ENGINE
$script:ComposeExe = $null
$script:ComposePrefix = @()

if ([string]::IsNullOrWhiteSpace($requested) -or $requested -eq "docker") {
    if (Test-Command "docker" -and (Test-ComposeCandidate "docker" @("compose"))) {
        $script:ComposeExe = "docker"
        $script:ComposePrefix = @("compose")
    }
}

if ($null -eq $script:ComposeExe -and ([string]::IsNullOrWhiteSpace($requested) -or $requested -eq "podman")) {
    if (Test-Command "podman" -and (Test-ComposeCandidate "podman" @("compose"))) {
        $script:ComposeExe = "podman"
        $script:ComposePrefix = @("compose")
    }
    elseif (Test-Command "podman-compose") {
        $script:ComposeExe = "podman-compose"
        $script:ComposePrefix = @()
    }
}

if ($null -eq $script:ComposeExe) {
    throw "No supported Compose provider found. Install Docker Compose, 'podman compose', or podman-compose."
}

$script:ProjectArgs = @("-f", $ComposeFile, "--project-directory", $RepoRoot)

function Invoke-Compose {
    param([Parameter(ValueFromRemainingArguments = $true)][string[]]$Arguments)

    & $script:ComposeExe @script:ComposePrefix @script:ProjectArgs @Arguments
    if ($LASTEXITCODE -ne 0) {
        throw "Compose command failed with exit code $LASTEXITCODE."
    }
}

function Invoke-ComposeRaw([string[]]$Arguments) {
    return & $script:ComposeExe @script:ComposePrefix @script:ProjectArgs @Arguments
}

function Wait-Service([string]$Name, [string[]]$HealthCommand) {
    for ($attempt = 1; $attempt -le 60; $attempt++) {
        & $script:ComposeExe @script:ComposePrefix @script:ProjectArgs exec -T $Name @HealthCommand *> $null
        if ($LASTEXITCODE -eq 0) {
            return
        }
        Start-Sleep -Seconds 2
    }

    Invoke-Compose ps
    throw "Timed out waiting for $Name."
}

function Get-PostgresConnectionString {
    $user = (Invoke-ComposeRaw @("exec", "-T", "postgres", "printenv", "POSTGRES_USER") | Out-String).Trim()
    $database = (Invoke-ComposeRaw @("exec", "-T", "postgres", "printenv", "POSTGRES_DB") | Out-String).Trim()
    $password = (Invoke-ComposeRaw @("exec", "-T", "postgres", "printenv", "POSTGRES_PASSWORD") | Out-String).Trim()
    $portOutput = (Invoke-ComposeRaw @("port", "postgres", "5432") | Select-Object -Last 1)
    $port = ($portOutput -split ":")[-1].Trim()
    return "Host=localhost;Port=$port;Database=$database;Username=$user;Password=$password"
}

switch ($Action) {
    { $_ -in @("up", "start") } {
        Invoke-Compose up -d
        Wait-Service "postgres" @("sh", "-ec", 'pg_isready -U "$POSTGRES_USER" -d "$POSTGRES_DB"')
        Wait-Service "rabbitmq" @("rabbitmq-diagnostics", "-q", "ping")

        $rabbitPortOutput = (Invoke-ComposeRaw @("port", "rabbitmq", "15672") | Select-Object -Last 1)
        $rabbitPort = ($rabbitPortOutput -split ":")[-1].Trim()
        $rabbitUser = (Invoke-ComposeRaw @("exec", "-T", "rabbitmq", "printenv", "RABBITMQ_DEFAULT_USER") | Out-String).Trim()
        $rabbitPassword = (Invoke-ComposeRaw @("exec", "-T", "rabbitmq", "printenv", "RABBITMQ_DEFAULT_PASS") | Out-String).Trim()

        Write-Host ""
        Write-Host "KubeJob development dependencies are ready."
        Write-Host "PostgreSQL: $(Get-PostgresConnectionString)"
        Write-Host "RabbitMQ UI: http://localhost:$rabbitPort"
        Write-Host "RabbitMQ credentials: $rabbitUser / $rabbitPassword"
    }
    "down" { Invoke-Compose down }
    "stop" { Invoke-Compose stop }
    { $_ -in @("status", "ps") } { Invoke-Compose ps }
    "logs" {
        if ([string]::IsNullOrWhiteSpace($Service)) {
            Invoke-Compose logs -f
        }
        else {
            Invoke-Compose logs -f $Service
        }
    }
    "reset" {
        if (-not $Yes) {
            throw "reset removes all local PostgreSQL and RabbitMQ data. Re-run with -Yes."
        }
        Invoke-Compose down --volumes --remove-orphans
    }
    "connection-string" { Write-Output (Get-PostgresConnectionString) }
}
