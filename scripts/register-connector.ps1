$ErrorActionPreference = "Stop"

# Корень проекта
$ProjectRoot = Split-Path $PSScriptRoot -Parent

# Пути к файлам
$EnvPath = Join-Path $ProjectRoot ".env"
$ConnectorPath = Join-Path $ProjectRoot "debezium\connector.json"

# Проверяем наличие файлов
if (-not (Test-Path $EnvPath)) {
    throw ".env file not found: $EnvPath"
}

if (-not (Test-Path $ConnectorPath)) {
    throw "Connector config not found: $ConnectorPath"
}

# Читаем .env
$EnvValues = @{}

foreach ($line in Get-Content $EnvPath) {
    $line = $line.Trim()

    if ($line -match "^([^#][^=]*)=(.*)$") {
        $EnvValues[$matches[1].Trim()] = $matches[2].Trim()
    }
}

# Проверяем пароль
if (-not $EnvValues.ContainsKey("POSTGRES_PASSWORD")) {
    throw "POSTGRES_PASSWORD not found in .env"
}

# Читаем Connector JSON
$Connector = Get-Content $ConnectorPath -Raw | ConvertFrom-Json

# Подставляем пароль PostgreSQL
$Connector.config."database.password" = $EnvValues["POSTGRES_PASSWORD"]

$BaseUrl = "http://localhost:8083"
$ConnectorName = $Connector.name

Write-Host "Checking Kafka Connect REST API..."

# Проверяем доступность Kafka Connect
Invoke-RestMethod `
    -Uri "$BaseUrl/connectors" `
    -Method Get `
    -TimeoutSec 10 | Out-Null

# Проверяем существование Connector
$ExistingConnectors = Invoke-RestMethod `
    -Uri "$BaseUrl/connectors" `
    -Method Get

if ($ExistingConnectors -contains $ConnectorName) {

    Write-Host "Connector exists. Updating..."

    $Body = $Connector.config | ConvertTo-Json -Depth 20

    Invoke-RestMethod `
        -Uri "$BaseUrl/connectors/$ConnectorName/config" `
        -Method Put `
        -ContentType "application/json" `
        -Body $Body | Out-Null
}
else {

    Write-Host "Registering new Connector..."

    $Body = $Connector | ConvertTo-Json -Depth 20

    Invoke-RestMethod `
        -Uri "$BaseUrl/connectors" `
        -Method Post `
        -ContentType "application/json" `
        -Body $Body | Out-Null
}

# Ожидаем запуска Connector и Task
Write-Host "Waiting for Connector to start..."

$MaxAttempts = 30
$Started = $false

for ($i = 1; $i -le $MaxAttempts; $i++) {

    Start-Sleep -Seconds 2

    $Status = Invoke-RestMethod `
        -Uri "$BaseUrl/connectors/$ConnectorName/status" `
        -Method Get

    if ($Status.connector.state -eq "FAILED") {
        throw "Connector failed: $($Status.connector.trace)"
    }

    $FailedTask = $Status.tasks | Where-Object {
        $_.state -eq "FAILED"
    }

    if ($FailedTask) {
        throw "Connector task failed: $($FailedTask.trace)"
    }

    if (
        $Status.connector.state -eq "RUNNING" -and
        $Status.tasks.Count -gt 0 -and
        @($Status.tasks | Where-Object { $_.state -ne "RUNNING" }).Count -eq 0
    ) {
        $Started = $true
        break
    }

    Write-Host "Attempt $i/$MaxAttempts. Current state: $($Status.connector.state)"
}

if (-not $Started) {
    throw "Connector did not reach RUNNING state within the expected time."
}

# Итоговый статус
Write-Host ""
Write-Host "Connector:" $Status.name
Write-Host "State:" $Status.connector.state

foreach ($task in $Status.tasks) {
    Write-Host "Task:" $task.id $task.state
}

Write-Host ""
Write-Host "Connector successfully started."
