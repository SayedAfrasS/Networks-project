param(
    [int]$Runs = 5
)

$ErrorActionPreference = "Stop"

Write-Host "Building project..."
cargo build

$exe = Join-Path $PSScriptRoot "target\debug\adaptive_transport.exe"

if (-not (Test-Path $exe)) {
    throw "Executable not found: $exe"
}

Write-Host "Removing old benchmark results..."

Remove-Item experiment_results.csv -ErrorAction SilentlyContinue
Remove-Item summary_results.csv -ErrorAction SilentlyContinue
Remove-Item benchmark_commands.txt -ErrorAction SilentlyContinue

Write-Host "Starting server..."

$server = Start-Process `
    -FilePath $exe `
    -ArgumentList "server","127.0.0.1:9000" `
    -PassThru `
    -WindowStyle Hidden

Start-Sleep -Seconds 3

try {
    Write-Host "Preparing benchmark commands..."

    $commands = @(
        "connect",

        "aimd",
        "scenario good",
        "experiment $Runs",
        "scenario lossy",
        "experiment $Runs",
        "scenario bad",
        "experiment $Runs",

        "predictive",
        "scenario good",
        "experiment $Runs",
        "scenario lossy",
        "experiment $Runs",
        "scenario bad",
        "experiment $Runs",

        "ai",
        "scenario good",
        "experiment $Runs",
        "scenario lossy",
        "experiment $Runs",
        "scenario bad",
        "experiment $Runs",

        "summary",
        "close",
        "exit"
    )

    $commands | Set-Content benchmark_commands.txt -Encoding utf8

    Write-Host "Running benchmark..."
    Write-Host "Runs per scenario: $Runs"

    Get-Content benchmark_commands.txt | & $exe client 127.0.0.1:9000
}
finally {
    Write-Host "Stopping server..."

    Stop-Process -Id $server.Id -Force -ErrorAction SilentlyContinue
}

Write-Host ""
Write-Host "Benchmark complete."
Write-Host "Check these files:"
Write-Host "  experiment_results.csv"
Write-Host "  summary_results.csv"