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

Write-Host "Removing old multipath benchmark files..."

Remove-Item multipath_results.csv -ErrorAction SilentlyContinue
Remove-Item multipath_summary.csv -ErrorAction SilentlyContinue
Remove-Item multipath_commands.txt -ErrorAction SilentlyContinue

Write-Host "Starting server..."

$server = Start-Process `
    -FilePath $exe `
    -ArgumentList "server","127.0.0.1:9000" `
    -PassThru `
    -WindowStyle Hidden

Start-Sleep -Seconds 3

try {
    Write-Host "Preparing multipath benchmark commands..."

    $commands = @(
        "connect",

        "multipath on",

        "aimd",
        "multipath good",
        "scenario good",
        "experiment $Runs",

        "multipath mixed",
        "scenario lossy",
        "experiment $Runs",

        "multipath bad",
        "scenario bad",
        "experiment $Runs",

        "predictive",
        "multipath good",
        "scenario good",
        "experiment $Runs",

        "multipath mixed",
        "scenario lossy",
        "experiment $Runs",

        "multipath bad",
        "scenario bad",
        "experiment $Runs",

        "ai",
        "multipath good",
        "scenario good",
        "experiment $Runs",

        "multipath mixed",
        "scenario lossy",
        "experiment $Runs",

        "multipath bad",
        "scenario bad",
        "experiment $Runs",

        "summary",
        "mpsummary",

        "close",
        "exit"
    )

    $commands | Set-Content multipath_commands.txt -Encoding utf8

    Write-Host "Running multipath benchmark..."
    Write-Host "Runs per scenario: $Runs"

    Get-Content multipath_commands.txt | & $exe client 127.0.0.1:9000
}
finally {
    Write-Host "Stopping server..."

    Stop-Process -Id $server.Id -Force -ErrorAction SilentlyContinue
}

Write-Host ""
Write-Host "Multipath benchmark complete."
Write-Host "Check these files:"
Write-Host "  experiment_results.csv"
Write-Host "  summary_results.csv"
Write-Host "  multipath_results.csv"
Write-Host "  multipath_summary.csv"