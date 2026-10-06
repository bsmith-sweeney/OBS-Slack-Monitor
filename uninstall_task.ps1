param([string]$TaskName = "BSidesNYC OBS Slack Monitor")

$task = Get-ScheduledTask -TaskName $TaskName -ErrorAction SilentlyContinue
if (-not $task) {
    Write-Host "Scheduled task not found: $TaskName"
    exit 0
}

Unregister-ScheduledTask `
    -TaskName $TaskName `
    -Confirm:$false `
    -ErrorAction Stop

Write-Host "Removed scheduled task: $TaskName"
