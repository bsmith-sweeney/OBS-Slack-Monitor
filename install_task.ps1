[CmdletBinding()]
param(
    [string]$TaskName = "BSidesNYC OBS Slack Monitor",
    [string]$InstallDir = $PSScriptRoot,
    [string]$ConfigFile = "config.ini",
    [string]$PythonExe = "",
    [switch]$AtStartup
)

$ErrorActionPreference = "Stop"

function Test-IsAdministrator {
    $identity = [Security.Principal.WindowsIdentity]::GetCurrent()
    $principal = New-Object Security.Principal.WindowsPrincipal($identity)
    return $principal.IsInRole(
        [Security.Principal.WindowsBuiltInRole]::Administrator
    )
}

function Resolve-RealPython {
    param([string]$RequestedPython)

    if ($RequestedPython) {
        if (-not (Test-Path $RequestedPython)) {
            throw "Python executable not found: $RequestedPython"
        }
        return (Resolve-Path $RequestedPython).Path
    }

    $commands = @()

    $py = Get-Command py.exe -ErrorAction SilentlyContinue
    if ($py) {
        $commands += ,@($py.Source, "-3")
    }

    $python = Get-Command python.exe -ErrorAction SilentlyContinue
    if ($python) {
        $commands += ,@($python.Source)
    }

    if ($commands.Count -eq 0) {
        throw "Python was not found. Supply -PythonExe C:\Path\To\python.exe"
    }

    foreach ($cmd in $commands) {
        try {
            $exe = $cmd[0]
            $prefixArgs = @()
            if ($cmd.Count -gt 1) {
                $prefixArgs = $cmd[1..($cmd.Count - 1)]
            }

            $resolved = & $exe @prefixArgs -c `
                "import sys; print(sys.executable)" 2>$null
            if ($LASTEXITCODE -eq 0 -and $resolved) {
                $resolved = ($resolved | Select-Object -First 1).Trim()
                if (Test-Path $resolved) {
                    return (Resolve-Path $resolved).Path
                }
            }
        }
        catch {
            # Try the next Python command.
        }
    }

    throw "Could not resolve the real Python executable. " +
        "Supply -PythonExe C:\Path\To\python.exe"
}

try {
    $InstallDir = (Resolve-Path $InstallDir).Path
    $agent = Join-Path $InstallDir "obs_slack_monitor.py"
    $config = Join-Path $InstallDir $ConfigFile

    if (-not (Test-Path $agent)) {
        throw "Agent not found: $agent"
    }
    if (-not (Test-Path $config)) {
        throw "Config not found: $config"
    }

    $consolePython = Resolve-RealPython -RequestedPython $PythonExe
    $pythonDir = Split-Path $consolePython
    $pythonw = Join-Path $pythonDir "pythonw.exe"
    $taskPython = if (Test-Path $pythonw) {
        (Resolve-Path $pythonw).Path
    }
    else {
        $consolePython
    }

    Write-Host "Validating configuration..."
    & $consolePython $agent --config $config --validate-config
    if ($LASTEXITCODE -ne 0) {
        throw "Configuration validation failed."
    }

    $arguments = "`"$agent`" --config `"$config`""
    $action = New-ScheduledTaskAction `
        -Execute $taskPython `
        -Argument $arguments `
        -WorkingDirectory $InstallDir

    $settings = New-ScheduledTaskSettingsSet `
        -StartWhenAvailable `
        -RestartCount 3 `
        -RestartInterval (New-TimeSpan -Minutes 1) `
        -ExecutionTimeLimit ([TimeSpan]::Zero)

    if ($AtStartup) {
        if (-not (Test-IsAdministrator)) {
            throw "The -AtStartup option requires an elevated " +
                "PowerShell session. Normally omit -AtStartup."
        }

        $trigger = New-ScheduledTaskTrigger -AtStartup
        Register-ScheduledTask `
            -TaskName $TaskName `
            -Action $action `
            -Trigger $trigger `
            -RunLevel Highest `
            -Settings $settings `
            -Force `
            -ErrorAction Stop | Out-Null
    }
    else {
        $user = [System.Security.Principal.WindowsIdentity]::GetCurrent().Name
        $trigger = New-ScheduledTaskTrigger -AtLogOn -User $user

        Register-ScheduledTask `
            -TaskName $TaskName `
            -Action $action `
            -Trigger $trigger `
            -User $user `
            -RunLevel Limited `
            -Settings $settings `
            -Force `
            -ErrorAction Stop | Out-Null
    }

    $task = Get-ScheduledTask -TaskName $TaskName -ErrorAction Stop
    if (-not $task) {
        throw "Task registration returned without error, but " +
            "'$TaskName' could not be found afterward."
    }

    Write-Host ""
    Write-Host "Installed scheduled task: $TaskName" -ForegroundColor Green
    Write-Host "Task state: $($task.State)"
    Write-Host "Python:     $taskPython"
    Write-Host "Agent:      $agent"
    Write-Host "Config:     $config"
    if (-not $AtStartup) {
        Write-Host "Trigger:    At logon for $user"
        Write-Host "Run level:  Limited"
    }
    else {
        Write-Host "Trigger:    At Windows startup"
        Write-Host "Run level:  Highest"
    }
}
catch {
    Write-Error "Installation failed: $($_.Exception.Message)"
    exit 1
}
