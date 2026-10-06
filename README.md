# BSidesNYC OBS -> Slack Monitor

A small, read-only Python agent that maintains a WebSocket connection to OBS
Studio, polls `GetRecordStatus`, and posts health/state notifications to a Slack
Incoming Webhook.

## Requirements

- Windows 10/11
- Python 3.10+
- OBS Studio 28+ with obs-websocket 5.x
- `websocket-client`
- A Slack Incoming Webhook

## Install

```powershell
python -m pip install -r requirements.txt
Copy-Item config.example.ini config.ini
notepad config.ini
```

Secrets can be supplied through environment variables:

```powershell
$env:OBS_MONITOR_OBS_PASSWORD = "your-obs-password"
$env:OBS_MONITOR_SLACK_WEBHOOK_URL = "https://hooks.slack.com/services/..."
```

For a Scheduled Task, use persistent user/system environment variables rather
than variables that exist only in the current PowerShell process.

## Validate before running

Version 1.3 adds explicit configuration validation:

```powershell
python .\obs_slack_monitor.py --config .\config.ini --validate-config
```

This checks booleans, numbers/ranges, OBS URL/port/scheme, Slack webhook URL,
logging level, and other settings before the agent starts.

Then test each external dependency separately:

```powershell
python .\obs_slack_monitor.py --config .\config.ini --test-slack
python .\obs_slack_monitor.py --config .\config.ini --once
```

Run interactively:

```powershell
python .\obs_slack_monitor.py --config .\config.ini
```

## Run automatically

Install as a Scheduled Task at user logon:

```powershell
.\install_task.ps1
```

The installer validates the configuration before registering the task. The
normal installation runs as the current user with Limited privileges. It
prefers the real `pythonw.exe` from the Python installation so no console window
is left open.

A true pre-logon startup task remains available with `-AtStartup`, but requires
an elevated PowerShell session.

## Recommended conference settings

```ini
poll_interval_seconds = 5
reconnect_delay_seconds = 5
notification_retry_seconds = 30
failure_threshold = 3
recovery_threshold = 1
startup_grace_seconds = 30
repeat_offline_minutes = 5
repeat_not_recording_minutes = 0
heartbeat_minutes = 5
expect_recording = true
```

With stateful heartbeats, `repeat_not_recording_minutes = 0` is recommended.
You get one immediate not-recording alert and then CRITICAL heartbeats while
that state persists. Keep `repeat_offline_minutes` enabled because no heartbeat
can be produced while OBS is unreachable.

## Health semantics

- **OK**: OBS is reachable and recording as expected.
- **WARN**: OBS is reachable, but the recording is paused.
- **CRITICAL**: OBS is unreachable, or recording is expected but not active.
- **INFO**: lifecycle/test information that is not itself a health judgment.

## Reliability behavior

- Polling and WebSocket failures are separated into connection/authentication
  failures vs. failures retrieving `GetRecordStatus`.
- OBS protocol replies are type-checked before they are used.
- Requests have an overall deadline, not just a per-read socket timeout.
- Slack HTTP errors include the returned status/body in the local log.
- Failed Slack notifications are rate-limited by
  `notification_retry_seconds`; a Slack outage will not cause a POST every poll.
- Unexpected internal exceptions are logged with a traceback and the process
  exits. The Scheduled Task can then restart it instead of silently looping in
  an unknown state.
- The agent remains read-only and never starts/stops/splits OBS recordings.

## Logging

By default:

```text
obs_slack_monitor.log
```

is written next to `config.ini`, with rotation controlled by:

```ini
max_bytes = 1048576
backup_count = 3
```

## Security

- Use `127.0.0.1` for OBS when the agent runs on the OBS machine.
- Keep OBS WebSocket authentication enabled.
- Do not put credentials in the OBS URL.
- Treat the Slack webhook URL as a bearer secret.
- Non-local webhook URLs must use HTTPS.
- Prefer environment variables for secrets.

## Tests

The package includes standard-library unit tests:

```powershell
python -m unittest discover -s tests -v
```

The tests cover configuration validation, malformed OBS status data, monitor
severity semantics, Slack error handling, and a mocked obs-websocket handshake
and `GetRecordStatus` exchange.

## Style/maintenance

Version 1.3 was reviewed against the Google Python Style Guide and current
Python 3.14 documentation while retaining Python 3.10 compatibility. In
particular, it uses:

- a `main()` entry point and import-safe module behavior;
- small, typed functions and dataclasses;
- `pathlib` for paths;
- narrow exception handling in normal code;
- broad `Exception` handling only where it re-raises for classification or at
  the top-level isolation boundary where the exception is logged;
- validated configuration rather than `max()`-silently-corrected bad inputs;
- 80-column source formatting aside from unavoidable literal data.

See `STYLE_NOTES.md` for the review summary.
