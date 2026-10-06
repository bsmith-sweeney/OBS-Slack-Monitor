#!/usr/bin/env python3
"""Lightweight OBS-to-Slack monitoring agent.

Monitors an OBS Studio obs-websocket 5.x endpoint by polling
GetRecordStatus over a persistent WebSocket connection. Sends state changes,
connectivity failures, recoveries, and optional heartbeats to a Slack incoming
webhook.

The agent is deliberately read-only. A monitor failure cannot stop or alter an
OBS recording.
"""

from __future__ import annotations

import argparse
from collections.abc import Sequence
import base64
import configparser
import dataclasses
import datetime
import hashlib
import json
import logging
import logging.handlers
import math
import os
import pathlib
import socket
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid
from typing import Any, Literal, TypeAlias

try:
    import websocket
except ImportError:
    print(
        "Missing dependency 'websocket-client'. Install with: "
        "python -m pip install -r requirements.txt",
        file=sys.stderr,
    )
    raise

VERSION = "1.3.0"

Severity: TypeAlias = Literal["OK", "WARN", "CRITICAL", "INFO"]
CheckStage: TypeAlias = Literal["connect", "status"]
JsonObject: TypeAlias = dict[str, Any]

_VALID_LOG_LEVELS = {
    "DEBUG": logging.DEBUG,
    "INFO": logging.INFO,
    "WARNING": logging.WARNING,
    "ERROR": logging.ERROR,
    "CRITICAL": logging.CRITICAL,
}
_SEVERITY_ICONS: dict[Severity, str] = {
    "OK": ":large_green_circle:",
    "WARN": ":large_yellow_circle:",
    "CRITICAL": ":red_circle:",
    "INFO": ":large_blue_circle:",
}


class ConfigurationError(ValueError):
    """Raised when monitor configuration is missing or invalid."""


class ObsProtocolError(RuntimeError):
    """Raised when OBS returns malformed or unexpected protocol data."""


class ObsCheckError(RuntimeError):
    """Raised when an OBS health check fails at a known stage."""

    def __init__(self, stage: CheckStage, cause: Exception):
        self.stage = stage
        self.cause = cause
        super().__init__(f"{type(cause).__name__}: {cause}")


@dataclasses.dataclass(frozen=True, slots=True)
class RecordStatus:
    """Validated subset of OBS GetRecordStatus response data."""

    active: bool
    paused: bool
    timecode: str | None
    output_bytes: int | None

    @classmethod
    def from_response(cls, data: JsonObject) -> RecordStatus:
        """Builds a validated recording status from OBS response data.

        Args:
            data: GetRecordStatus responseData object.

        Returns:
            A validated RecordStatus.

        Raises:
            ObsProtocolError: If required fields have invalid types.
        """
        active = _require_bool(data, "outputActive")
        paused = _require_bool(data, "outputPaused")

        timecode_value = data.get("outputTimecode")
        if timecode_value is not None and not isinstance(
            timecode_value, str
        ):
            raise ObsProtocolError(
                "GetRecordStatus outputTimecode must be a string"
            )

        bytes_value = data.get("outputBytes")
        if bytes_value is not None:
            if isinstance(bytes_value, bool) or not isinstance(
                bytes_value, int
            ):
                raise ObsProtocolError(
                    "GetRecordStatus outputBytes must be an integer"
                )
            if bytes_value < 0:
                raise ObsProtocolError(
                    "GetRecordStatus outputBytes cannot be negative"
                )

        return cls(
            active=active,
            paused=paused,
            timecode=timecode_value,
            output_bytes=bytes_value,
        )


@dataclasses.dataclass(frozen=True, slots=True)
class Settings:
    """Validated monitor configuration."""

    room: str
    hostname: str

    obs_url: str
    obs_password: str
    connect_timeout_seconds: float
    request_timeout_seconds: float

    slack_webhook_url: str
    slack_timeout_seconds: float
    slack_prefix: str
    slack_mention: str
    include_hostname: bool
    include_timestamp: bool

    poll_interval_seconds: float
    reconnect_delay_seconds: float
    notification_retry_seconds: float
    failure_threshold: int
    recovery_threshold: int
    startup_grace_seconds: float
    repeat_offline_minutes: float
    repeat_not_recording_minutes: float
    heartbeat_minutes: float

    expect_recording: bool
    alert_on_startup: bool
    alert_on_connectivity: bool
    alert_on_record_start: bool
    alert_on_record_stop: bool
    alert_on_pause: bool
    alert_if_not_recording: bool

    log_level: str
    log_file: str
    log_max_bytes: int
    log_backup_count: int

    @classmethod
    def from_file(cls, path: pathlib.Path) -> Settings:
        """Loads and validates settings from an INI file.

        Environment variables override the two secret values:
        OBS_MONITOR_OBS_PASSWORD and OBS_MONITOR_SLACK_WEBHOOK_URL.

        Args:
            path: Path to the INI configuration file.

        Returns:
            Validated Settings.

        Raises:
            ConfigurationError: If the file cannot be read or has bad values.
        """
        parser = configparser.ConfigParser(interpolation=None)
        try:
            loaded = parser.read(path, encoding="utf-8")
        except (OSError, configparser.Error) as exc:
            raise ConfigurationError(
                f"Cannot read config file {path}: {exc}"
            ) from exc

        if not loaded:
            raise ConfigurationError(f"Cannot read config file: {path}")

        hostname = socket.gethostname()
        room = _get_text(
            parser,
            "identity",
            "room",
            default=hostname,
            allow_empty=False,
        )

        explicit_obs_url = _get_text(
            parser,
            "obs",
            "url",
            default="",
            allow_empty=True,
        )
        if explicit_obs_url:
            obs_url = _validate_obs_url(explicit_obs_url)
        else:
            obs_host = _get_text(
                parser,
                "obs",
                "host",
                default="127.0.0.1",
                allow_empty=False,
            )
            obs_port = _get_int(
                parser,
                "obs",
                "port",
                default=4455,
                minimum=1,
                maximum=65535,
            )
            obs_scheme = _get_text(
                parser,
                "obs",
                "scheme",
                default="ws",
                allow_empty=False,
            ).lower()
            if obs_scheme not in {"ws", "wss"}:
                raise ConfigurationError(
                    "[obs] scheme must be 'ws' or 'wss'"
                )
            obs_url = _validate_obs_url(
                f"{obs_scheme}://{obs_host}:{obs_port}"
            )

        obs_password = _secret_value(
            "OBS_MONITOR_OBS_PASSWORD",
            parser,
            "obs",
            "password",
        )
        slack_webhook_url = _secret_value(
            "OBS_MONITOR_SLACK_WEBHOOK_URL",
            parser,
            "slack",
            "webhook_url",
        ).strip()
        _validate_slack_webhook_url(slack_webhook_url)

        log_level = _get_text(
            parser,
            "logging",
            "level",
            default="INFO",
            allow_empty=False,
        ).upper()
        if log_level not in _VALID_LOG_LEVELS:
            allowed = ", ".join(_VALID_LOG_LEVELS)
            raise ConfigurationError(
                f"[logging] level must be one of: {allowed}"
            )

        settings = cls(
            room=room,
            hostname=hostname,
            obs_url=obs_url,
            obs_password=obs_password,
            connect_timeout_seconds=_get_float(
                parser,
                "obs",
                "connect_timeout_seconds",
                default=5.0,
                minimum=0.1,
            ),
            request_timeout_seconds=_get_float(
                parser,
                "obs",
                "request_timeout_seconds",
                default=5.0,
                minimum=0.1,
            ),
            slack_webhook_url=slack_webhook_url,
            slack_timeout_seconds=_get_float(
                parser,
                "slack",
                "timeout_seconds",
                default=5.0,
                minimum=0.1,
            ),
            slack_prefix=_get_text(
                parser,
                "slack",
                "prefix",
                default="OBS Monitor",
                allow_empty=False,
            ),
            slack_mention=_get_text(
                parser,
                "slack",
                "mention",
                default="",
                allow_empty=True,
            ),
            include_hostname=_get_bool(
                parser,
                "slack",
                "include_hostname",
                default=True,
            ),
            include_timestamp=_get_bool(
                parser,
                "slack",
                "include_timestamp",
                default=True,
            ),
            poll_interval_seconds=_get_float(
                parser,
                "monitoring",
                "poll_interval_seconds",
                default=5.0,
                minimum=0.1,
            ),
            reconnect_delay_seconds=_get_float(
                parser,
                "monitoring",
                "reconnect_delay_seconds",
                default=5.0,
                minimum=0.0,
            ),
            notification_retry_seconds=_get_float(
                parser,
                "monitoring",
                "notification_retry_seconds",
                default=30.0,
                minimum=1.0,
            ),
            failure_threshold=_get_int(
                parser,
                "monitoring",
                "failure_threshold",
                default=3,
                minimum=1,
            ),
            recovery_threshold=_get_int(
                parser,
                "monitoring",
                "recovery_threshold",
                default=1,
                minimum=1,
            ),
            startup_grace_seconds=_get_float(
                parser,
                "monitoring",
                "startup_grace_seconds",
                default=30.0,
                minimum=0.0,
            ),
            repeat_offline_minutes=_get_float(
                parser,
                "monitoring",
                "repeat_offline_minutes",
                default=15.0,
                minimum=0.0,
            ),
            repeat_not_recording_minutes=_get_float(
                parser,
                "monitoring",
                "repeat_not_recording_minutes",
                default=15.0,
                minimum=0.0,
            ),
            heartbeat_minutes=_get_float(
                parser,
                "monitoring",
                "heartbeat_minutes",
                default=0.0,
                minimum=0.0,
            ),
            expect_recording=_get_bool(
                parser,
                "monitoring",
                "expect_recording",
                default=True,
            ),
            alert_on_startup=_get_bool(
                parser,
                "monitoring",
                "alert_on_startup",
                default=True,
            ),
            alert_on_connectivity=_get_bool(
                parser,
                "monitoring",
                "alert_on_connectivity",
                default=True,
            ),
            alert_on_record_start=_get_bool(
                parser,
                "monitoring",
                "alert_on_record_start",
                default=True,
            ),
            alert_on_record_stop=_get_bool(
                parser,
                "monitoring",
                "alert_on_record_stop",
                default=True,
            ),
            alert_on_pause=_get_bool(
                parser,
                "monitoring",
                "alert_on_pause",
                default=True,
            ),
            alert_if_not_recording=_get_bool(
                parser,
                "monitoring",
                "alert_if_not_recording",
                default=True,
            ),
            log_level=log_level,
            log_file=_get_text(
                parser,
                "logging",
                "file",
                default="obs_slack_monitor.log",
                allow_empty=True,
            ),
            log_max_bytes=_get_int(
                parser,
                "logging",
                "max_bytes",
                default=1_048_576,
                minimum=1_024,
            ),
            log_backup_count=_get_int(
                parser,
                "logging",
                "backup_count",
                default=3,
                minimum=0,
            ),
        )
        return settings


def _get_raw(
    parser: configparser.ConfigParser,
    section: str,
    option: str,
    default: str,
) -> str:
    """Returns an INI value without interpolation."""
    try:
        return parser.get(section, option, fallback=default)
    except configparser.Error as exc:
        raise ConfigurationError(
            f"Invalid [{section}] {option}: {exc}"
        ) from exc


def _get_text(
    parser: configparser.ConfigParser,
    section: str,
    option: str,
    *,
    default: str,
    allow_empty: bool,
) -> str:
    """Returns a stripped text configuration value."""
    value = _get_raw(parser, section, option, default).strip()
    if not allow_empty and not value:
        raise ConfigurationError(
            f"[{section}] {option} cannot be empty"
        )
    return value


def _get_bool(
    parser: configparser.ConfigParser,
    section: str,
    option: str,
    *,
    default: bool,
) -> bool:
    """Returns a validated boolean configuration value."""
    if not parser.has_option(section, option):
        return default
    try:
        return parser.getboolean(section, option)
    except ValueError as exc:
        raise ConfigurationError(
            f"[{section}] {option} must be a boolean"
        ) from exc


def _get_int(
    parser: configparser.ConfigParser,
    section: str,
    option: str,
    *,
    default: int,
    minimum: int | None = None,
    maximum: int | None = None,
) -> int:
    """Returns a validated integer configuration value."""
    if not parser.has_option(section, option):
        value = default
    else:
        raw = _get_raw(parser, section, option, str(default)).strip()
        try:
            value = int(raw, 10)
        except ValueError as exc:
            raise ConfigurationError(
                f"[{section}] {option} must be an integer"
            ) from exc

    if minimum is not None and value < minimum:
        raise ConfigurationError(
            f"[{section}] {option} must be >= {minimum}"
        )
    if maximum is not None and value > maximum:
        raise ConfigurationError(
            f"[{section}] {option} must be <= {maximum}"
        )
    return value


def _get_float(
    parser: configparser.ConfigParser,
    section: str,
    option: str,
    *,
    default: float,
    minimum: float | None = None,
) -> float:
    """Returns a validated finite floating-point configuration value."""
    if not parser.has_option(section, option):
        value = default
    else:
        raw = _get_raw(parser, section, option, str(default)).strip()
        try:
            value = float(raw)
        except ValueError as exc:
            raise ConfigurationError(
                f"[{section}] {option} must be a number"
            ) from exc

    if not math.isfinite(value):
        raise ConfigurationError(
            f"[{section}] {option} must be finite"
        )
    if minimum is not None and value < minimum:
        raise ConfigurationError(
            f"[{section}] {option} must be >= {minimum}"
        )
    return value


def _secret_value(
    environment_name: str,
    parser: configparser.ConfigParser,
    section: str,
    option: str,
) -> str:
    """Returns a secret, preferring an environment variable."""
    environment_value = os.getenv(environment_name)
    if environment_value is not None:
        return environment_value
    return _get_raw(parser, section, option, "")


def _validate_obs_url(value: str) -> str:
    """Validates and returns an OBS WebSocket URL."""
    try:
        parsed = urllib.parse.urlsplit(value)
        port = parsed.port
    except ValueError as exc:
        raise ConfigurationError(f"Invalid OBS URL: {exc}") from exc

    if parsed.scheme.lower() not in {"ws", "wss"}:
        raise ConfigurationError(
            "OBS URL scheme must be ws:// or wss://"
        )
    if not parsed.hostname:
        raise ConfigurationError("OBS URL must include a hostname")
    if parsed.username is not None or parsed.password is not None:
        raise ConfigurationError(
            "OBS URL must not contain embedded credentials"
        )
    if port is not None and not 1 <= port <= 65535:
        raise ConfigurationError("OBS URL port must be 1..65535")
    if parsed.query or parsed.fragment:
        raise ConfigurationError(
            "OBS URL must not contain a query string or fragment"
        )
    if parsed.path not in {"", "/"}:
        raise ConfigurationError(
            "OBS URL path must be empty or '/'"
        )
    return value


def _validate_slack_webhook_url(value: str) -> None:
    """Validates a Slack-compatible incoming webhook URL."""
    if not value:
        raise ConfigurationError(
            "Slack webhook URL is required in [slack] webhook_url or "
            "OBS_MONITOR_SLACK_WEBHOOK_URL"
        )
    if "CHANGE/ME" in value.upper():
        raise ConfigurationError(
            "Slack webhook URL still contains the CHANGE/ME placeholder"
        )

    try:
        parsed = urllib.parse.urlsplit(value)
        _ = parsed.port
    except ValueError as exc:
        raise ConfigurationError(
            f"Invalid Slack webhook URL: {exc}"
        ) from exc

    if parsed.scheme not in {"http", "https"}:
        raise ConfigurationError(
            "Slack webhook URL must use http:// or https://"
        )
    if not parsed.hostname:
        raise ConfigurationError(
            "Slack webhook URL must include a hostname"
        )
    if parsed.username is not None or parsed.password is not None:
        raise ConfigurationError(
            "Slack webhook URL must not contain embedded credentials"
        )

    local_hosts = {"127.0.0.1", "::1", "localhost"}
    if parsed.scheme != "https" and parsed.hostname not in local_hosts:
        raise ConfigurationError(
            "Slack webhook URL must use HTTPS except for localhost tests"
        )


def _require_bool(data: JsonObject, key: str) -> bool:
    """Returns a required boolean field from a JSON object."""
    value = data.get(key)
    if not isinstance(value, bool):
        raise ObsProtocolError(f"{key} must be a boolean")
    return value


def _require_object(data: JsonObject, key: str) -> JsonObject:
    """Returns a required object field from a JSON object."""
    value = data.get(key)
    if not isinstance(value, dict):
        raise ObsProtocolError(f"{key} must be a JSON object")
    return value


def _require_nonempty_string(data: JsonObject, key: str) -> str:
    """Returns a required nonempty string field from a JSON object."""
    value = data.get(key)
    if not isinstance(value, str) or not value:
        raise ObsProtocolError(f"{key} must be a nonempty string")
    return value


def _local_timestamp() -> str:
    """Returns the current local time as ISO-8601."""
    return (
        datetime.datetime.now(datetime.timezone.utc)
        .astimezone()
        .isoformat(timespec="seconds")
    )


def _format_bytes(value: int | None) -> str:
    """Formats a byte count for human-readable Slack output."""
    if value is None:
        return "unknown"

    amount = float(value)
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if amount < 1024.0 or unit == "TB":
            return f"{amount:.1f} {unit}"
        amount /= 1024.0
    return f"{value} B"


def configure_logging(
    settings: Settings,
    config_path: pathlib.Path,
) -> logging.Logger:
    """Configures console and optional rotating-file logging.

    Args:
        settings: Validated monitor settings.
        config_path: Config file path, used to resolve relative log paths.

    Returns:
        Configured application logger.

    Raises:
        OSError: If the log directory or file cannot be created.
    """
    logger = logging.getLogger("obs_slack_monitor")
    for handler in logger.handlers[:]:
        logger.removeHandler(handler)
        handler.close()

    logger.propagate = False
    level = _VALID_LOG_LEVELS[settings.log_level]
    logger.setLevel(level)

    formatter = logging.Formatter(
        "%(asctime)s %(levelname)s %(message)s"
    )
    console_handler = logging.StreamHandler()
    console_handler.setLevel(level)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    if settings.log_file:
        log_path = pathlib.Path(settings.log_file)
        if not log_path.is_absolute():
            log_path = config_path.parent / log_path
        if log_path.exists() and log_path.is_dir():
            raise OSError(f"Log path is a directory: {log_path}")
        log_path.parent.mkdir(parents=True, exist_ok=True)

        file_handler = logging.handlers.RotatingFileHandler(
            log_path,
            maxBytes=settings.log_max_bytes,
            backupCount=settings.log_backup_count,
            encoding="utf-8",
            errors="backslashreplace",
        )
        file_handler.setLevel(level)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

    return logger


class SlackNotifier:
    """Posts formatted notifications to a Slack incoming webhook."""

    def __init__(
        self,
        settings: Settings,
        logger: logging.Logger,
    ):
        self.settings = settings
        self.logger = logger

    def _format(self, severity: Severity, message: str) -> str:
        parts = []
        if self.settings.slack_mention:
            parts.append(self.settings.slack_mention)
        parts.append(_SEVERITY_ICONS[severity])

        identity = self.settings.room
        if (
            self.settings.include_hostname
            and self.settings.hostname.lower()
            != self.settings.room.lower()
        ):
            identity += f"/{self.settings.hostname}"

        parts.append(
            f"*{self.settings.slack_prefix} [{identity}]*"
        )
        parts.append(f"*{severity}:* {message}")

        if self.settings.include_timestamp:
            parts.append(f"_{_local_timestamp()}_")
        return " ".join(parts)

    def send(self, severity: Severity, message: str) -> bool:
        """Sends one notification.

        Args:
            severity: Notification severity.
            message: Human-readable message.

        Returns:
            True if Slack accepted the request, otherwise False.
        """
        text = self._format(severity, message)
        payload = json.dumps({"text": text}).encode("utf-8")
        request = urllib.request.Request(
            self.settings.slack_webhook_url,
            data=payload,
            headers={
                "Content-Type": "application/json",
                "User-Agent": f"obs-slack-monitor/{VERSION}",
            },
            method="POST",
        )

        try:
            with urllib.request.urlopen(
                request,
                timeout=self.settings.slack_timeout_seconds,
            ) as response:
                body = (
                    response.read(4096)
                    .decode("utf-8", "replace")
                    .strip()
                )
                if not 200 <= response.status < 300:
                    self.logger.error(
                        "Slack webhook returned HTTP %s body=%r",
                        response.status,
                        body,
                    )
                    return False
                if body not in {"", "ok"}:
                    self.logger.error(
                        "Slack webhook returned unexpected body=%r",
                        body,
                    )
                    return False
        except urllib.error.HTTPError as exc:
            try:
                body = (
                    exc.read(4096)
                    .decode("utf-8", "replace")
                    .strip()
                )
            except OSError:
                body = "<unavailable>"
            self.logger.error(
                "Slack webhook returned HTTP %s body=%r",
                exc.code,
                body,
            )
            return False
        except urllib.error.URLError as exc:
            self.logger.error(
                "Slack webhook connection failed: %s",
                exc.reason,
            )
            return False
        except (TimeoutError, OSError) as exc:
            self.logger.error(
                "Slack webhook send failed: %s",
                exc,
            )
            return False

        self.logger.info(
            "Slack notification sent: %s - %s",
            severity,
            message,
        )
        return True


class ObsClient:
    """Minimal validated obs-websocket 5.x JSON client."""

    def __init__(
        self,
        settings: Settings,
        logger: logging.Logger,
    ):
        self.settings = settings
        self.logger = logger
        self.websocket: websocket.WebSocket | None = None
        self.obs_version: str | None = None
        self.websocket_version: str | None = None

    @staticmethod
    def _auth_response(
        password: str,
        salt: str,
        challenge: str,
    ) -> str:
        secret = base64.b64encode(
            hashlib.sha256(
                (password + salt).encode("utf-8")
            ).digest()
        )
        return base64.b64encode(
            hashlib.sha256(
                secret + challenge.encode("utf-8")
            ).digest()
        ).decode("utf-8")

    def connect(self) -> None:
        """Connects and performs the obs-websocket Hello/Identify handshake."""
        self.close()
        self.logger.debug(
            "Connecting to OBS WebSocket %s",
            self.settings.obs_url,
        )

        connection = websocket.create_connection(
            self.settings.obs_url,
            timeout=self.settings.connect_timeout_seconds,
            subprotocols=["obswebsocket.json"],
        )
        self.websocket = connection

        try:
            connection.settimeout(
                self.settings.request_timeout_seconds
            )
            hello = self._recv_json()
            if hello.get("op") != 0:
                raise ObsProtocolError(
                    f"Expected OBS Hello (op 0), got: {hello!r}"
                )

            hello_data = _require_object(hello, "d")
            server_rpc = hello_data.get("rpcVersion")
            if (
                isinstance(server_rpc, bool)
                or not isinstance(server_rpc, int)
                or server_rpc < 1
            ):
                raise ObsProtocolError(
                    "OBS Hello rpcVersion must be an integer >= 1"
                )

            obs_version = hello_data.get("obsStudioVersion")
            websocket_version = hello_data.get(
                "obsWebSocketVersion"
            )
            if obs_version is not None and not isinstance(
                obs_version, str
            ):
                raise ObsProtocolError(
                    "OBS Hello obsStudioVersion must be a string"
                )
            if websocket_version is not None and not isinstance(
                websocket_version, str
            ):
                raise ObsProtocolError(
                    "OBS Hello obsWebSocketVersion must be a string"
                )

            self.obs_version = obs_version
            self.websocket_version = websocket_version

            identify_data: JsonObject = {
                "rpcVersion": 1,
                "eventSubscriptions": 0,
            }
            auth = hello_data.get("authentication")
            if auth is not None:
                if not isinstance(auth, dict):
                    raise ObsProtocolError(
                        "OBS Hello authentication must be an object"
                    )
                if not self.settings.obs_password:
                    raise ObsProtocolError(
                        "OBS requires authentication, but no password "
                        "is configured"
                    )
                salt = _require_nonempty_string(auth, "salt")
                challenge = _require_nonempty_string(
                    auth,
                    "challenge",
                )
                identify_data["authentication"] = (
                    self._auth_response(
                        self.settings.obs_password,
                        salt,
                        challenge,
                    )
                )

            self._send_json({"op": 1, "d": identify_data})
            identified = self._recv_json()
            if identified.get("op") != 2:
                raise ObsProtocolError(
                    f"OBS identification failed: {identified!r}"
                )

            identified_data = _require_object(identified, "d")
            negotiated_rpc = identified_data.get(
                "negotiatedRpcVersion"
            )
            if (
                isinstance(negotiated_rpc, bool)
                or not isinstance(negotiated_rpc, int)
                or negotiated_rpc != 1
            ):
                raise ObsProtocolError(
                    "OBS negotiated an unsupported RPC version"
                )
        except Exception:
            self.close()
            raise

        self.logger.info(
            "Connected to OBS %s (obs-websocket %s)",
            self.obs_version or "unknown",
            self.websocket_version or "unknown",
        )

    def close(self) -> None:
        """Closes the current WebSocket connection, if any."""
        connection = self.websocket
        self.websocket = None
        if connection is None:
            return
        try:
            connection.close()
        except (OSError, websocket.WebSocketException) as exc:
            self.logger.debug(
                "Error while closing OBS WebSocket: %s",
                exc,
            )

    def _send_json(self, payload: JsonObject) -> None:
        connection = self.websocket
        if connection is None:
            raise ObsProtocolError(
                "OBS WebSocket is not connected"
            )
        connection.send(
            json.dumps(payload, separators=(",", ":"))
        )

    def _recv_json(
        self,
        *,
        timeout_seconds: float | None = None,
    ) -> JsonObject:
        connection = self.websocket
        if connection is None:
            raise ObsProtocolError(
                "OBS WebSocket is not connected"
            )
        if timeout_seconds is not None:
            connection.settimeout(timeout_seconds)

        raw = connection.recv()
        if isinstance(raw, bytes):
            try:
                raw = raw.decode("utf-8")
            except UnicodeDecodeError as exc:
                raise ObsProtocolError(
                    "OBS sent invalid UTF-8"
                ) from exc
        if not isinstance(raw, str):
            raise ObsProtocolError(
                "OBS sent an unexpected WebSocket frame type"
            )

        try:
            parsed = json.loads(raw)
        except json.JSONDecodeError as exc:
            raise ObsProtocolError(
                "OBS sent invalid JSON"
            ) from exc
        if not isinstance(parsed, dict):
            raise ObsProtocolError(
                "OBS sent a non-object JSON message"
            )
        return parsed

    def request(
        self,
        request_type: str,
        request_data: JsonObject | None = None,
    ) -> JsonObject:
        """Sends one OBS request and returns validated responseData."""
        request_id = uuid.uuid4().hex
        payload: JsonObject = {
            "op": 6,
            "d": {
                "requestType": request_type,
                "requestId": request_id,
            },
        }
        if request_data is not None:
            payload["d"]["requestData"] = request_data

        self._send_json(payload)
        deadline = (
            time.monotonic()
            + self.settings.request_timeout_seconds
        )

        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise TimeoutError(
                    f"Timed out waiting for OBS {request_type}"
                )

            response = self._recv_json(
                timeout_seconds=remaining
            )
            opcode = response.get("op")
            if opcode == 5:
                continue
            if opcode != 7:
                raise ObsProtocolError(
                    f"Unexpected OBS opcode {opcode!r} "
                    f"while waiting for {request_type}"
                )

            data = _require_object(response, "d")
            if data.get("requestId") != request_id:
                continue

            request_status = _require_object(
                data,
                "requestStatus",
            )
            result = request_status.get("result")
            if not isinstance(result, bool):
                raise ObsProtocolError(
                    "OBS requestStatus.result must be boolean"
                )
            if not result:
                code = request_status.get("code")
                comment = request_status.get("comment", "")
                raise ObsProtocolError(
                    f"OBS request {request_type} failed: "
                    f"code={code!r} comment={comment!r}"
                )

            response_data = data.get("responseData", {})
            if response_data is None:
                return {}
            if not isinstance(response_data, dict):
                raise ObsProtocolError(
                    "OBS responseData must be a JSON object"
                )
            return response_data

    def get_record_status(self) -> RecordStatus:
        """Returns the validated current OBS recording state."""
        return RecordStatus.from_response(
            self.request("GetRecordStatus")
        )


class Monitor:
    """Coordinates OBS checks, state tracking, and Slack notifications."""

    def __init__(
        self,
        settings: Settings,
        logger: logging.Logger,
    ):
        self.settings = settings
        self.logger = logger
        self.slack = SlackNotifier(settings, logger)
        self.obs = ObsClient(settings, logger)

        self._started_at = time.monotonic()
        self._online: bool | None = None
        self._recording: bool | None = None
        self._paused: bool | None = None

        self._failure_count = 0
        self._success_count = 0
        self._startup_notice_processed = False
        self._last_success_at: float | None = None

        self._last_offline_sent = 0.0
        self._last_offline_attempt = 0.0
        self._last_not_recording_sent = 0.0
        self._last_not_recording_attempt = 0.0
        self._last_heartbeat_sent = 0.0
        self._last_heartbeat_attempt = 0.0

    def _status_summary(self, status: RecordStatus) -> str:
        parts = [
            f"recording={'YES' if status.active else 'NO'}"
        ]
        if status.active:
            parts.append(
                f"paused={'YES' if status.paused else 'NO'}"
            )
            if status.timecode:
                parts.append(f"time={status.timecode}")
            if status.output_bytes is not None:
                parts.append(
                    f"size={_format_bytes(status.output_bytes)}"
                )
        return ", ".join(parts)

    def _overall_severity(
        self,
        status: RecordStatus,
    ) -> Severity:
        if self.settings.expect_recording and not status.active:
            return "CRITICAL"
        if status.active and status.paused:
            return "WARN"
        return "OK"

    def _state_message(
        self,
        status: RecordStatus,
        ok_prefix: str,
        problem_prefix: str,
    ) -> tuple[Severity, str]:
        severity = self._overall_severity(status)
        prefix = (
            ok_prefix if severity == "OK" else problem_prefix
        )
        return (
            severity,
            f"{prefix}; {self._status_summary(status)}",
        )

    def _can_retry_notification(
        self,
        now: float,
        last_attempt: float,
    ) -> bool:
        if last_attempt == 0.0:
            return True
        return (
            now - last_attempt
            >= self.settings.notification_retry_seconds
        )

    def _notify_startup(
        self,
        status: RecordStatus,
        now: float,
    ) -> None:
        if self._startup_notice_processed:
            return
        self._startup_notice_processed = True

        if not self.settings.alert_on_startup:
            self._last_heartbeat_sent = now
            return

        version = self.obs.obs_version or "unknown"
        sent = self.slack.send(
            "INFO",
            f"Agent {VERSION} started; OBS {version} reachable; "
            f"{self._status_summary(status)}",
        )
        self._last_heartbeat_attempt = now
        if sent:
            self._last_heartbeat_sent = now

    def _notify_not_recording(
        self,
        now: float,
        *,
        immediate: bool = False,
    ) -> None:
        if not self.settings.alert_if_not_recording:
            return

        if self._last_not_recording_sent == 0.0:
            due = immediate or self._can_retry_notification(
                now,
                self._last_not_recording_attempt,
            )
        elif self.settings.repeat_not_recording_minutes > 0:
            due = (
                now - self._last_not_recording_sent
                >= self.settings.repeat_not_recording_minutes * 60
            )
        else:
            due = False

        if not due:
            return
        if not self._can_retry_notification(
            now,
            self._last_not_recording_attempt,
        ):
            return

        self._last_not_recording_attempt = now
        if self.slack.send(
            "CRITICAL",
            "OBS is reachable but is NOT recording",
        ):
            self._last_not_recording_sent = now

    def _notify_heartbeat(
        self,
        status: RecordStatus,
        now: float,
    ) -> None:
        if self.settings.heartbeat_minutes <= 0:
            return

        if self._last_heartbeat_sent > 0.0:
            due = (
                now - self._last_heartbeat_sent
                >= self.settings.heartbeat_minutes * 60
            )
        else:
            due = True
        if not due:
            return
        if not self._can_retry_notification(
            now,
            self._last_heartbeat_attempt,
        ):
            return

        severity = self._overall_severity(status)
        summary = self._status_summary(status)
        if severity == "OK":
            message = (
                "Healthy heartbeat: agent running; OBS reachable; "
                f"{summary}"
            )
        elif severity == "WARN":
            message = (
                "Heartbeat: agent running; OBS reachable, but "
                f"recording is PAUSED; {summary}"
            )
        else:
            message = (
                "Heartbeat: agent running; OBS reachable, but "
                f"expected recording is NOT active; {summary}"
            )

        self._last_heartbeat_attempt = now
        if self.slack.send(severity, message):
            self._last_heartbeat_sent = now

    def _handle_online(self, status: RecordStatus) -> None:
        now = time.monotonic()
        self._last_success_at = now
        self._failure_count = 0
        self._success_count = min(
            self._success_count + 1,
            self.settings.recovery_threshold,
        )

        recovered = (
            self._online is False
            and self._success_count
            >= self.settings.recovery_threshold
        )
        if recovered:
            self._online = True
            if self.settings.alert_on_connectivity:
                severity, message = self._state_message(
                    status,
                    "OBS WebSocket connection recovered",
                    "OBS WebSocket connection recovered, but "
                    "recorder state is not healthy",
                )
                if self.slack.send(severity, message):
                    self._last_heartbeat_sent = now
                self._last_heartbeat_attempt = now
        elif self._online is None:
            self._online = True

        self._notify_startup(status, now)

        if self._recording is None:
            self._recording = status.active
        elif status.active != self._recording:
            was_recording = self._recording
            self._recording = status.active

            if (
                status.active
                and not was_recording
                and self.settings.alert_on_record_start
            ):
                if self.slack.send(
                    "OK",
                    "Recording STARTED; "
                    f"{self._status_summary(status)}",
                ):
                    self._last_heartbeat_sent = now
            elif (
                was_recording
                and not status.active
                and self.settings.alert_on_record_stop
            ):
                severity: Severity = (
                    "CRITICAL"
                    if self.settings.expect_recording
                    else "WARN"
                )
                self._last_not_recording_attempt = now
                if self.slack.send(
                    severity,
                    "Recording STOPPED",
                ):
                    self._last_not_recording_sent = now

        if self._paused is None:
            self._paused = status.paused
        elif status.paused != self._paused:
            self._paused = status.paused
            if self.settings.alert_on_pause:
                severity = "WARN" if status.paused else "OK"
                state = (
                    "PAUSED" if status.paused else "RESUMED"
                )
                if self.slack.send(
                    severity,
                    f"Recording {state}",
                ):
                    self._last_heartbeat_sent = now

        grace_elapsed = (
            now - self._started_at
            >= self.settings.startup_grace_seconds
        )
        if (
            self.settings.expect_recording
            and grace_elapsed
            and not status.active
        ):
            self._notify_not_recording(now)
        elif status.active:
            self._last_not_recording_sent = 0.0
            self._last_not_recording_attempt = 0.0

        self._notify_heartbeat(status, now)

    def _handle_failure(self, error: ObsCheckError) -> None:
        now = time.monotonic()
        self._success_count = 0
        self._failure_count += 1
        self.obs.close()

        self.logger.warning(
            "OBS %s check failed (%d/%d): %s",
            error.stage,
            self._failure_count,
            self.settings.failure_threshold,
            error.cause,
        )

        if self._failure_count < self.settings.failure_threshold:
            return

        first_offline = self._online is not False
        self._online = False

        if not self.settings.alert_on_connectivity:
            return

        if self._last_offline_sent == 0.0:
            due = first_offline or self._can_retry_notification(
                now,
                self._last_offline_attempt,
            )
        elif self.settings.repeat_offline_minutes > 0:
            due = (
                now - self._last_offline_sent
                >= self.settings.repeat_offline_minutes * 60
            )
        else:
            due = False

        if not due:
            return
        if not self._can_retry_notification(
            now,
            self._last_offline_attempt,
        ):
            return

        if error.stage == "connect":
            issue = (
                "Cannot connect/authenticate to the OBS WebSocket"
            )
        else:
            issue = (
                "OBS WebSocket was connected, but "
                "GetRecordStatus failed"
            )

        if self._last_success_at is None:
            last_ok = (
                "no successful OBS status check has occurred "
                "since the agent started"
            )
        else:
            age = max(0, int(now - self._last_success_at))
            if age < 60:
                age_text = f"{age}s"
            else:
                minutes, seconds = divmod(age, 60)
                age_text = f"{minutes}m {seconds}s"
            last_ok = (
                "last successful OBS status check was "
                f"{age_text} ago"
            )

        self._last_offline_attempt = now
        sent = self.slack.send(
            "CRITICAL",
            f"{issue} after {self._failure_count} consecutive "
            f"failed checks; {last_ok}. Error: "
            f"{type(error.cause).__name__}: {error.cause}",
        )
        if sent:
            self._last_offline_sent = now

    def check_once(self) -> RecordStatus:
        """Performs one OBS check and updates monitor state.

        Raises:
            ObsCheckError: If connection or status retrieval fails.
        """
        if self.obs.websocket is None:
            try:
                self.obs.connect()
            except Exception as exc:
                raise ObsCheckError("connect", exc) from exc

        try:
            status = self.obs.get_record_status()
        except Exception as exc:
            raise ObsCheckError("status", exc) from exc

        self._handle_online(status)
        return status

    def run(self) -> int:
        """Runs the monitor until interrupted or an internal error occurs."""
        self.logger.info(
            "OBS Slack Monitor %s starting for room=%s endpoint=%s",
            VERSION,
            self.settings.room,
            self.settings.obs_url,
        )
        try:
            while True:
                started = time.monotonic()
                try:
                    self.check_once()
                except ObsCheckError as exc:
                    self._handle_failure(exc)

                elapsed = time.monotonic() - started
                delay = (
                    self.settings.poll_interval_seconds - elapsed
                )
                if self.obs.websocket is None:
                    delay = max(
                        delay,
                        self.settings.reconnect_delay_seconds,
                    )
                time.sleep(max(0.1, delay))
        except KeyboardInterrupt:
            self.logger.info("Stopped by user")
            return 0
        finally:
            self.obs.close()


def parse_args(
    argv: Sequence[str] | None = None,
) -> argparse.Namespace:
    """Parses command-line arguments."""
    parser = argparse.ArgumentParser(
        description=(
            "Monitor OBS recording status and post alerts to Slack"
        )
    )
    parser.add_argument(
        "--config",
        type=pathlib.Path,
        default=pathlib.Path("config.ini"),
        help="Path to INI config file (default: config.ini)",
    )

    mode = parser.add_mutually_exclusive_group()
    mode.add_argument(
        "--once",
        action="store_true",
        help="Query OBS once, print status, then exit",
    )
    mode.add_argument(
        "--test-slack",
        action="store_true",
        help="Send a test Slack message and exit",
    )
    mode.add_argument(
        "--validate-config",
        action="store_true",
        help="Validate configuration and exit",
    )

    parser.add_argument(
        "--version",
        action="version",
        version=f"%(prog)s {VERSION}",
    )
    return parser.parse_args(argv)


def _print_status(status: RecordStatus) -> None:
    """Prints a one-shot status as JSON."""
    data = {
        "outputActive": status.active,
        "outputPaused": status.paused,
        "outputTimecode": status.timecode,
        "outputBytes": status.output_bytes,
    }
    print(json.dumps(data, indent=2, sort_keys=True))


def main(
    argv: Sequence[str] | None = None,
) -> int:
    """Runs the command-line program."""
    args = parse_args(argv)
    config_path = args.config.expanduser().resolve()

    try:
        settings = Settings.from_file(config_path)
    except ConfigurationError as exc:
        print(f"Configuration error: {exc}", file=sys.stderr)
        return 2

    if args.validate_config:
        print(
            f"Configuration OK: room={settings.room!r}, "
            f"OBS={settings.obs_url}, "
            f"heartbeat={settings.heartbeat_minutes:g}m"
        )
        return 0

    try:
        logger = configure_logging(settings, config_path)
    except OSError as exc:
        print(
            f"Logging configuration error: {exc}",
            file=sys.stderr,
        )
        return 2

    monitor = Monitor(settings, logger)

    if args.test_slack:
        ok = monitor.slack.send(
            "INFO",
            f"Slack webhook test from OBS monitor agent {VERSION}",
        )
        return 0 if ok else 1

    if args.once:
        try:
            status = monitor.check_once()
            _print_status(status)
            return 0
        except ObsCheckError as exc:
            logger.error(
                "One-shot OBS check failed during %s: %s",
                exc.stage,
                exc.cause,
            )
            return 1
        finally:
            monitor.obs.close()

    try:
        return monitor.run()
    except Exception:
        logger.exception(
            "Unexpected internal error; exiting so the task "
            "supervisor can restart the agent"
        )
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
