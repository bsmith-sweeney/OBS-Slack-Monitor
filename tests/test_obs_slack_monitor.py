"""Tests for obs_slack_monitor."""

from __future__ import annotations

import configparser
import importlib.util
import io
import logging
import pathlib
import sys
import tempfile
import unittest
from unittest import mock

MODULE_PATH = (
    pathlib.Path(__file__).resolve().parents[1]
    / "obs_slack_monitor.py"
)
SPEC = importlib.util.spec_from_file_location(
    "obs_slack_monitor",
    MODULE_PATH,
)
assert SPEC is not None
assert SPEC.loader is not None
monitor = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = monitor
SPEC.loader.exec_module(monitor)


def _write_config(directory: pathlib.Path, extra: str = "") -> pathlib.Path:
    path = directory / "config.ini"
    path.write_text(
        """
[identity]
room = Track 1

[obs]
host = 127.0.0.1
port = 4455
scheme = ws
password = test

[slack]
webhook_url = https://hooks.slack.com/services/T/B/X

[monitoring]
heartbeat_minutes = 5
repeat_not_recording_minutes = 0

[logging]
level = INFO
file = monitor.log
"""
        + extra,
        encoding="utf-8",
    )
    return path


class SettingsTest(unittest.TestCase):

    def test_valid_config_loads(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = _write_config(pathlib.Path(directory))
            settings = monitor.Settings.from_file(path)

        self.assertEqual(settings.room, "Track 1")
        self.assertEqual(settings.obs_url, "ws://127.0.0.1:4455")
        self.assertEqual(settings.heartbeat_minutes, 5.0)

    def test_bad_boolean_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = _write_config(
                pathlib.Path(directory),
                "\n[bad]\n",
            )
            parser = configparser.ConfigParser()
            parser.read_dict(
                {"x": {"flag": "definitely"}}
            )
            with self.assertRaises(monitor.ConfigurationError):
                monitor._get_bool(
                    parser,
                    "x",
                    "flag",
                    default=False,
                )

    def test_bad_obs_port_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = pathlib.Path(directory) / "config.ini"
            path.write_text(
                """
[obs]
host = 127.0.0.1
port = 70000

[slack]
webhook_url = https://hooks.slack.com/services/T/B/X
""",
                encoding="utf-8",
            )
            with self.assertRaises(monitor.ConfigurationError):
                monitor.Settings.from_file(path)

    def test_remote_http_webhook_is_rejected(self) -> None:
        with self.assertRaises(monitor.ConfigurationError):
            monitor._validate_slack_webhook_url(
                "http://example.com/webhook"
            )

    def test_local_http_webhook_is_allowed(self) -> None:
        monitor._validate_slack_webhook_url(
            "http://127.0.0.1:8080/webhook"
        )

    def test_nan_is_rejected(self) -> None:
        parser = configparser.ConfigParser()
        parser.read_dict({"x": {"value": "nan"}})
        with self.assertRaises(monitor.ConfigurationError):
            monitor._get_float(
                parser,
                "x",
                "value",
                default=1.0,
            )


class RecordStatusTest(unittest.TestCase):

    def test_valid_status(self) -> None:
        status = monitor.RecordStatus.from_response(
            {
                "outputActive": True,
                "outputPaused": False,
                "outputTimecode": "00:01:02.003",
                "outputBytes": 1234,
            }
        )
        self.assertTrue(status.active)
        self.assertFalse(status.paused)
        self.assertEqual(status.output_bytes, 1234)

    def test_string_false_is_not_accepted_as_boolean(self) -> None:
        with self.assertRaises(monitor.ObsProtocolError):
            monitor.RecordStatus.from_response(
                {
                    "outputActive": "false",
                    "outputPaused": False,
                }
            )


class MonitorSeverityTest(unittest.TestCase):

    def _settings(self) -> monitor.Settings:
        with tempfile.TemporaryDirectory() as directory:
            path = _write_config(pathlib.Path(directory))
            return monitor.Settings.from_file(path)

    def test_severity_reflects_recording_state(self) -> None:
        instance = monitor.Monitor(
            self._settings(),
            logging.getLogger("test-severity"),
        )
        self.assertEqual(
            instance._overall_severity(
                monitor.RecordStatus(
                    active=True,
                    paused=False,
                    timecode=None,
                    output_bytes=None,
                )
            ),
            "OK",
        )
        self.assertEqual(
            instance._overall_severity(
                monitor.RecordStatus(
                    active=True,
                    paused=True,
                    timecode=None,
                    output_bytes=None,
                )
            ),
            "WARN",
        )
        self.assertEqual(
            instance._overall_severity(
                monitor.RecordStatus(
                    active=False,
                    paused=False,
                    timecode=None,
                    output_bytes=None,
                )
            ),
            "CRITICAL",
        )


class FakeWebSocket:

    def __init__(self, messages: list[str]):
        self._messages = iter(messages)
        self.sent: list[str] = []
        self.timeout: float | None = None
        self.closed = False

    def settimeout(self, timeout: float) -> None:
        self.timeout = timeout

    def recv(self) -> str:
        return next(self._messages)

    def send(self, value: str) -> None:
        self.sent.append(value)

    def close(self) -> None:
        self.closed = True


class ObsClientTest(unittest.TestCase):

    def test_handshake_and_status_request(self) -> None:
        fake = FakeWebSocket(
            [
                '{"op":0,"d":{"obsStudioVersion":"32.2.2",'
                '"obsWebSocketVersion":"5.6.0","rpcVersion":1}}',
                '{"op":2,"d":{"negotiatedRpcVersion":1}}',
                '{"op":7,"d":{"requestId":"PLACEHOLDER",'
                '"requestStatus":{"result":true,"code":100},'
                '"responseData":{"outputActive":true,'
                '"outputPaused":false,"outputBytes":42}}}',
            ]
        )

        settings = self._settings()
        logger = logging.getLogger("test-obs-client")
        client = monitor.ObsClient(settings, logger)

        with mock.patch.object(
            monitor.websocket,
            "create_connection",
            return_value=fake,
        ):
            client.connect()

            original_send = client._send_json

            def send_and_patch_id(payload):
                original_send(payload)
                if payload.get("op") == 6:
                    request_id = payload["d"]["requestId"]
                    remaining = list(fake._messages)
                    patched = remaining[0].replace(
                        "PLACEHOLDER",
                        request_id,
                    )
                    fake._messages = iter([patched])

            client._send_json = send_and_patch_id
            status = client.get_record_status()

        self.assertTrue(status.active)
        self.assertFalse(status.paused)
        self.assertEqual(status.output_bytes, 42)

    def _settings(self) -> monitor.Settings:
        with tempfile.TemporaryDirectory() as directory:
            path = _write_config(pathlib.Path(directory))
            return monitor.Settings.from_file(path)


class SlackNotifierTest(unittest.TestCase):

    def test_http_error_returns_false(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            settings = monitor.Settings.from_file(
                _write_config(pathlib.Path(directory))
            )

        notifier = monitor.SlackNotifier(
            settings,
            logging.getLogger("test-slack"),
        )
        error = monitor.urllib.error.HTTPError(
            settings.slack_webhook_url,
            403,
            "Forbidden",
            hdrs=None,
            fp=io.BytesIO(b"action_prohibited"),
        )

        with mock.patch.object(
            monitor.urllib.request,
            "urlopen",
            side_effect=error,
        ):
            self.assertFalse(
                notifier.send("INFO", "test")
            )


if __name__ == "__main__":
    unittest.main()
