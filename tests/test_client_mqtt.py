import builtins
import unittest
from unittest.mock import Mock

import ecdsa

import client_mqtt
from multi_mqtt import get_standard_pem_bytes


class ClientMqttTests(unittest.TestCase):
    def test_fallback_input_ignores_invalid_python(self):
        answers = [
            "/workspaces/build_xime_home/git.py push https://example.com/repo -m",
            "print(123)",
        ]
        original_input = builtins.input
        builtins.input = lambda prompt="": answers.pop(0)
        try:
            result = client_mqtt._fallback_code_input()
            self.assertEqual(result, "print(123)")
        finally:
            builtins.input = original_input

    def test_request_uses_configured_private_key_by_default(self):
        node = client_mqtt.MQTTClientNode(client_private_key_bytes="2333")
        self.assertIsNotNone(node.mqtt_net.client_private_key_bytes)

    def test_expression_private_key_is_supported(self):
        pem = get_standard_pem_bytes("1+1")
        self.assertIn(b"BEGIN EC PRIVATE KEY", pem)

    def test_signed_request_is_blocked_when_server_pubkey_is_missing(self):
        node = client_mqtt.MQTTClientNode(
            client_private_key_bytes="2333",
        )
        event = Mock()
        req_id = "req-123|deadbeef"
        node.pending_requests["req-123"] = {
            "event": event,
            "start_time": 0.0,
            "response": None,
            "client_private_key_bytes": "1+1",
            "allow_no_server_pubkey_response": False,
        }

        node._on_message("sys/device/response", {"req_id": req_id, "ok": True}, "mqtt.emqx.io")

        self.assertFalse(event.set.called)
        self.assertIsNone(node.pending_requests["req-123"]["response"])

    def test_signed_request_is_accepted_when_server_pubkey_is_known(self):
        server_key = ecdsa.SigningKey.generate(curve=ecdsa.NIST256p).verifying_key.to_pem()
        node = client_mqtt.MQTTClientNode(
            client_private_key_bytes="2333",
            server_public_key_bytes=server_key,
        )
        event = Mock()
        req_id = "req-123|deadbeef"
        node.pending_requests["req-123"] = {
            "event": event,
            "start_time": 0.0,
            "response": None,
            "client_private_key_bytes": "1+1",
            "allow_no_server_pubkey_response": False,
        }

        node._on_message("sys/device/response", {"req_id": req_id, "ok": True}, "mqtt.emqx.io")

        self.assertTrue(event.set.called)
        self.assertEqual(node.pending_requests["req-123"]["response"]["req_id"], "req-123")

    def test_signed_request_is_accepted_when_response_req_id_is_clean(self):
        node = client_mqtt.MQTTClientNode(
            client_private_key_bytes="2333",
        )
        event = Mock()
        req_id = "req-123"
        node.pending_requests[req_id] = {
            "event": event,
            "start_time": 0.0,
            "response": None,
            "client_private_key_bytes": "1+1",
            "allow_no_server_pubkey_response": False,
        }

        node._on_message("sys/device/response", {"req_id": req_id, "ok": True}, "mqtt.emqx.io")

        self.assertTrue(event.set.called)
        self.assertIsNotNone(node.pending_requests[req_id]["response"])

    def test_signed_request_with_allow_flag_accepts_no_pubkey_response(self):
        node = client_mqtt.MQTTClientNode(
            client_private_key_bytes="2333",
        )
        event = Mock()
        req_id = "req-123|deadbeef"
        node.pending_requests["req-123"] = {
            "event": event,
            "start_time": 0.0,
            "response": None,
            "client_private_key_bytes": "1+1",
            "allow_no_server_pubkey_response": True,
        }

        node._on_message("sys/device/response", {"req_id": req_id, "ok": True}, "mqtt.emqx.io")

        self.assertTrue(event.set.called)
        self.assertEqual(node.pending_requests["req-123"]["response"]["req_id"], "req-123")

    def test_request_uses_client_allow_flag_by_default(self):
        node = client_mqtt.MQTTClientNode(
            client_private_key_bytes="2333",
            allow_no_server_pubkey_response=True,
        )
        node.mqtt_net.publish_broadcast = Mock()

        req_id = "req-999"
        node.pending_requests[req_id] = {
            "event": Mock(),
            "start_time": 0.0,
            "response": None,
            "client_private_key_bytes": "1+1",
            "allow_no_server_pubkey_response": True,
        }

        node._on_message(
            "sys/device/response",
            {"req_id": "req-999|deadbeef", "ok": True},
            "mqtt.emqx.io",
        )

        self.assertTrue(node.pending_requests[req_id]["response"]["req_id"] == "req-999")

    def test_request_interrupt_returns_none_cleanly(self):
        node = client_mqtt.MQTTClientNode(client_private_key_bytes="2333")
        node.mqtt_net.publish_broadcast = Mock(side_effect=RuntimeError("boom"))

        result = node.request("print(1)", timeout=0.1)
        self.assertIsNone(result)


if __name__ == "__main__":
    unittest.main()
