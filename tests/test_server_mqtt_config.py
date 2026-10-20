import ast
import json
import sys
import tempfile
import unittest
import urllib.parse
import urllib.request
from pathlib import Path
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import server_mqtt


class FakeMQTTManager:
    instances = []

    def __init__(self, brokers, server_public_key_bytes=None, **_kwargs):
        self.brokers = brokers
        self.server_public_key_bytes = server_public_key_bytes
        self.server_vk = None
        self._server_vk_invalid = False
        self.clients = {}
        self.subscribed_topics = set()
        self.subscribed = []
        self.unsubscribed = []
        self.stopped = False
        self.callback = None
        self.__class__.instances.append(self)

    def set_on_message(self, callback):
        self.callback = callback

    def start(self):
        self.clients = {"fake-broker": object()}

    def subscribe(self, topic):
        self.subscribed_topics.add(topic)
        self.subscribed.append(topic)

    def publish_broadcast(self, _topic, _payload, **_kwargs):
        pass

    def unsubscribe(self, topic):
        self.subscribed_topics.discard(topic)
        self.unsubscribed.append(topic)

    def set_periodic_task(self, _fn, interval=60.0):
        self.periodic_interval = interval

    def stop(self):
        self.stopped = True
        self.clients = {}


class ServerMqttConfigTests(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.config_path = Path(self.temp_dir.name) / "rpc.json"
        self.path_patch = mock.patch.object(server_mqtt, "RPC_CONFIG_PATH", self.config_path)
        self.path_patch.start()
        self.manager_patch = mock.patch.object(server_mqtt, "MultiMQTTManager", FakeMQTTManager)
        self.manager_patch.start()
        self.previous_active_server = server_mqtt._ACTIVE_SERVER
        FakeMQTTManager.instances.clear()
        self.server = server_mqtt.MQTTServer(
            brokers=[("old-broker.example", 1883)],
            request_topic="old/request",
            server_public_key_bytes=None,
            enable_global_topic_list=False,
        )
        self.server.mqtt_net.start()
        self.server.mqtt_net.subscribe(self.server.request_topic)
        server_mqtt._ACTIVE_SERVER = self.server

    def tearDown(self):
        server_mqtt._ACTIVE_SERVER = self.previous_active_server
        self.manager_patch.stop()
        self.path_patch.stop()
        self.temp_dir.cleanup()

    def test_topic_update_persists_and_replaces_subscription(self):
        result = server_mqtt.rpc_config(request_topic="new/request")

        self.assertEqual(result["request_topic"], "new/request")
        self.assertIn("new/request", self.server.mqtt_net.subscribed_topics)
        self.assertEqual(self.server.mqtt_net.unsubscribed, ["old/request"])
        self.assertEqual(server_mqtt.rpc_config_get(), result)
        self.assertEqual(server_mqtt.load_rpc_config()["request_topic"], "new/request")
        self.assertEqual(json.loads(self.config_path.read_text())["request_topic"], "new/request")

    def test_broker_and_public_key_update_replaces_live_network(self):
        previous_manager = self.server.mqtt_net
        result = server_mqtt.rpc_config(
            broker_list=[["new-broker.example", 1884]],
            pub_key=server_mqtt.DEFAULT_PUB_KEY,
        )

        self.assertEqual(result["broker_list"], [["new-broker.example", 1884]])
        self.assertEqual(result["pub_key"], server_mqtt.DEFAULT_PUB_KEY)
        self.assertTrue(previous_manager.stopped)
        self.assertIsNot(self.server.mqtt_net, previous_manager)
        self.assertEqual(self.server.mqtt_net.brokers, [("new-broker.example", 1884)])
        self.assertIn("old/request", self.server.mqtt_net.subscribed_topics)
        stored = json.loads(self.config_path.read_text())
        self.assertEqual(stored["broker_list"], [["new-broker.example", 1884]])
        self.assertEqual(stored["pub_key"], server_mqtt.DEFAULT_PUB_KEY)

    def test_invalid_update_does_not_change_runtime_or_file(self):
        with self.assertRaisesRegex(ValueError, "request_topic"):
            server_mqtt.rpc_config(request_topic=" ")

        self.assertEqual(self.server.request_topic, "old/request")
        self.assertFalse(self.config_path.exists())

    def test_command_line_values_override_file_and_defaults(self):
        self.config_path.write_text(
            json.dumps({
                "broker_list": [["file-broker.example", 1884]],
                "request_topic": "file/request",
                "pub_key": None,
            }),
            encoding="utf-8",
        )

        config = server_mqtt._rpc_config_with_cli_overrides(
            request_topic="cli/request",
            pub=[server_mqtt.DEFAULT_PUB_KEY],
            brokers=["cli-broker.example:1885"],
        )

        self.assertEqual(config["request_topic"], "cli/request")
        self.assertEqual(config["broker_list"], [("cli-broker.example", 1885)])
        self.assertEqual(config["pub_key"], server_mqtt.DEFAULT_PUB_KEY)

    def test_command_line_pub_without_value_disables_file_public_key(self):
        self.config_path.write_text(
            json.dumps({"pub_key": server_mqtt.DEFAULT_PUB_KEY}),
            encoding="utf-8",
        )

        config = server_mqtt._rpc_config_with_cli_overrides(pub=[])

        self.assertIsNone(config["pub_key"])

    def test_config_ignores_unknown_fields_and_defaults_missing_values(self):
        self.config_path.write_text(
            json.dumps({
                "request_topic": "partial/request",
                "obsolete_option": "ignored",
            }),
            encoding="utf-8",
        )

        config = server_mqtt.load_rpc_config()

        self.assertEqual(config["request_topic"], "partial/request")
        self.assertEqual(config["broker_list"], server_mqtt._default_rpc_config()["broker_list"])
        self.assertEqual(config["pub_key"], server_mqtt.DEFAULT_PUB_KEY)
        self.assertEqual(config["global_topic_url"], server_mqtt.DEFAULT_GLOBAL_TOPIC_URL)
        self.assertNotIn("obsolete_option", config)

    def test_global_topic_url_update_is_saved_and_used(self):
        url = "https://reports.example.net/topics/"
        result = server_mqtt.rpc_config(global_topic_url=url)

        self.assertEqual(result["global_topic_url"], url)
        self.assertEqual(self.server.global_topic_url, url)
        self.assertEqual(
            json.loads(self.config_path.read_text())["global_topic_url"],
            url,
        )

        response = mock.MagicMock()
        response.__enter__.return_value = response
        with mock.patch("urllib.request.urlopen", return_value=response) as urlopen:
            self.server._publish_global_topic_list("test")
        request = urlopen.call_args.args[0]
        self.assertTrue(request.full_url.startswith(url + "with%20open"))

    def test_invalid_global_topic_url_is_rejected(self):
        with self.assertRaisesRegex(ValueError, "HTTP\\(S\\) URL"):
            server_mqtt.rpc_config(global_topic_url="ftp://reports.example.net/")
        self.assertFalse(self.config_path.exists())

    def test_rpc_functions_are_callable_over_http(self):
        import server_http

        http_server = server_http.start_rpc_server(
            port=0,
            ip="127.0.0.1",
            globals={
                "rpc_config": server_mqtt.rpc_config,
                "rpc_config_get": server_mqtt.rpc_config_get,
            },
        )
        try:
            expression = "rpc_config(request_topic='http/request')"
            url = (
                f"http://127.0.0.1:{http_server.server_port}/"
                f"r={urllib.parse.quote(expression, safe='')}"
            )
            with urllib.request.urlopen(url) as response:
                result = ast.literal_eval(response.read().decode("utf-8"))
            self.assertEqual(result["request_topic"], "http/request")
            self.assertEqual(self.server.request_topic, "http/request")
            get_expression = "rpc_config_get()"
            get_url = (
                f"http://127.0.0.1:{http_server.server_port}/"
                f"r={urllib.parse.quote(get_expression, safe='')}"
            )
            with urllib.request.urlopen(get_url) as response:
                actual = ast.literal_eval(response.read().decode("utf-8"))
            self.assertEqual(actual, result)
        finally:
            http_server.shutdown()
            http_server.server_close()


if __name__ == "__main__":
    unittest.main()
