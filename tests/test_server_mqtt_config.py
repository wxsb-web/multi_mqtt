import ast
import json
import types
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

    def test_empty_public_key_is_persisted_and_disables_key_after_reload(self):
        server_mqtt._write_rpc_config_updates({"pub_key": server_mqtt.DEFAULT_PUB_KEY})

        result = server_mqtt.rpc_config(pub_key="")

        self.assertIsNone(result["pub_key"])
        self.assertIsNone(self.server.get_rpc_config()["pub_key"])
        self.assertEqual(json.loads(self.config_path.read_text())["pub_key"], "")
        self.assertIsNone(server_mqtt._rpc_config_with_cli_overrides()["pub_key"])

    def test_broker_add_and_del_overlay_default_list(self):
        defaults = [
            ("broker-a.example", 1883),
            ("broker-b.example", 1884, ["old-user", "old-pass"]),
        ]
        config = server_mqtt._normalize_rpc_config({
            "broker_list": defaults,
            "broker_list_add": [
                ["broker-a.example", 1883, ["new-user", "new-pass"]],
                ["broker-c.example", 1885],
            ],
            "broker_list_del": [["broker-b.example", 1884]],
        })

        self.assertEqual(
            config["broker_list"],
            [
                ("broker-a.example", 1883, ["new-user", "new-pass"]),
                ("broker-c.example", 1885),
            ],
        )

    def test_rpc_broker_delta_updates_can_restore_deleted_defaults(self):
        deleted_broker = ("old-broker.example", 1883)
        added_broker = ("new-broker.example", 1886)

        result = server_mqtt.rpc_config(
            broker_list_add=[added_broker],
            broker_list_del=[deleted_broker],
        )
        self.assertNotIn(list(deleted_broker), result["broker_list"])
        self.assertIn(list(added_broker), result["broker_list"])

        result = server_mqtt.rpc_config(broker_list_del=[])
        self.assertIn(list(deleted_broker), result["broker_list"])
        self.assertIn(list(added_broker), result["broker_list"])
        self.assertEqual(self.server.mqtt_net.brokers, [
            tuple(entry) for entry in result["broker_list"]
        ])

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

    def test_missing_topic_uses_and_persists_machine_id(self):
        machine_id_module = types.ModuleType("U")
        machine_id_module.get_machine_id = lambda: "TestDevice_123456"
        with mock.patch.dict(sys.modules, {"U": machine_id_module}):
            config = server_mqtt._rpc_config_with_cli_overrides()

        self.assertEqual(config["request_topic"], "TestDevice_123456")
        self.assertEqual(
            json.loads(self.config_path.read_text())["request_topic"],
            "TestDevice_123456",
        )

    def test_missing_topic_falls_back_when_machine_id_fails(self):
        machine_id_module = types.ModuleType("U")
        machine_id_module.get_machine_id = mock.Mock(side_effect=OSError("unavailable"))
        with mock.patch.dict(sys.modules, {"U": machine_id_module}):
            config = server_mqtt._rpc_config_with_cli_overrides()

        self.assertEqual(config["request_topic"], server_mqtt.REQUEST_TOPIC)
        self.assertFalse(self.config_path.exists())

    def test_explicit_cli_topic_skips_machine_id_and_persists_nothing(self):
        machine_id_module = types.ModuleType("U")
        machine_id_module.get_machine_id = mock.Mock(side_effect=AssertionError("must not run"))
        with mock.patch.dict(sys.modules, {"U": machine_id_module}):
            config = server_mqtt._rpc_config_with_cli_overrides(
                request_topic="cli/request"
            )

        self.assertEqual(config["request_topic"], "cli/request")
        self.assertFalse(self.config_path.exists())

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

    def test_timestamp_reject_publishes_explicit_error_reply(self):
        """重放窗口拒收不得静默：按标准回包格式向 reply_topic 发明确错误。

        客户端（client_mqtt.request）按 req_id 匹配回包后立即把 error 打印
        出来——不再死等到自身超时还摸不清是时钟偏差还是重放拒收。
        """
        recorded = []

        def _rec(topic, payload, **ka):
            recorded.append((topic, dict(payload)))

        self.server.mqtt_net.publish_broadcast = _rec

        class _MustNotExecute:
            def execute(self, *a, **k):
                raise AssertionError("窗口拒收的请求绝不能进入执行器")

        self.server.executor = _MustNotExecute()

        frame = {
            "req_id": "20261024_000101.002|deadbeefcafe",
            "reply_topic": "client/unique/reply",
            "code": "while True: pass",
            "timestamp": 1,   # 远古时间戳
            "_cmq_timestamp_reject": {
                "reason": "timestamp_window", "window": 120.0,
                "frame_ts": 1, "server_now_ms": 1790000000000,
                "error": ("rejected: timestamp outside replay window (±120s). "
                          "frame_ts=1 server_now_ms=1790000000000. "
                          "若为时钟偏差请校时(NTP)或调宽/关闭 "
                          "MultiMQTTManager(timestamp_window=...)。"),
            },
        }
        self.server.handle_message("old/request", frame, "fake-broker")

        self.assertEqual(len(recorded), 1, "必须恰好回包一次错误")
        topic, resp = recorded[0]
        self.assertEqual(topic, "client/unique/reply")
        self.assertFalse(resp["ok"])
        self.assertIn("replay window", resp["error"])
        self.assertIn("timestamp_window", resp["error"])
        self.assertEqual(resp["rejected"], "timestamp_window")
        self.assertEqual(resp["req_id"], "20261024_000101.002",
                         "回包 req_id 必须剥掉签名后缀，客户端才能按 base 匹配")
        self.assertEqual(resp["server_from"], "fake-broker")
        self.assertIn("server_time", resp)
        self.assertIsNone(resp["r"])
        # 标记已消费，不残留
        self.assertNotIn("_cmq_timestamp_reject", frame)

    def test_global_topic_list_publish_is_always_qos1(self):
        """global_topic_list 报告恒以 QoS1 发布：默认参数与自定义参数都如此。"""
        # 默认发布参数：QoS1 + Retain
        self.assertEqual(self.server.global_topic_list_publish_kwargs["qos"], 1)
        self.assertTrue(self.server.global_topic_list_publish_kwargs.get("retain"))

        # 自定义发布参数缺 qos（甚至显式给 0）时都必须钉在 1：报告的
        # broker 侧保留语义依赖 QoS1，静默跌回 QoS0 会让新订阅者拿不到
        # 最新列表
        other = server_mqtt.MQTTServer(
            brokers=[("b.example", 1883)],
            request_topic="t/request",
            server_public_key_bytes=None,
            enable_global_topic_list=False,
            global_topic_list_publish_kwargs={"retain": False, "qos": 0},
        )
        self.assertEqual(other.global_topic_list_publish_kwargs["qos"], 1)

        # 发布链路确实带 qos=1 透传到 publish_broadcast
        recorded = {}

        def _rec(_topic, _payload, **ka):
            recorded.update(ka)

        other.mqtt_net.publish_broadcast = _rec
        response = mock.MagicMock()
        response.__enter__.return_value = response
        with mock.patch("urllib.request.urlopen", return_value=response):
            other._publish_global_topic_list("test")
        self.assertEqual(recorded.get("qos"), 1)

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


class IsVerifyEnabledTests(unittest.TestCase):
    """[R4 死代码清理] _server_vk_invalid 标志删除后，验签状态只由真实属性决定。

    公钥配置但解析失败的场景由 MultiMQTTManager 构造时的 from_pem 异常
    兜底（fail-closed），不再依赖任何 invalid 标志位。
    """

    def test_enabled_when_vk_present(self):
        net = types.SimpleNamespace(server_vk=object(),
                                    server_public_key_bytes=b"pem")
        enabled, reason = server_mqtt._is_verify_enabled(net)
        self.assertTrue(enabled)
        self.assertEqual(reason, "启用")

    def test_bytes_without_vk_reported(self):
        net = types.SimpleNamespace(server_vk=None,
                                    server_public_key_bytes=b"pem")
        enabled, reason = server_mqtt._is_verify_enabled(net)
        self.assertFalse(enabled)
        self.assertIn("未初始化", reason)

    def test_disabled_without_key(self):
        net = types.SimpleNamespace(server_vk=None,
                                    server_public_key_bytes=None)
        enabled, reason = server_mqtt._is_verify_enabled(net)
        self.assertFalse(enabled)
        self.assertIn("关闭", reason)

    def test_real_manager_has_no_invalid_flag(self):
        import ecdsa
        from multi_mqtt import MultiMQTTManager
        sk = ecdsa.SigningKey.generate(curve=ecdsa.NIST256p)
        mgr = MultiMQTTManager(
            brokers=[], enable_stats=False, log_connection=False,
            recovery_enabled=False,
            server_public_key_bytes=sk.get_verifying_key().to_pem())
        self.assertFalse(hasattr(mgr, "_server_vk_invalid"),
                         "死标志位不应再存在于 MultiMQTTManager")
        self.assertEqual(server_mqtt._is_verify_enabled(mgr), (True, "启用"))

    def test_invalid_pubkey_fails_closed_at_construction(self):
        from multi_mqtt import MultiMQTTManager
        with self.assertRaises(Exception):
            MultiMQTTManager(
                brokers=[], enable_stats=False, log_connection=False,
                recovery_enabled=False,
                server_public_key_bytes=b"definitely-not-a-pem")


if __name__ == "__main__":
    unittest.main()
