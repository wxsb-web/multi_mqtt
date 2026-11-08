import ast
import json
import types
import sys
import tempfile
import time
import unittest
import urllib.parse
import urllib.request
import weakref
from pathlib import Path
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import server_mqtt
import rpc_config


class FakeMQTTManager:
    instances = []

    def __init__(self, brokers, server_public_key_bytes=None,
                 stats_print_interval=None, stats_ping_interval=None, **_kwargs):
        self.brokers = brokers
        self.server_public_key_bytes = server_public_key_bytes
        self.server_vk = None
        self.clients = {}
        self.subscribed_topics = set()
        self.subscribed = []
        self.unsubscribed = []
        self.stopped = False
        self.updated_brokers = []
        self.updated_public_keys = []
        self.callback = None
        self.stats_print_interval = (
            server_mqtt.DEFAULT_STATS_PRINT_INTERVAL
            if stats_print_interval is None else stats_print_interval
        )
        self.stats_ping_interval = (
            server_mqtt.DEFAULT_STATS_PING_INTERVAL
            if stats_ping_interval is None else stats_ping_interval
        )
        self.__class__.instances.append(self)

    def set_stats_print_interval(self, seconds):
        self.stats_print_interval = seconds

    def set_stats_ping_interval(self, seconds):
        self.stats_ping_interval = seconds

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

    def update_brokers(self, brokers):
        """模拟网络层增量热更新：只改配置表，不替换 manager。"""
        self.brokers = list(brokers)
        self.updated_brokers.append(list(brokers))

    def update_server_public_key(self, server_public_key_bytes):
        """模拟验签公钥热更新：不碰连接。"""
        self.server_public_key_bytes = server_public_key_bytes
        self.updated_public_keys.append(server_public_key_bytes)

    def set_periodic_task(self, fn, interval=60.0):
        self.periodic_fn = fn
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
        # 每个用例用独立的实例注册表，避免上一个用例残留实例被模块级更新连带
        self.previous_servers = server_mqtt._RPC_SERVERS
        server_mqtt._RPC_SERVERS = weakref.WeakSet()
        self.manager_patch = mock.patch.object(server_mqtt, "MultiMQTTManager", FakeMQTTManager)
        self.manager_patch.start()
        self.previous_gms = server_mqtt.gms
        FakeMQTTManager.instances.clear()
        self.server = server_mqtt.MQTTServer(
            brokers=[("old-broker.example", 1883)],
            request_topic="old/request",
            server_public_key_bytes=None,
        )
        self.server.mqtt_net.start()
        self.server.mqtt_net.subscribe(self.server.request_topic)
        server_mqtt.gms = self.server

    def tearDown(self):
        server_mqtt.gms = self.previous_gms
        server_mqtt._RPC_SERVERS = self.previous_servers
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

    def test_broker_and_public_key_update_hot_applies_without_reconnect(self):
        previous_manager = self.server.mqtt_net
        result = server_mqtt.rpc_config(
            broker_list=[["new-broker.example", 1884]],
            pub_key=server_mqtt.DEFAULT_PUB_KEY,
        )

        self.assertEqual(result["broker_list"], [["new-broker.example", 1884]])
        self.assertEqual(result["pub_key"], server_mqtt.DEFAULT_PUB_KEY)
        # 公钥不参与建连：换公钥 + 换 broker 也只热更，不重建 manager
        self.assertIs(self.server.mqtt_net, previous_manager)
        self.assertFalse(previous_manager.stopped)
        self.assertEqual(previous_manager.brokers, [("new-broker.example", 1884)])
        self.assertEqual(previous_manager.updated_public_keys,
                         [server_mqtt.DEFAULT_PUB_KEY])
        self.assertIn("old/request", previous_manager.subscribed_topics)
        stored = json.loads(self.config_path.read_text())
        self.assertEqual(stored["broker_list"], [["new-broker.example", 1884]])
        self.assertEqual(stored["pub_key"], server_mqtt.DEFAULT_PUB_KEY)

    def test_public_key_only_change_never_touches_connections(self):
        previous_manager = self.server.mqtt_net

        server_mqtt.rpc_config(pub_key=server_mqtt.DEFAULT_PUB_KEY)

        self.assertIs(self.server.mqtt_net, previous_manager)   # 没换 manager
        self.assertFalse(previous_manager.stopped)              # 连接没断
        self.assertEqual(previous_manager.updated_brokers, [])  # broker 一条没动
        self.assertEqual(previous_manager.updated_public_keys,
                         [server_mqtt.DEFAULT_PUB_KEY])

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

    def test_rpc_json_broker_list_replaces_builtin_with_new_password(self):
        """rpc.json 里的 broker_list 整体替换内置列表（同 host 换密码不残留旧凭据）。"""
        self.config_path.write_text(
            json.dumps({
                "broker_list": [
                    ["public.runmqtt.com", 1883,
                     ["ajbkvbp/device", "kSPmkUIFWxSRgtPq"]],
                ]
            }),
            encoding="utf-8",
        )

        config = server_mqtt.load_rpc_config()

        self.assertEqual(config["broker_list"], [
            ("public.runmqtt.com", 1883,
             ["ajbkvbp/device", "kSPmkUIFWxSRgtPq"]),
        ])

    def test_broker_add_same_host_new_password_hot_updates_only_one(self):
        """动态改一条 broker：同名换密码原位替换，其它 broker 连接继续不动。"""
        previous_manager = self.server.mqtt_net
        new_entry = ["public.runmqtt.com", 1883,
                     ["ajbkvbp/device", "kSPmkUIFWxSRgtPq"]]

        result = server_mqtt.rpc_config(broker_list_add=[new_entry])

        self.assertEqual(result["broker_list"], [
            ["old-broker.example", 1883], new_entry])
        self.assertIs(self.server.mqtt_net, previous_manager)   # 没换 manager
        self.assertFalse(previous_manager.stopped)              # 其它连接没被停
        self.assertEqual(len(previous_manager.updated_brokers), 1)
        self.assertEqual(previous_manager.brokers, [
            ("old-broker.example", 1883),
            ("public.runmqtt.com", 1883,
             ["ajbkvbp/device", "kSPmkUIFWxSRgtPq"]),
        ])

        # 再换一次密码：仍然只替换同一条，不会新增重复条目
        second = ["public.runmqtt.com", 1883, ["ajbkvbp/device", "another-pass"]]
        result = server_mqtt.rpc_config(broker_list_add=[second])
        self.assertEqual(result["broker_list"], [
            ["old-broker.example", 1883], second])
        stored = json.loads(self.config_path.read_text())
        self.assertEqual(stored["broker_list_add"], [second])

    def test_add_broker_alias_accepts_host_port_string(self):
        """HTTP/MQTT 入口常见的 add_broker='host:port' 要真正生效并落盘。"""
        result = server_mqtt.rpc_config(add_broker="mqtt.bemfa.com:9501")

        self.assertIn(["mqtt.bemfa.com", 9501], result["broker_list"])
        self.assertEqual(
            json.loads(self.config_path.read_text())["broker_list_add"],
            [["mqtt.bemfa.com", 9501]],
        )

    def test_add_broker_alias_accepts_dsn_with_credentials(self):
        """mqtt://user:pass@host:port（前缀可省略）要保留认证并落盘。"""
        result = server_mqtt.rpc_config(
            add_broker="mqtt://appID:secretKey@bemfa.com:9501")

        self.assertIn(["bemfa.com", 9501, ["appID", "secretKey"]],
                      result["broker_list"])
        self.assertEqual(
            json.loads(self.config_path.read_text())["broker_list_add"],
            [["bemfa.com", 9501, ["appID", "secretKey"]]],
        )

    def test_add_broker_alias_defaults_port_to_1883(self):
        result = server_mqtt.rpc_config(add_broker="broker.codenow.cn")

        self.assertIn(["broker.codenow.cn", 1883], result["broker_list"])

    def test_instance_level_alias_matches_module_level(self):
        """实例级 gms.rpc_config_set 也走同一套别名。"""
        result = self.server.rpc_config_set(
            add_broker="mqtt.bemfa.com:9501", t="alias/request")

        self.assertIn(["mqtt.bemfa.com", 9501], result["broker_list"])
        self.assertEqual(result["request_topic"], "alias/request")
        self.assertIn("alias/request", self.server.mqtt_net.subscribed_topics)

    def test_del_broker_alias_accepts_host_port_string(self):
        result = server_mqtt.rpc_config(del_broker="old-broker.example:1883")

        self.assertNotIn(["old-broker.example", 1883], result["broker_list"])

    def test_broker_alias_bad_port_raises_instead_of_silent_noop(self):
        """显式输入写错要立刻报错，不能像未知键一样静默忽略。"""
        with self.assertRaises(ValueError):
            server_mqtt.rpc_config(add_broker="mqtt.bemfa.com:not-a-port")

    def test_conflicting_field_aliases_raise(self):
        with self.assertRaises(ValueError):
            server_mqtt.rpc_config(t="a/request", request_topic="b/request")

    def test_object_level_rpc_config_applies_and_reaches_all_instances(self):
        """实例级 get/set 可用（不再 AttributeError），模块级 set 作用于全部实例。"""
        second = server_mqtt.MQTTServer(
            brokers=[("second-broker.example", 1883)],
            request_topic="second/request",
            server_public_key_bytes=None,
        )
        second.mqtt_net.start()
        second.mqtt_net.subscribe(second.request_topic)

        self.assertEqual(self.server.rpc_config_get()["request_topic"], "old/request")
        self.assertEqual(
            self.server.rpc_config(request_topic="obj/request")["request_topic"],
            "obj/request")
        self.assertEqual(
            json.loads(self.config_path.read_text())["request_topic"], "obj/request")

        server_mqtt.rpc_config(request_topic="shared/request")
        self.assertEqual(self.server.request_topic, "shared/request")
        self.assertEqual(second.request_topic, "shared/request")
        self.assertIn("shared/request", second.mqtt_net.subscribed_topics)

    def test_rpc_config_path_argument_loads_and_hot_applies(self):
        """get 只读内存且没有 path；set 只有 path 时读入该文件并逐条热更 broker。"""
        second = server_mqtt.MQTTServer(
            brokers=[("second-broker.example", 1883)],
            request_topic="second/request",
            server_public_key_bytes=None,
        )
        second.mqtt_net.start()

        # get 无 path 参数，只返回内存最新状态
        self.assertEqual(server_mqtt.rpc_config_get()["request_topic"], "old/request")
        with self.assertRaises(TypeError):
            server_mqtt.rpc_config_get(path="whatever.json")

        # 只有 path：读入文件并热应用
        file_b = Path(self.temp_dir.name) / "b.json"
        file_b.write_text(json.dumps({
            "broker_list": [
                ["old-broker.example", 1883],
                ["new-broker.example", 1883],
            ],
            "request_topic": "file/request",
            "pub_key": None,
        }), encoding="utf-8")

        previous_manager = self.server.mqtt_net
        result = self.server.rpc_config(path=file_b)
        self.assertEqual(result["request_topic"], "file/request")
        self.assertIs(self.server.mqtt_net, previous_manager)   # 没换 manager
        self.assertFalse(previous_manager.stopped)              # 连接没断
        self.assertEqual(len(previous_manager.updated_brokers), 1)
        self.assertEqual(self.server.rpc_config_path, file_b)

        # 另一份文件 broker 完全相同：不触发 broker 更新，连接保持
        file_c = Path(self.temp_dir.name) / "c.json"
        file_c.write_text(json.dumps({
            "broker_list": [
                ["old-broker.example", 1883],
                ["new-broker.example", 1883],
            ],
            "request_topic": "file2/request",
            "pub_key": None,
        }), encoding="utf-8")
        result = self.server.rpc_config(path=file_c)
        self.assertEqual(result["request_topic"], "file2/request")
        self.assertFalse(previous_manager.stopped)
        self.assertEqual(len(previous_manager.updated_brokers), 1)  # 没有新增热更
        self.assertEqual(self.server.rpc_config_path, file_c)

        # 之后 set 默认写到新位置
        self.server.rpc_config(stats_ping_interval=60)
        stored = json.loads(file_c.read_text())
        self.assertEqual(stored["stats_ping_interval"], 60.0)

        # 模块级只有 path：所有实例一起读入并更新默认位置
        server_mqtt.rpc_config(path=file_c)
        self.assertEqual(second.rpc_config_path, file_c)
        self.assertEqual(server_mqtt.RPC_CONFIG_PATH, file_c)
        self.assertEqual(second.request_topic, "file2/request")
        self.assertFalse(self.config_path.exists())

        # 显式路径写错：当场报错，不动现有连接
        with self.assertRaises(FileNotFoundError):
            self.server.rpc_config(path=Path(self.temp_dir.name) / "missing.json")
        self.assertFalse(previous_manager.stopped)

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

    def test_command_line_broker_accepts_dsn_and_default_port(self):
        config = server_mqtt._rpc_config_with_cli_overrides(
            brokers=["appID:secretKey@bemfa.com:9501", "broker.codenow.cn"])

        self.assertEqual(config["broker_list"], [
            ("bemfa.com", 9501, ["appID", "secretKey"]),
            ("broker.codenow.cn", 1883),
        ])

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
        self.assertEqual(config["global_topic_url_list"],
                         server_mqtt._default_rpc_config()["global_topic_url_list"])
        self.assertNotIn("obsolete_option", config)

    def test_global_topic_url_list_update_is_saved_and_used(self):
        url = "https://reports.example.net/topics/REPORT_BODY"
        result = server_mqtt.rpc_config(global_topic_url_list=[url])

        self.assertEqual(result["global_topic_url_list"], [url])
        self.assertEqual(self.server.global_topic_url_list, [url])
        self.assertEqual(
            json.loads(self.config_path.read_text())["global_topic_url_list"],
            [url],
        )

        response = mock.MagicMock()
        response.__enter__.return_value = response
        with mock.patch("urllib.request.urlopen", return_value=response) as urlopen:
            self.server._publish_global_topic_list("test", confirm_timeout=0)
            deadline = time.time() + 5          # 上报已改异步线程，轮询等它发出
            while not urlopen.called and time.time() < deadline:
                time.sleep(0.01)
            self.assertTrue(urlopen.called, "远端上报线程未在 5s 内发出请求")
            request = urlopen.call_args.args[0]
        self.assertTrue(request.full_url.startswith(url.replace("REPORT_BODY", "%7B")))
        self.assertNotIn("REPORT_BODY", request.full_url)

    def test_invalid_url_entries_are_skipped_and_only_empty_list_disables(self):
        """URL 模板逐条校验：坏项只跳过坏项；只有显式 [] 才关闭远端上报。"""
        good = "https://reports.example.net/topics/REPORT_BODY"

        # 同一数组里，合法 URL 保留，非法项（None / 非 http(s)）只被跳过
        result = server_mqtt.rpc_config(
            global_topic_url_list=[good, None, "ftp://reports.example.net/"])
        self.assertEqual(result["global_topic_url_list"], [good])
        self.assertEqual(self.server.global_topic_url_list, [good])
        self.assertEqual(
            json.loads(self.config_path.read_text())["global_topic_url_list"], [good])

        # 只有明确设置 [] 才是关闭上报
        result = server_mqtt.rpc_config(global_topic_url_list=[])
        self.assertEqual(result["global_topic_url_list"], [])
        self.assertEqual(self.server.global_topic_url_list, [])
        self.assertEqual(
            json.loads(self.config_path.read_text())["global_topic_url_list"], [])

        # 非数组、或整组全非法：回落代码默认值（≠ 关闭）
        defaults = server_mqtt._default_rpc_config()["global_topic_url_list"]
        result = server_mqtt.rpc_config(
            global_topic_url_list="https://reports.example.net/")
        self.assertEqual(result["global_topic_url_list"], defaults)
        result = server_mqtt.rpc_config(
            global_topic_url_list=["ftp://reports.example.net/"])
        self.assertEqual(result["global_topic_url_list"], defaults)

    def test_shipped_url_list_entries_are_plain_strings(self):
        """默认列表里每项必须是字符串：模板曾被写成 list/tuple，默认上报被静默关闭。"""
        shipped = server_mqtt.DEFAULT_GLOBAL_TOPIC_URL_LIST
        self.assertTrue(shipped)
        for entry in shipped:
            self.assertIsInstance(entry, str)
            self.assertTrue(entry.strip())
            self.assertIn(server_mqtt.REPORT_BODY_TOKEN, entry)

        defaults = server_mqtt._default_global_topic_url_list()
        self.assertTrue(defaults)                       # 默认值可用（不是 []）
        self.assertEqual(defaults,
                         server_mqtt._default_rpc_config()["global_topic_url_list"])

    def test_load_rpc_config_falls_back_for_invalid_file_values(self):
        """rpc.json 里的脏值只丢该项、回落代码默认值，绝不阻断启动。"""
        self.config_path.write_text(
            json.dumps({
                "request_topic": "device/topic",
                "global_topic_url_list": [None],       # 元素类型错
                "stats_ping_interval": "abc",          # 不是数字
                "global_topic_list": "",               # 空字符串
            }),
            encoding="utf-8",
        )

        config = server_mqtt.load_rpc_config()
        defaults = server_mqtt._default_rpc_config()

        self.assertEqual(config["request_topic"], "device/topic")
        self.assertEqual(config["global_topic_url_list"],
                         defaults["global_topic_url_list"])
        self.assertEqual(config["stats_ping_interval"], defaults["stats_ping_interval"])
        self.assertEqual(config["global_topic_list"], defaults["global_topic_list"])

    def test_load_rpc_config_survives_broken_json_and_bad_shape(self):
        """整份 JSON 坏掉或不是 object 时同样回落默认值，不抛异常。"""
        self.config_path.write_text("{ not json", encoding="utf-8")
        config = server_mqtt.load_rpc_config()
        self.assertEqual(config["broker_list"], server_mqtt._default_rpc_config()["broker_list"])

        self.config_path.write_text('["not", "an", "object"]', encoding="utf-8")
        config = server_mqtt.load_rpc_config()
        self.assertEqual(config["request_topic"], server_mqtt.REQUEST_TOPIC)

    def test_rpc_json_partial_bad_urls_keep_valid_entries(self):
        """rpc.json 里同组 URL：坏项只跳过坏项，合法 URL 照常生效。"""
        good = "https://reports.example.net/topics/REPORT_BODY"
        self.config_path.write_text(
            json.dumps({"global_topic_url_list": [good, None, "ftp://bad.example/"]}),
            encoding="utf-8",
        )

        config = server_mqtt.load_rpc_config()

        self.assertEqual(config["global_topic_url_list"], [good])

    def test_rpc_json_empty_url_list_disables_remote_report(self):
        """只有显式空数组才关闭远端上报；缺键仍是默认值（默认开启）。"""
        self.config_path.write_text(
            json.dumps({"global_topic_url_list": []}), encoding="utf-8")

        config = server_mqtt.load_rpc_config()

        self.assertEqual(config["global_topic_url_list"], [])
        self.assertNotEqual(
            server_mqtt._default_rpc_config()["global_topic_url_list"], [])
        self.assertEqual(
            server_mqtt.load_rpc_config(path=self.config_path)["global_topic_url_list"], [])

    def test_cli_overrides_fall_back_for_bad_urls_and_keep_strictness_elsewhere(self):
        """文件/命令行的 URL 脏值都只降级不报错；其他显式键仍严格。"""
        self.config_path.write_text(
            json.dumps({"global_topic_url_list": [[None]]}),
            encoding="utf-8",
        )

        config = server_mqtt._rpc_config_with_cli_overrides()
        self.assertEqual(config["global_topic_url_list"],
                         server_mqtt._default_rpc_config()["global_topic_url_list"])

        # 命令行显式 []：关闭上报（不是回落默认值）
        config = server_mqtt._rpc_config_with_cli_overrides(global_topic_url_list=[])
        self.assertEqual(config["global_topic_url_list"], [])

        # 命令行显式合法 URL：只保留合法项
        url = "https://reports.example.net/log/REPORT_BODY"
        config = server_mqtt._rpc_config_with_cli_overrides(
            global_topic_url_list=[url, "ftp://bad.example/"])
        self.assertEqual(config["global_topic_url_list"], [url])

        # 其它显式键（非 URL）仍严格报错
        with self.assertRaisesRegex(ValueError, "request_topic"):
            server_mqtt._rpc_config_with_cli_overrides(request_topic=" ")

    def test_bad_edge_stats_only_fall_back_that_key(self):
        """边缘统计是附加功能：坏一项只回落该项，其他统计与主流程项照常生效。"""
        config = server_mqtt._normalize_rpc_config({
            "request_topic": "device/topic",
            "global_topic_url_list": [None],        # URL 模板写错
            "global_topic_list_check_interval": "abc",  # tick 粒度写错
            "stats_ping_interval": 30,              # 同族统计，必须保留
            "global_topic_list_interval": 120,      # 同族统计，必须保留
        })

        defaults = server_mqtt._default_rpc_config()
        self.assertEqual(config["request_topic"], "device/topic")
        self.assertEqual(config["global_topic_url_list"],
                         defaults["global_topic_url_list"])
        self.assertEqual(config["global_topic_list_check_interval"],
                         defaults["global_topic_list_check_interval"])
        self.assertEqual(config["stats_ping_interval"], 30.0)
        self.assertEqual(config["global_topic_list_interval"], 120.0)

    def test_broken_json_does_not_block_cli_startup(self):
        """整份 rpc.json 不可解析时命令行启动路径也必须能算出一份可用配置。"""
        self.config_path.write_text("{ not json", encoding="utf-8")

        config = server_mqtt._rpc_config_with_cli_overrides()

        self.assertEqual(config["broker_list"],
                         server_mqtt._normalize_broker_list(
                             server_mqtt.BROKER_LIST, "broker_list"))
        self.assertIsInstance(config["request_topic"], str)
        self.assertTrue(config["request_topic"].strip())

    def test_server_starts_even_when_shipped_url_default_is_broken(self):
        """回归：APK 打包的默认 URL 模板写成嵌套 list 时，手机也不能启动崩溃。

        默认值有误属于代码/打包 bug，边缘统计必须静默降级（关闭远端上报），
        而不是让 load_rpc_config 抛 ValueError 把整个 start() 打断。
        """
        broken = [["https://reports.example.net/REPORT_BODY"]]
        with mock.patch.object(server_mqtt, "DEFAULT_GLOBAL_TOPIC_URL_LIST", broken):
            server = server_mqtt.MQTTServer(
                brokers=[("old-broker.example", 1883)],
                request_topic="old/request",
            )

        self.assertEqual(server.global_topic_url_list, [],
                         "非法默认值必须降级为关闭远端上报")
        self.assertEqual(server.get_rpc_config()["global_topic_url_list"], [])
        self.assertIsNotNone(server.mqtt_net)

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
            global_topic_list_publish_kwargs={"retain": False, "qos": 0},
            global_topic_url_list=[],          # 本用例只验证 qos，不上报远端
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
            other._publish_global_topic_list("test", confirm_timeout=0)
        self.assertEqual(recorded.get("qos"), 1)

    def test_global_topic_list_settings_load_from_rpc_json(self):
        """rpc.json 中的 global_topic_list_* 键必须进配置并生效到 MQTTServer。"""
        self.config_path.write_text(
            json.dumps({
                "global_topic_list": "rpc/topic_list",
                "global_topic_list_interval": 30,
                "global_topic_list_idle_threshold": 60,
                "global_topic_list_check_interval": 15,
                "global_topic_list_publish_kwargs": {"retain": False},
            }),
            encoding="utf-8",
        )

        config = server_mqtt.load_rpc_config()
        self.assertEqual(config["global_topic_list"], "rpc/topic_list")
        self.assertEqual(config["global_topic_list_interval"], 30.0)
        self.assertEqual(config["global_topic_list_idle_threshold"], 60.0)
        self.assertEqual(config["global_topic_list_check_interval"], 15.0)
        self.assertNotIn("enable_global_topic_list", config)
        self.assertEqual(config["global_topic_list_publish_kwargs"], {"retain": False})

        server = server_mqtt.MQTTServer(
            brokers=[("b.example", 1883)],
            request_topic="t/request",
            server_public_key_bytes=None,
            rpc_config_source=config,
        )
        self.assertEqual(server.global_topic_list, "rpc/topic_list")
        self.assertEqual(server.global_topic_list_interval, 30.0)
        self.assertEqual(server.global_topic_list_idle_threshold, 60.0)
        self.assertEqual(server.global_topic_list_check_interval, 15.0)
        self.assertFalse(hasattr(server, "enable_global_topic_list"))
        # rpc.json 值进入 _rpc_config：get_rpc_config()/后续 rpc_config() 以此为基线
        self.assertEqual(server.get_rpc_config()["global_topic_list_interval"], 30.0)
        # qos 无论配置什么都不允许跌回低档（retain 可自定义）
        self.assertEqual(server.global_topic_list_publish_kwargs, {"qos": 1, "retain": False})

    def test_rpc_config_updates_global_topic_list_settings_hot(self):
        """rpc_config(global_topic_list_interval=...) 运行期热生效并持久化。"""
        result = server_mqtt.rpc_config(
            global_topic_list_interval=120.0,
            global_topic_list_idle_threshold=300.0,
            global_topic_list="hot/topic_list",
        )

        self.assertEqual(result["global_topic_list_interval"], 120.0)
        self.assertEqual(self.server.global_topic_list_interval, 120.0)
        self.assertEqual(self.server.global_topic_list_idle_threshold, 300.0)
        self.assertEqual(self.server.global_topic_list, "hot/topic_list")
        stored = json.loads(self.config_path.read_text())
        self.assertEqual(stored["global_topic_list_interval"], 120.0)
        self.assertEqual(stored["global_topic_list_idle_threshold"], 300.0)
        self.assertEqual(stored["global_topic_list"], "hot/topic_list")

    def test_rpc_config_check_interval_change_reregisters_tick(self):
        """check_interval 变更须以新粒度重注册 tick（报告常开，无开关）。"""
        server_mqtt.rpc_config(global_topic_list_check_interval=5.0)
        self.assertIs(
            self.server.mqtt_net.periodic_fn.__func__,
            self.server.global_topic_list_tick.__func__,
        )   # bound method 每次访问都是新对象，比较底层函数
        self.assertEqual(self.server.mqtt_net.periodic_interval, 5.0)

        server_mqtt.rpc_config(global_topic_list_check_interval=17.0)
        self.assertEqual(self.server.mqtt_net.periodic_interval, 17.0)

    def test_invalid_global_topic_list_settings_rejected(self):
        """非法报告参数必须整体拒收：运行时与 rpc.json 均不变。"""
        for bad_updates in (
            {"global_topic_list_interval": 0},
            {"global_topic_list_idle_threshold": -5},
            {"global_topic_list_check_interval": "abc"},
            {"global_topic_list": " "},
            {"global_topic_list_publish_kwargs": "qos1"},
        ):
            with self.assertRaises(ValueError):
                server_mqtt.rpc_config(**bad_updates)
        self.assertEqual(self.server.global_topic_list_interval,
                         server_mqtt.DEFAULT_GLOBAL_TOPIC_LIST_INTERVAL)
        self.assertFalse(self.config_path.exists())

    def test_stats_intervals_load_from_rpc_json(self):
        """rpc.json 中的 stats_print_interval / stats_ping_interval 必须生效。"""
        self.config_path.write_text(
            json.dumps({
                "stats_print_interval": 120,
                "stats_ping_interval": 30,
            }),
            encoding="utf-8",
        )

        config = server_mqtt.load_rpc_config()
        self.assertEqual(config["stats_print_interval"], 120.0)
        self.assertEqual(config["stats_ping_interval"], 30.0)

        server = server_mqtt.MQTTServer(
            brokers=[("b.example", 1883)],
            request_topic="t/request",
            server_public_key_bytes=None,
            rpc_config_source=config,
        )
        self.assertEqual(server.stats_print_interval, 120.0)
        self.assertEqual(server.stats_ping_interval, 30.0)
        # 配置值已传给网络层 stats，报告/Ping 按新周期运行
        self.assertEqual(server.mqtt_net.stats_print_interval, 120.0)
        self.assertEqual(server.mqtt_net.stats_ping_interval, 30.0)
        self.assertEqual(server.get_rpc_config()["stats_print_interval"], 120.0)
        self.assertEqual(server.get_rpc_config()["stats_ping_interval"], 30.0)

    def test_rpc_config_updates_stats_intervals_hot(self):
        """只改统计周期时热更新现有 manager，不重建网络连接。"""
        manager_count = len(FakeMQTTManager.instances)

        result = server_mqtt.rpc_config(
            stats_print_interval=15.0,
            stats_ping_interval=5.0,
        )

        self.assertEqual(result["stats_print_interval"], 15.0)
        self.assertEqual(result["stats_ping_interval"], 5.0)
        self.assertEqual(self.server.stats_print_interval, 15.0)
        self.assertEqual(self.server.stats_ping_interval, 5.0)
        self.assertEqual(self.server.mqtt_net.stats_print_interval, 15.0)
        self.assertEqual(self.server.mqtt_net.stats_ping_interval, 5.0)
        self.assertEqual(len(FakeMQTTManager.instances), manager_count)
        stored = json.loads(self.config_path.read_text())
        self.assertEqual(stored["stats_print_interval"], 15.0)
        self.assertEqual(stored["stats_ping_interval"], 5.0)

    def test_stats_intervals_allow_zero_to_disable(self):
        """0 是合法值：关闭周期报告 / 停用周期 Ping。"""
        result = server_mqtt.rpc_config(
            stats_print_interval=0,
            stats_ping_interval=0,
        )
        self.assertEqual(result["stats_print_interval"], 0.0)
        self.assertEqual(result["stats_ping_interval"], 0.0)
        self.assertEqual(self.server.mqtt_net.stats_print_interval, 0.0)
        self.assertEqual(self.server.mqtt_net.stats_ping_interval, 0.0)

    def test_invalid_stats_intervals_rejected(self):
        """负数/非数字/NaN/inf 必须整体拒收，不落盘。"""
        for bad_updates in (
            {"stats_print_interval": -1},
            {"stats_ping_interval": -5},
            {"stats_print_interval": "abc"},
            {"stats_ping_interval": float("inf")},
            {"stats_print_interval": float("nan")},
            {"stats_ping_interval": True},
        ):
            with self.assertRaises(ValueError):
                server_mqtt.rpc_config(**bad_updates)
        self.assertEqual(self.server.stats_print_interval,
                         server_mqtt.DEFAULT_STATS_PRINT_INTERVAL)
        self.assertEqual(self.server.stats_ping_interval,
                         server_mqtt.DEFAULT_STATS_PING_INTERVAL)

    def test_client_stats_print_interval_loads_without_touching_server(self):
        """客户端专属键写入配置，但服务端统计周期仍按 stats_print_interval。"""
        self.config_path.write_text(
            json.dumps({
                "client_stats_print_interval": 90,
                "stats_print_interval": 120,
            }),
            encoding="utf-8",
        )

        config = server_mqtt.load_rpc_config()
        self.assertEqual(config["client_stats_print_interval"], 90.0)
        self.assertEqual(config["stats_print_interval"], 120.0)

        server = server_mqtt.MQTTServer(
            brokers=[("b.example", 1883)],
            request_topic="t/request",
            server_public_key_bytes=None,
            rpc_config_source=config,
        )
        # 服务端只应用 stats_print_interval，不应用 client_stats_print_interval
        self.assertEqual(server.stats_print_interval, 120.0)
        self.assertEqual(server.mqtt_net.stats_print_interval, 120.0)
        self.assertEqual(
            server.get_rpc_config()["client_stats_print_interval"], 90.0)

    def test_client_stats_print_interval_hot_update_and_unset(self):
        """rpc_config 可设置/清除客户端专属键，且不影响服务端周期。"""
        result = server_mqtt.rpc_config(client_stats_print_interval=45)
        self.assertEqual(result["client_stats_print_interval"], 45.0)
        self.assertEqual(self.server.stats_print_interval,
                         server_mqtt.DEFAULT_STATS_PRINT_INTERVAL)
        stored = json.loads(self.config_path.read_text())
        self.assertEqual(stored["client_stats_print_interval"], 45.0)

        # null/空字符串表示未设置：客户端会继续回退 stats_print_interval
        for unset in (None, ""):
            result = server_mqtt.rpc_config(client_stats_print_interval=unset)
            self.assertIsNone(result["client_stats_print_interval"])
            stored = json.loads(self.config_path.read_text())
            self.assertIsNone(stored["client_stats_print_interval"])

    def test_client_stats_print_interval_invalid_rejected(self):
        """客户端专属键只接受 null/空串或 >=0 的秒数。"""
        for bad_value in (-1, "abc", float("inf"), float("nan"), True):
            with self.assertRaises(ValueError):
                server_mqtt.rpc_config(client_stats_print_interval=bad_value)
        self.assertIsNone(self.server.get_rpc_config()[
            "client_stats_print_interval"])

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


class _ReplyNet:
    """[#17] 记录 publish_broadcast 的假 manager；result 模拟成功入队数（0=全失败）。"""

    def __init__(self, result=3):
        self.published = []
        self.result = result
        self.waited = None

    def publish_broadcast(self, topic, payload, **ka):
        self.published.append((topic, dict(payload)))
        return self.result

    def wait_connected(self, min_count=1, timeout=0.0):
        self.waited = (min_count, timeout)
        return 1


class ReplyPublishFallbackTests(unittest.TestCase):
    """[#17] handle_message 回包经入口 manager 快照发出；0 成功且换网时
    等新 manager 连上后重发——rpc_config 换 broker/公钥不再丢自身回包。"""

    def setUp(self):
        self.manager_patch = mock.patch.object(
            server_mqtt, "MultiMQTTManager", FakeMQTTManager)
        self.manager_patch.start()
        FakeMQTTManager.instances.clear()
        self.server = server_mqtt.MQTTServer(
            brokers=[("b.example", 1883)],
            request_topic="t/request",
            server_public_key_bytes=None,
        )

    def tearDown(self):
        self.manager_patch.stop()

    def _handle(self, req_id, code, reply="rep/x"):
        self.server.handle_message(
            "t/request",
            {"req_id": req_id, "reply_topic": reply, "code": code},
            "fake-broker")

    def test_normal_reply_single_publish_no_fallback(self):
        old = _ReplyNet(result=3)
        self.server.mqtt_net = old
        self._handle("r1", "1+1", "rep/1")
        self.assertEqual(len(old.published), 1, "正常路径只发一次，不触发回退")
        topic, payload = old.published[0]
        self.assertEqual(topic, "rep/1")
        self.assertTrue(payload["ok"])
        self.assertEqual(payload["req_id"], "r1")

    def test_swapped_manager_zero_publish_falls_back_to_new(self):
        """rpc_config 换网场景：旧 manager 已 stop（0 成功），新 manager
        冷启动——回包必须等新 manager 连上后经它重发，不再静默丢失。"""
        old = _ReplyNet(result=0)
        new = _ReplyNet(result=2)
        self.server.mqtt_net = old
        # 执行期间换网：executor 里的代码把 self.server.mqtt_net 换成 new
        from rpc_executor import PythonExecutor
        self.server.executor = PythonExecutor(
            globals={"_swap": lambda: setattr(self.server, "mqtt_net", new)})
        self._handle("r2", "_swap(); r='done'", "rep/2")
        self.assertEqual(len(old.published), 1, "先经入口快照（旧 manager）发一次")
        self.assertEqual(len(new.published), 1, "0 成功后必须经新 manager 重发")
        self.assertIsNotNone(new.waited, "重发前应等新 manager 连上")
        topic, payload = new.published[0]
        self.assertEqual(topic, "rep/2")
        self.assertEqual(payload["r"], "done")
        self.assertEqual(payload["req_id"], "r2")

    def test_zero_publish_without_swap_logs_error(self):
        old = _ReplyNet(result=0)
        self.server.mqtt_net = old
        with self.assertLogs("Server", level="ERROR") as cm:
            self._handle("r3", "1+1", "rep/3")
        self.assertTrue(any("回包发布 0 成功" in line for line in cm.output))
        self.assertEqual(len(old.published), 1, "未换网不得重发（发了也没人收）")

    def test_zero_publish_with_swap_new_also_zero_logs_error(self):
        old = _ReplyNet(result=0)
        new = _ReplyNet(result=0)
        self.server.mqtt_net = old
        from rpc_executor import PythonExecutor
        self.server.executor = PythonExecutor(
            globals={"_swap": lambda: setattr(self.server, "mqtt_net", new)})
        with self.assertLogs("Server", level="ERROR") as cm:
            self._handle("r4", "_swap(); r=1", "rep/4")
        self.assertEqual(len(new.published), 1, "换网路径应已重发一次")
        self.assertTrue(any("回包发布 0 成功" in line for line in cm.output))


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


class BrokerDirectChannelTests(unittest.TestCase):
    """broker 直读通道：load_broker_list / filter_broker_list / broker_list_no。

    全部用例只读写临时目录里的 rpc.json，绝不碰仓库 rpc.json。
    """

    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.config_path = Path(self.temp_dir.name) / "rpc.json"

    def tearDown(self):
        self.temp_dir.cleanup()

    def _write(self, document):
        self.config_path.write_text(json.dumps(document), encoding="utf-8")

    def test_compose_matches_server_channel(self):
        """双通道对拍：直读通道与 load_rpc_config 同一份合成结果。"""
        self._write({
            "broker_list": [["broker-a.example", 1883], ["broker-b.example", 1884]],
            "broker_list_add": [
                ["broker-a.example", 1883, ["user", "pass"]],   # 同身份原位替换
                ["broker-c.example", 1885],                     # 新身份追加
            ],
            "broker_list_del": [["broker-b.example", 1884]],
        })

        expected = server_mqtt.load_rpc_config(self.config_path)["broker_list"]
        self.assertEqual(server_mqtt.load_broker_list(path=self.config_path), expected)
        self.assertEqual(expected, [
            ("broker-a.example", 1883, ["user", "pass"]),
            ("broker-c.example", 1885),
        ])

    def test_broker_list_no_masks_only_listed_modules(self):
        self._write({
            "broker_list": [["broker-a.example", 1883], ["broker-b.example", 1884]],
            "broker_list_no": {"broker-b.example:1884": ["pty"]},
        })

        full = server_mqtt.load_broker_list(path=self.config_path)
        self.assertEqual(full,
                         [("broker-a.example", 1883), ("broker-b.example", 1884)])
        self.assertEqual(server_mqtt.load_broker_list(path=self.config_path,
                                                      module="pty"),
                         [("broker-a.example", 1883)])
        # 其它模块 / module=None 不受影响（未知模块名宽松放行）
        self.assertEqual(server_mqtt.load_broker_list(path=self.config_path,
                                                      module="server"), full)
        self.assertEqual(server_mqtt.load_broker_list(path=self.config_path,
                                                      module="web"), full)

    def test_mask_matches_identity_after_add_replace(self):
        """add 原位替换后屏蔽按身份继续生效（不按具体条目实例）；key host 大小写不敏感。"""
        self._write({
            "broker_list": [["broker-a.example", 1883]],
            "broker_list_add": [["broker-a.example", 1883, ["user", "pass"]]],
            "broker_list_no": {"BROKER-A.example:1883": ["pty", "tools"]},
        })

        self.assertEqual(server_mqtt.load_broker_list(path=self.config_path,
                                                      module="pty"), [])
        self.assertEqual(server_mqtt.load_broker_list(path=self.config_path,
                                                      module="tools"), [])
        self.assertEqual(server_mqtt.load_broker_list(path=self.config_path,
                                                      module="socks5"),
                         [("broker-a.example", 1883, ["user", "pass"])])

    def test_invalid_mask_entries_are_skipped(self):
        self._write({
            "broker_list": [["broker-a.example", 1883], ["broker-b.example", 1884]],
            "broker_list_no": {
                "broker-a.example": ["pty"],          # 无 port
                ":1884": ["pty"],                     # 无 host
                "broker-b.example:99999": ["pty"],    # port 越界
                "broker-a.example:1883": "pty",       # 值不是列表
            },
        })

        self.assertEqual(
            server_mqtt.load_broker_list(path=self.config_path, module="pty"),
            [("broker-a.example", 1883), ("broker-b.example", 1884)])

    def test_broken_file_falls_back_to_builtin(self):
        self.config_path.write_text("{ not json", encoding="utf-8")
        result = server_mqtt.load_broker_list(path=self.config_path)
        self.assertEqual(result, server_mqtt._normalize_broker_list(
            server_mqtt.BROKER_LIST, "broker_list"))

        missing = Path(self.temp_dir.name) / "missing.json"
        self.assertEqual(server_mqtt.load_broker_list(path=missing), result)

    def test_write_back_preserves_broker_list_no(self):
        """server 端配置写回以文件原始键为准，不得丢掉 broker_list_no。"""
        self._write({
            "request_topic": "old/request",
            "broker_list_no": {"broker-b.example:1884": ["pty"]},
        })

        server_mqtt._write_rpc_config_updates({"request_topic": "new/request"},
                                              self.config_path)

        stored = json.loads(self.config_path.read_text(encoding="utf-8"))
        self.assertEqual(stored["broker_list_no"],
                         {"broker-b.example:1884": ["pty"]})
        self.assertEqual(stored["request_topic"], "new/request")

    def test_filter_broker_list_applies_mask_to_explicit_list(self):
        self._write({"broker_list_no": {"broker-b.example:1884": ["server"]}})
        brokers = [("broker-a.example", 1883), ("broker-b.example", 1884)]
        self.assertEqual(
            server_mqtt.filter_broker_list(brokers, "server",
                                           path=self.config_path),
            [("broker-a.example", 1883)])
        self.assertEqual(
            server_mqtt.filter_broker_list(brokers, None, path=self.config_path),
            brokers)

    def test_manager_default_uses_channel_and_explicit_brokers_win(self):
        from multi_mqtt import MultiMQTTManager

        mgr = MultiMQTTManager(enable_stats=False, log_connection=False,
                               recovery_enabled=False)
        self.assertEqual(mgr.brokers, server_mqtt.load_broker_list())
        self.assertEqual(MultiMQTTManager(brokers=[], enable_stats=False,
                                          log_connection=False,
                                          recovery_enabled=False).brokers, [])


class RequestTopicMapTests(unittest.TestCase):
    """request_topic_map 解析链：显式 > map[设备id] > request_topic > 兜底。

    多机共用一份 rpc.json 时按 U.get_machine_id() 的设备 id 区别各机器
    topic；map 命中只读不写回文件，避免共用配置被本机 topic 污染。
    全部用例只读写临时目录里的 rpc.json，绝不碰仓库 rpc.json。
    """

    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.config_path = Path(self.temp_dir.name) / "rpc.json"

    def tearDown(self):
        self.temp_dir.cleanup()

    def _write(self, document):
        self.config_path.write_text(json.dumps(document), encoding="utf-8")

    def _mock_machine(self, machine_id):
        module = types.ModuleType("U")
        if isinstance(machine_id, BaseException):
            module.get_machine_id = mock.Mock(side_effect=machine_id)
        else:
            module.get_machine_id = mock.Mock(return_value=machine_id)
        patcher = mock.patch.dict(sys.modules, {"U": module})
        return patcher, module

    def test_map_hit_overrides_request_topic_without_write_back(self):
        self._write({
            "request_topic": "x230",
            "request_topic_map": {"TestDevice_123456": "lenovo"},
        })
        patcher, _ = self._mock_machine("TestDevice_123456")
        with patcher:
            self.assertEqual(
                rpc_config._resolve_request_topic(path=self.config_path),
                "lenovo")
            self.assertEqual(
                rpc_config.resolve_client_request_topic(path=self.config_path),
                "lenovo")
        stored = json.loads(self.config_path.read_text(encoding="utf-8"))
        self.assertEqual(stored["request_topic"], "x230")   # 命中不写回

    def test_map_miss_falls_back_to_request_topic(self):
        self._write({
            "request_topic": "x230",
            "request_topic_map": {"OtherDevice_999999": "other"},
        })
        patcher, _ = self._mock_machine("TestDevice_123456")
        with patcher:
            self.assertEqual(
                rpc_config._resolve_request_topic(path=self.config_path),
                "x230")
            self.assertEqual(
                rpc_config.resolve_client_request_topic(path=self.config_path),
                "x230")

    def test_explicit_topic_wins_over_map(self):
        self._write({"request_topic_map": {"TestDevice_123456": "lenovo"}})
        patcher, module = self._mock_machine(AssertionError("must not run"))
        with patcher:
            self.assertEqual(
                rpc_config._resolve_request_topic("hi", path=self.config_path),
                "hi")
            self.assertEqual(
                rpc_config.resolve_client_request_topic(
                    "hi", path=self.config_path),
                "hi")
        self.assertFalse(module.get_machine_id.called)

    def test_map_absent_keeps_machine_id_persist_fallback(self):
        # 零回归：没有 map 时保持原有"首启生成设备 topic 并持久化"行为
        patcher, _ = self._mock_machine("TestDevice_123456")
        with patcher:
            self.assertEqual(
                rpc_config._resolve_request_topic(path=self.config_path),
                "TestDevice_123456")
        self.assertEqual(
            json.loads(self.config_path.read_text(encoding="utf-8"))
            ["request_topic"], "TestDevice_123456")

    def test_client_default_without_config_is_builtin(self):
        # 客户端链不生成机器 ID、不写文件
        patcher, module = self._mock_machine(AssertionError("must not run"))
        with patcher:
            self.assertEqual(
                rpc_config.resolve_client_request_topic(path=self.config_path),
                rpc_config.REQUEST_TOPIC)
        self.assertFalse(module.get_machine_id.called)
        self.assertFalse(self.config_path.exists())

    def test_invalid_map_entries_are_skipped(self):
        self._write({
            "request_topic": "x230",
            "request_topic_map": {
                "": "lenovo",                        # 空设备 id
                "TestDevice_123456": "  ",           # 空 topic
                "Other_1": 5,                        # 非字符串 topic
            },
        })
        patcher, _ = self._mock_machine("TestDevice_123456")
        with patcher:
            self.assertEqual(
                rpc_config._resolve_request_topic(path=self.config_path),
                "x230")

        self._write({
            "request_topic": "x230",
            "request_topic_map": "not-an-object",    # 整体非法按空处理
        })
        with patcher:
            self.assertEqual(
                rpc_config.resolve_client_request_topic(path=self.config_path),
                "x230")

    def test_server_cli_chain_applies_map(self):
        self._write({
            "request_topic": "x230",
            "request_topic_map": {"TestDevice_123456": "lenovo"},
        })
        with mock.patch.object(server_mqtt, "RPC_CONFIG_PATH", self.config_path):
            patcher, _ = self._mock_machine("TestDevice_123456")
            with patcher:
                config = server_mqtt._rpc_config_with_cli_overrides()
        self.assertEqual(config["request_topic"], "lenovo")


class BrokerAddForceTests(unittest.TestCase):
    """CLI --broker_add 强制加入：绕过 broker_list_no 屏蔽与 broker_list_del 删除。

    全部用例只读写临时目录里的 rpc.json，绝不碰仓库 rpc.json。
    """

    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.config_path = Path(self.temp_dir.name) / "rpc.json"

    def tearDown(self):
        self.temp_dir.cleanup()

    def _write(self, document):
        self.config_path.write_text(json.dumps(document), encoding="utf-8")

    def test_parse_broker_add(self):
        self.assertEqual(rpc_config.parse_broker_add("192.168.1.20:1883"),
                         ("192.168.1.20", 1883))
        self.assertEqual(rpc_config.parse_broker_add("broker.example"),
                         ("broker.example", 1883))
        self.assertIsNone(rpc_config.parse_broker_add(""))
        self.assertIsNone(rpc_config.parse_broker_add("host:abc"))
        self.assertIsNone(rpc_config.parse_broker_add("host:99999"))
        self.assertIsNone(rpc_config.parse_broker_add(":1883"))

    def test_parse_broker_add_dsn(self):
        self.assertEqual(
            rpc_config.parse_broker_add("mqtt://appID:secretKey@bemfa.com:9501"),
            ("bemfa.com", 9501, ["appID", "secretKey"]))
        self.assertEqual(
            rpc_config.parse_broker_add("appID:secretKey@bemfa.com"),
            ("bemfa.com", 1883, ["appID", "secretKey"]))
        self.assertEqual(
            rpc_config.parse_broker_add("token@bemfa.com:9501"),
            ("bemfa.com", 9501, ["token", ""]))
        self.assertEqual(
            rpc_config.parse_broker_add("mqtt://[2001:db8::1]:1883"),
            ("2001:db8::1", 1883))
        # 密码里的冒号按 URI 规则写成 %3A
        self.assertEqual(
            rpc_config.parse_broker_add("user:p%3Ass@host:1883"),
            ("host", 1883, ["user", "p:ss"]))
        self.assertIsNone(rpc_config.parse_broker_add("mqtts://host:1883"))
        self.assertIsNone(rpc_config.parse_broker_add("@host:1883"))

    def test_empty_auth_list_equals_no_auth(self):
        # ["host", port, []] 等价于 ["host", port]，不再判认证信息非法
        self.assertEqual(
            rpc_config._normalize_broker_list(
                [["192.168.1.20", 1883, []]], "broker_list_add"),
            [("192.168.1.20", 1883)])
        self._write({
            "broker_list_add": [["192.168.1.20", 1883, []]],
        })
        self.assertIn(
            ("192.168.1.20", 1883),
            server_mqtt.load_broker_list(path=self.config_path, module="pty"))
        # 单元素认证列表仍属非法
        with self.assertRaises(ValueError):
            rpc_config._normalize_broker_list(
                [["192.168.1.20", 1883, ["user"]]], "broker_list_add")

    def test_forced_add_bypasses_mask_and_del(self):
        self._write({
            "broker_list": [["broker-a.example", 1883], ["broker-b.example", 1884]],
            "broker_list_del": [["broker-a.example", 1883]],
            "broker_list_no": {"broker-b.example:1884": ["pty"]},
        })
        self.assertEqual(
            server_mqtt.load_broker_list(path=self.config_path, module="pty"),
            [])
        self.assertEqual(
            server_mqtt.load_broker_list(
                path=self.config_path, module="pty",
                broker_add=["broker-a.example:1883", "broker-b.example:1884",
                            "broker-c.example:1885"]),
            [("broker-a.example", 1883), ("broker-b.example", 1884),
             ("broker-c.example", 1885)])

    def test_forced_add_restores_masked_entry_with_auth(self):
        self._write({
            "broker_list": [["broker-a.example", 1883]],
            "broker_list_add": [["broker-a.example", 1883, ["user", "pass"]]],
            "broker_list_no": {"broker-a.example:1883": ["pty"]},
        })
        self.assertEqual(
            server_mqtt.load_broker_list(path=self.config_path, module="pty"),
            [])
        # 同身份补回合成表原条目（保留认证），host 大小写不敏感
        self.assertEqual(
            server_mqtt.load_broker_list(
                path=self.config_path, module="pty",
                broker_add=["BROKER-A.example:1883"]),
            [("broker-a.example", 1883, ["user", "pass"])])

    def test_forced_add_keeps_existing_entry_untouched(self):
        self._write({
            "broker_list": [["broker-a.example", 1883, ["user", "pass"]]],
        })
        self.assertEqual(
            server_mqtt.load_broker_list(
                path=self.config_path,
                broker_add=["broker-a.example:1883", "broker-a.example"]),
            [("broker-a.example", 1883, ["user", "pass"])])   # 不重复追加纯条目

    def test_forced_add_skips_invalid_entries(self):
        self._write({"broker_list": [["broker-a.example", 1883]]})
        self.assertEqual(
            server_mqtt.load_broker_list(
                path=self.config_path,
                broker_add=["", "host:abc", "broker-b.example:1884"]),
            [("broker-a.example", 1883), ("broker-b.example", 1884)])

    def test_manager_forces_broker_add_into_default_channel(self):
        from multi_mqtt import MultiMQTTManager

        mgr = MultiMQTTManager(enable_stats=False, log_connection=False,
                               recovery_enabled=False,
                               broker_add=["forced.example:1883"])
        self.assertEqual(mgr.brokers[-1], ("forced.example", 1883))


class JsonPreserveWriteTests(unittest.TestCase):
    """rpc.json 文本级按键改写：只动被改键一行，其余字节原样保留。

    全部用例只读写临时目录里的 rpc.json，绝不碰仓库 rpc.json。
    """

    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.config_path = Path(self.temp_dir.name) / "rpc.json"

    def tearDown(self):
        self.temp_dir.cleanup()

    def _write_text(self, text):
        self.config_path.write_text(text, encoding="utf-8")

    def test_update_existing_key_keeps_other_bytes(self):
        text = ('{\n'
                '\t"a": 1,\n'
                '    "b" :   [1, {"x": "y,z}"}],\n'
                '  "c": "keep"\n'
                '}\n')
        self._write_text(text)
        rpc_config.update_json_document(self.config_path, {"a": 2})
        rpc_config.update_json_document(self.config_path, {"c": "new"})
        self.assertEqual(
            self.config_path.read_text(encoding="utf-8"),
            text.replace('\t"a": 1,', '\t"a": 2,')
                .replace('"c": "keep"', '"c": "new"'))

    def test_update_nested_value_stays_one_line(self):
        text = '{\n  "a": 1,\n  "b": [\n    1,\n    2\n  ]\n}\n'
        self._write_text(text)
        rpc_config.update_json_document(self.config_path, {"b": [9]})
        self.assertEqual(self.config_path.read_text(encoding="utf-8"),
                         '{\n  "a": 1,\n  "b": [9]\n}\n')

    def test_add_key_only_appends_one_line(self):
        text = '{\n  "a": 1\n}\n'
        self._write_text(text)
        rpc_config.update_json_document(self.config_path, {"b": 2})
        result = self.config_path.read_text(encoding="utf-8")
        self.assertEqual(result, '{\n  "a": 1,\n  "b": 2\n}\n')
        self.assertEqual(result.count("\n"), text.count("\n") + 1)

    def test_add_key_with_comma_first_style_line(self):
        # 逗号开头行等手写风格：新键仍独占一行，缩进沿用末条目行首空白
        text = '{\n  "a": 1\n  ,"b": {\n    "x": 1\n  }\n}\n'
        self._write_text(text)
        rpc_config.update_json_document(self.config_path, {"c": 3})
        result = self.config_path.read_text(encoding="utf-8")
        self.assertEqual(result,
                         '{\n  "a": 1\n  ,"b": {\n    "x": 1\n  },\n  "c": 3\n}\n')
        self.assertEqual(result.count("\n"), text.count("\n") + 1)

    def test_add_key_to_single_line_file_stays_inline(self):
        text = '{"a": 1}'
        self._write_text(text)
        rpc_config.update_json_document(self.config_path, {"b": 2})
        self.assertEqual(self.config_path.read_text(encoding="utf-8"),
                         '{"a": 1, "b": 2}')

    def test_delete_key_only_removes_its_line(self):
        text = '{\n  "a": 1,\n  "b": 2,\n  "c": 3\n}\n'
        self._write_text(text)
        rpc_config.update_json_document(self.config_path, {}, deletes=("b",))
        self.assertEqual(self.config_path.read_text(encoding="utf-8"),
                         '{\n  "a": 1,\n  "c": 3\n}\n')

    def test_write_rpc_config_updates_preserves_formatting(self):
        text = '{\n\t"request_topic": "x230",\n    "pub_key" : "abc"\n}\n'
        self._write_text(text)
        rpc_config._write_rpc_config_updates(
            {"request_topic": "new/topic"}, self.config_path)
        self.assertEqual(
            self.config_path.read_text(encoding="utf-8"),
            text.replace('"request_topic": "x230"',
                         '"request_topic": "new/topic"'))

    def test_machine_id_fallback_only_adds_one_line(self):
        text = '{\n  "pub_key": "abc"\n}\n'
        self._write_text(text)
        module = types.ModuleType("U")
        module.get_machine_id = mock.Mock(return_value="TestDevice_123456")
        with mock.patch.dict(sys.modules, {"U": module}):
            self.assertEqual(
                rpc_config._resolve_request_topic(path=self.config_path),
                "TestDevice_123456")
        self.assertEqual(
            self.config_path.read_text(encoding="utf-8"),
            '{\n  "pub_key": "abc",\n'
            '  "request_topic": "TestDevice_123456"\n}\n')

    def test_invalid_file_rejected_without_force(self):
        # 非空错误文件默认拒绝覆盖：抛 ValueError 且文件一个字节不动
        text = '{ broken json'
        self._write_text(text)
        with self.assertRaises(ValueError):
            rpc_config.update_json_document(self.config_path, {"a": 1})
        self.assertEqual(self.config_path.read_text(encoding="utf-8"), text)

    def test_invalid_file_full_write_with_force(self):
        self._write_text('{ broken json')
        rpc_config.update_json_document(self.config_path, {"a": 1},
                                        force_write=True)
        self.assertEqual(
            json.loads(self.config_path.read_text(encoding="utf-8")),
            {"a": 1})

    def test_empty_file_written_without_force(self):
        self._write_text('')
        rpc_config.update_json_document(self.config_path, {"a": 1})
        self.assertEqual(
            json.loads(self.config_path.read_text(encoding="utf-8")), {"a": 1})

    def test_missing_file_written_without_force(self):
        rpc_config.update_json_document(self.config_path, {"a": 1})
        self.assertEqual(
            json.loads(self.config_path.read_text(encoding="utf-8")), {"a": 1})

    def test_write_rpc_config_updates_force_passthrough(self):
        self._write_text('{ broken json')
        with self.assertRaises(ValueError):
            rpc_config._write_rpc_config_updates(
                {"request_topic": "t"}, self.config_path)
        rpc_config._write_rpc_config_updates(
            {"request_topic": "t"}, self.config_path, force_write=True)
        self.assertEqual(
            json.loads(self.config_path.read_text(encoding="utf-8")),
            {"request_topic": "t"})

    def test_set_rpc_config_force_write_parameter(self):
        # set_rpc_config 默认拒绝覆盖非空错误文件，force_write=True 才整文件写
        text = '{ broken json'
        self._write_text(text)
        with mock.patch.object(server_mqtt, "gms", None), \
                mock.patch.object(server_mqtt, "_RPC_SERVERS",
                                  weakref.WeakSet()):
            with self.assertRaises(ValueError):
                server_mqtt.set_rpc_config(
                    path=self.config_path, request_topic="t/request")
            self.assertEqual(self.config_path.read_text(encoding="utf-8"), text)
            server_mqtt.set_rpc_config(
                path=self.config_path, force_write=True,
                request_topic="t/request")
        self.assertEqual(
            json.loads(self.config_path.read_text(encoding="utf-8")),
            {"request_topic": "t/request"})


if __name__ == "__main__":
    unittest.main()
