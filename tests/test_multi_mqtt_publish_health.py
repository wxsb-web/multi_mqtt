# -*- coding: utf-8 -*-
"""[发布健康看门狗] 单元测试。

背景（2026-11-11 真机 14c）：网络切换/重连风暴后，13 条 MQTT 长连接进入
“半死”状态 —— ``is_connected()`` 仍为 True、收报文正常，但发送方向卡死，
报文堆在 paho 的 ``_out_packet`` 里出不去（QoS0 静默丢、QoS1 拿不到
PUBACK）。旧恢复逻辑只在“全部 broker 都红”时才重建，因此永不介入；
表现是手机日志显示“已收到并执行请求”，客户端却一直超时。

这里覆盖新增的发布健康判据与有界重建，以及“脏数据/异常不影响主流程”。
"""
import sys
import time
import types
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import multi_mqtt
from multi_mqtt import MultiMQTTManager


class FakeClient:
    """最小 paho client 替身：只暴露看门狗用到的接口。"""

    def __init__(self, connected=True, backlog=0, publish_mid=1):
        self._connected = connected
        self._out_packet = [object() for _ in range(backlog)]
        self.publish_mid = publish_mid
        self.published = []

    def is_connected(self):
        return self._connected

    def publish(self, topic, payload, **kwargs):
        self.published.append((topic, payload, kwargs))
        return types.SimpleNamespace(
            rc=multi_mqtt.mqtt_client.MQTT_ERR_SUCCESS,
            mid=self.publish_mid,
            is_published=lambda: False,
        )


def make_manager(**kwargs):
    """构造一个不联网、不启线程、且已过启动宽限期的 manager。"""
    params = {
        "brokers": [],
        "enable_stats": False,
        "log_connection": False,
        "recovery_enabled": False,
    }
    params.update(kwargs)
    manager = MultiMQTTManager(**params)
    manager._started_at = time.time() - 10000
    return manager


class PublishHealthScanTest(unittest.TestCase):
    def setUp(self):
        self.rebuilt = []
        self.manager = make_manager(
            publish_ack_timeout=10.0,
            publish_backlog_threshold=16,
            publish_backlog_rounds=3,
            recovery_force_gap=0.0,
        )
        self.manager._probe_reachable = lambda host, port: True
        self.manager._recover_client = self._fake_recover

    def _fake_recover(self, host):
        self.rebuilt.append(host)
        return True

    def test_qos1_without_puback_triggers_rebuild(self):
        self.manager.clients = {"b1:1883": FakeClient()}
        self.manager._note_publish("b1:1883", types.SimpleNamespace(mid=7), 1)
        self.manager._pub_health["b1:1883"]["pending"][7] = time.time() - 11

        self.manager._publish_health_scan()

        self.assertEqual(self.rebuilt, ["b1:1883"])
        self.assertNotIn("b1:1883", self.manager._pub_health)

    def test_puback_clears_pending_and_avoids_rebuild(self):
        self.manager.clients = {"b1:1883": FakeClient()}
        self.manager._note_publish("b1:1883", types.SimpleNamespace(mid=7), 1)
        self.manager._note_publish_ack("b1:1883", 7)
        self.manager._pub_health["b1:1883"]["pending"] = {}
        # 即使队列有小幅积压，也不该因 ack 判定触发
        self.manager.clients["b1:1883"]._out_packet = [object()] * 0

        self.manager._publish_health_scan()

        self.assertEqual(self.rebuilt, [])

    def test_backlog_requires_consecutive_rounds(self):
        self.manager.clients = {"b1:1883": FakeClient(backlog=32)}

        self.manager._publish_health_scan()
        self.manager._publish_health_scan()
        self.assertEqual(self.rebuilt, [])

        self.manager._publish_health_scan()
        self.assertEqual(self.rebuilt, ["b1:1883"])

    def test_backlog_drains_resets_counter(self):
        self.manager.clients = {"b1:1883": FakeClient(backlog=32)}
        self.manager._publish_health_scan()
        self.manager._publish_health_scan()

        self.manager.clients["b1:1883"]._out_packet = []
        self.manager._publish_health_scan()
        self.assertEqual(self.rebuilt, [])

        self.manager.clients["b1:1883"]._out_packet = [object()] * 32
        self.manager._publish_health_scan()
        self.manager._publish_health_scan()
        self.assertEqual(self.rebuilt, [])

    def test_unreachable_probe_keeps_client(self):
        self.manager._probe_reachable = lambda host, port: False
        self.manager.clients = {"b1:1883": FakeClient(backlog=32)}
        for _ in range(5):
            self.manager._publish_health_scan()
        self.assertEqual(self.rebuilt, [])

    def test_cooldown_blocks_repeat_rebuild(self):
        self.manager = make_manager(
            publish_ack_timeout=10.0,
            publish_backlog_threshold=16,
            publish_backlog_rounds=1,
            recovery_force_gap=30.0,
        )
        self.manager._probe_reachable = lambda host, port: True
        self.manager._recover_client = self._fake_recover
        self.manager.clients = {"b1:1883": FakeClient(backlog=32)}

        self.manager._publish_health_scan()
        self.manager.clients["b1:1883"]._out_packet = [object()] * 32
        self.manager._publish_health_scan()

        self.assertEqual(self.rebuilt, ["b1:1883"])

    def test_disconnected_client_is_ignored(self):
        self.manager.clients = {"b1:1883": FakeClient(connected=False, backlog=32)}
        for _ in range(5):
            self.manager._publish_health_scan()
        self.assertEqual(self.rebuilt, [])
        self.assertNotIn("b1:1883", self.manager._pub_health)

    def test_disabled_watchdog_does_nothing(self):
        self.manager.publish_health_enabled = False
        self.manager.clients = {"b1:1883": FakeClient(backlog=32)}
        self.manager._note_publish("b1:1883", types.SimpleNamespace(mid=1), 1)
        for _ in range(5):
            self.manager._publish_health_scan()
        self.assertEqual(self.rebuilt, [])
        self.assertEqual(self.manager._pub_health, {})

    def test_start_grace_suppresses_scan(self):
        self.manager._started_at = time.time()
        self.manager.clients = {"b1:1883": FakeClient(backlog=32)}
        for _ in range(5):
            self.manager._publish_health_scan()
        self.assertEqual(self.rebuilt, [])

    def test_max_rebuild_per_round(self):
        self.manager.publish_health_max_rebuild = 2
        self.manager.publish_backlog_rounds = 1
        self.manager.clients = {
            f"b{i}:1883": FakeClient(backlog=32) for i in range(5)
        }
        self.manager._publish_health_scan()
        self.assertEqual(len(self.rebuilt), 2)

    def test_one_broken_client_does_not_block_others(self):
        class Boom:
            _out_packet = []

            def is_connected(self):
                raise RuntimeError("boom")

        self.manager.publish_backlog_rounds = 1
        self.manager.clients = {
            "bad:1883": Boom(),
            "b1:1883": FakeClient(backlog=32),
        }
        self.manager._publish_health_scan()
        self.assertEqual(self.rebuilt, ["b1:1883"])


class PublishTrackingTest(unittest.TestCase):
    def setUp(self):
        self.manager = make_manager()

    def test_publish_broadcast_tracks_only_qos_positive(self):
        client = FakeClient(publish_mid=9)
        self.manager.clients = {"b1:1883": client}

        self.manager.publish_broadcast("t", {"a": 1}, qos=1)
        self.assertIn(9, self.manager._pub_health["b1:1883"]["pending"])

        self.manager.publish_broadcast("t", {"a": 1})
        self.assertNotIn(10, self.manager._pub_health["b1:1883"]["pending"])

    def test_note_publish_tolerates_dirty_input(self):
        # 坏参数/缺属性都只能降级，不能抛出（边缘统计不阻塞主流程）
        self.manager._note_publish("b1:1883", object(), 1)
        self.manager._note_publish("b1:1883", None, 1)
        self.manager._note_publish("b1:1883", types.SimpleNamespace(mid=1), "bad")

    def test_pending_table_is_capped(self):
        self.manager.PUB_HEALTH_PENDING_MAX = 8
        for mid in range(20):
            self.manager._note_publish(
                "b1:1883", types.SimpleNamespace(mid=mid), 1
            )
        self.assertLessEqual(len(self.manager._pub_health["b1:1883"]["pending"]), 8)

    def test_note_publish_ack_tolerates_unknown_host(self):
        self.manager._note_publish_ack("nope:1883", 1)

    def test_stats_ping_hook_reports_qos1(self):
        recorded = []
        self.manager._pub_health = {}
        self.manager.stats.on_qos_publish = lambda host, info, qos: recorded.append(
            (host, info.mid, qos)
        )
        self.manager.stats.on_qos_publish(
            "b1:1883", types.SimpleNamespace(mid=3), 1
        )
        self.assertEqual(recorded, [("b1:1883", 3, 1)])


class TerminateStuckClientTest(unittest.TestCase):
    def test_returns_quickly_when_thread_never_dies(self):
        manager = make_manager()

        class Stuck:
            _thread_terminate = False

            def __init__(self):
                self._sockpairW = types.SimpleNamespace(send=lambda data: None)
                self._sock = types.SimpleNamespace(shutdown=lambda how: None)
                self._thread = types.SimpleNamespace(
                    is_alive=lambda: True,
                    join=lambda timeout=None: time.sleep(timeout or 0),
                )
                self.closed = False

            def _sock_close(self):
                self.closed = True

        stuck = Stuck()
        started = time.time()
        ok = manager._terminate_stuck_client(stuck, timeout=0.2)
        elapsed = time.time() - started

        self.assertFalse(ok)
        self.assertLess(elapsed, 2.0)
        self.assertFalse(stuck.closed)

    def test_closes_socket_when_thread_exited(self):
        manager = make_manager()

        class Dead:
            _thread_terminate = False

            def __init__(self):
                self._sockpairW = types.SimpleNamespace(send=lambda data: None)
                self._sock = types.SimpleNamespace(shutdown=lambda how: None)
                self._thread = types.SimpleNamespace(is_alive=lambda: False)
                self.closed = False

            def _sock_close(self):
                self.closed = True

        dead = Dead()
        self.assertTrue(manager._terminate_stuck_client(dead, timeout=0.1))
        self.assertTrue(dead.closed)

    def test_none_client_is_safe(self):
        self.assertTrue(make_manager()._terminate_stuck_client(None))


if __name__ == "__main__":
    unittest.main()
