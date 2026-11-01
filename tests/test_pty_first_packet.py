#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""first_packet_stats（pty 首包统计）的纯内存回归测试。

不连任何 broker：用 FakeNet / FakeClient 复现
- publish_broadcast → 每 broker 的 publish(mid) → on_publish 竞速
- message_callback 下行首包（请求 req_id / PTY seq 去重 / hb 延迟）
- 脏输入（非 dict、broker=None、坏 hb）不得抛异常
- get_report(reset=True) 读完清零
运行：
    cd multi_mqtt && python -m unittest tests.test_pty_first_packet -v
"""
import os
import sys
import threading
import time
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from client import pty_client_mqtt as pcm  # noqa: E402

HOSTS = ("broker-a", "broker-b", "broker-c")


class _Info:
    def __init__(self, mid, rc=0):
        self.mid = mid
        self.rc = rc


class _FakeClient:
    """最小 paho client：publish 返回 mid，on_publish 由测试手动触发。"""

    def __init__(self, host):
        self.host = host
        self.mid = 0
        self.next_rc = 0
        self.published = []
        self.on_publish = None

    def is_connected(self):
        return True

    def publish(self, topic, payload=None, qos=0, retain=False, properties=None):
        self.mid += 1
        info = _Info(self.mid, rc=self.next_rc)
        self.published.append((topic, qos, info.mid))
        return info

    def ack(self, mid):
        if self.on_publish is not None:
            self.on_publish(self, self.host, mid, None, None)


class _FakeNet:
    def __init__(self):
        self.lock = threading.Lock()
        self.clients = {h: _FakeClient(h) for h in HOSTS}
        self.message_callback = None
        self._cmq_stream_installed = False

    def set_on_message(self, cb):
        self.message_callback = cb

    def publish_broadcast(self, topic, payload_dict, **kw):
        published = 0
        with self.lock:
            items = list(self.clients.items())
        for host, c in items:
            if c.is_connected():
                info = c.publish(topic, payload_dict, qos=kw.get("qos", 0))
                if int(info.rc) == 0:
                    published += 1
        return published

    def downlink(self, topic, data, broker):
        if self.message_callback is not None:
            self.message_callback(topic, data, broker)


class _FakeNode:
    def __init__(self):
        self.lock = threading.Lock()
        self.pending_requests = {}


class _FakeTransport:
    def __init__(self):
        self.node = _FakeNode()
        self.node.mqtt_net = _FakeNet()


class FirstPacketStatsTests(unittest.TestCase):
    def setUp(self):
        self.tr = _FakeTransport()
        self.net = self.tr.node.mqtt_net
        self.stats = pcm.FirstPacketStats()
        self.assertTrue(self.stats.install(self.tr))
        self.assertTrue(self.stats._installed)

    def test_install_is_idempotent_and_needs_net(self):
        self.assertFalse(self.stats.install(self.tr))
        self.assertFalse(pcm.FirstPacketStats().install(object()))

    def test_uplink_first_broker_is_first_on_publish(self):
        net = self.net
        net.publish_broadcast("sys/device/request", {"x": 1})
        mids = {h: net.clients[h].published[-1][2] for h in HOSTS}
        # broker-b 的 on_publish 最先完成 -> 上行首包赢家
        net.clients["broker-b"].ack(mids["broker-b"])
        net.clients["broker-a"].ack(mids["broker-a"])
        net.clients["broker-c"].ack(mids["broker-c"])
        rep = self.stats.get_report()
        up = rep["uplink_publish"]
        self.assertEqual(up["calls"], 1)
        self.assertEqual(up["first_packet_broker"], "broker-b")
        self.assertEqual(up["brokers_queued"]["n"], 1)
        self.assertEqual(up["brokers_queued"]["min"], 3)
        self.assertEqual(up["send_ms"]["n"], 3)
        self.assertLessEqual(up["send_ms"]["min"], up["send_ms"]["max"])
        rows = {r["broker"]: r for r in up["broker_distribution"]}
        self.assertEqual(rows["broker-b"]["wins"], 1)
        self.assertEqual(rows["broker-b"]["queued"], 1)
        self.assertEqual(rows["broker-a"]["wins"], 0)
        self.assertEqual(rep["in_flight"]["mids"], 0)
        self.assertEqual(rep["in_flight"]["batches"], 0)

    def test_uplink_failed_publish_not_counted_as_queued(self):
        net = self.net
        net.clients["broker-a"].next_rc = 4   # 包装器内的原 publish 返回失败
        net.publish_broadcast("t", {"x": 1})
        net.clients["broker-a"].next_rc = 0
        rep = self.stats.get_report()
        rows = {r["broker"]: r for r in
                rep["uplink_publish"]["broker_distribution"]}
        self.assertEqual(rows["broker-a"]["queued"], 0)
        self.assertEqual(rows["broker-a"]["failed"], 1)
        self.assertEqual(rep["uplink_publish"]["brokers_queued"]["min"], 2)

    def test_downlink_request_first_packet_and_latency(self):
        net = self.net
        start = time.perf_counter() - 0.050
        with self.tr.node.lock:
            self.tr.node.pending_requests["req-1"] = {"start_time": start}
        net.downlink("sys/device/response", {"req_id": "req-1"}, "broker-c")
        rep = self.stats.get_report()
        req = rep["requests"]
        self.assertEqual(req["n"], 1)
        self.assertEqual(req["first_packet_broker"], "broker-c")
        self.assertEqual(req["latency_ms"]["n"], 1)
        self.assertGreaterEqual(req["latency_ms"]["min"], 40.0)
        self.assertLess(req["latency_ms"]["max"], 5000.0)

    def test_downlink_pty_seq_wins_dups_and_hb(self):
        net = self.net
        net.downlink("pty/out", {"pty": "s1", "seq": 1, "d": ""}, "broker-b")
        net.downlink("pty/out", {"pty": "s1", "seq": 1, "d": ""}, "broker-a")
        net.downlink("pty/out", {"pty": "s1", "seq": 2, "d": ""}, "broker-a")
        net.downlink("pty/out", {"pty": "s1", "hb": 1}, "broker-a")
        time.sleep(0.03)
        net.downlink("pty/out", {"pty": "s1", "hb": 2}, "broker-a")
        rep = self.stats.get_report()
        pf = rep["pty_frames"]
        self.assertEqual(pf["unique"], 2)
        self.assertEqual(pf["duplicates"], 1)
        rows = {r["broker"]: r for r in pf["broker_distribution"]}
        self.assertEqual(rows["broker-b"]["wins"], 1)
        self.assertEqual(rows["broker-b"]["dups"], 0)   # 重复副本由 a 先到
        self.assertEqual(rows["broker-a"]["wins"], 1)
        self.assertEqual(rows["broker-a"]["dups"], 1)
        self.assertEqual(rows["broker-a"]["rx"], 4)
        self.assertEqual(rows["broker-a"]["hb_frames"], 2)
        self.assertEqual(rows["broker-a"]["hb_gap_ms"]["n"], 1)
        self.assertGreaterEqual(rows["broker-a"]["hb_gap_ms"]["min"], 20.0)

    def test_heartbeat_gap_uses_local_clock_only(self):
        """hb 里塞离谱的远端时间戳也不影响统计：只按本地到达间隔计算。"""
        net = self.net
        net.downlink("pty/out", {"pty": "s1", "hb": 10 ** 15}, "broker-a")
        time.sleep(0.03)
        net.downlink("pty/out", {"pty": "s1", "hb": -10 ** 15}, "broker-b")
        time.sleep(0.03)
        net.downlink("pty/out", {"pty": "s1", "hb": "not-a-number"},
                     "broker-a")
        rep = self.stats.get_report()
        pf = rep["pty_frames"]
        self.assertNotIn("hb_baseline_ms", pf)
        rows = {r["broker"]: r for r in pf["broker_distribution"]}
        self.assertEqual(rows["broker-a"]["hb_frames"], 2)
        self.assertEqual(rows["broker-a"]["hb_gap_ms"]["n"], 1)
        self.assertGreaterEqual(rows["broker-a"]["hb_gap_ms"]["min"], 20.0)
        self.assertLess(rows["broker-a"]["hb_gap_ms"]["max"], 500.0)
        self.assertEqual(rows["broker-b"]["hb_frames"], 1)

    def test_dirty_inputs_never_raise(self):
        net = self.net
        net.downlink("x", "not-a-dict", "broker-a")
        net.downlink("x", {"pty": "s1", "hb": "bad"}, None)
        net.downlink("x", {"pty": "s1", "seq": 1}, "")
        net.downlink("x", {"pty": "s1", "hb": "bad"}, "broker-a")
        self.stats.get_report()

    def test_reset_clears_all_counters(self):
        self.net.publish_broadcast("t", {"x": 1})
        self.stats.get_report(reset=True)
        rep = self.stats.get_report()
        self.assertEqual(rep["requests"]["n"], 0)
        self.assertEqual(rep["pty_frames"]["unique"], 0)
        self.assertEqual(rep["uplink_publish"]["calls"], 0)
        self.assertEqual(rep["uplink_publish"]["broker_distribution"], [])
        self.assertIsNone(rep["uplink_publish"]["first_packet_broker"])


if __name__ == "__main__":
    unittest.main()
