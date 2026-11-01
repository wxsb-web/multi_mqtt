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
        self.brokers = [(h, 1883) for h in HOSTS]
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
        # pending_sum 必须与在途 mid 数一致，否则说明有批次没被释放
        self.assertEqual(rep["in_flight"]["pending_sum"], 0)

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

    def test_heartbeat_duplicate_delivery_is_deduped(self):
        """同一帧在同一 broker 被投递两次：记 hb_dups，不污染到达间隔。"""
        net = self.net
        frame = {"pty": "s1", "hb": 123456}
        net.downlink("pty/out", frame, "broker-a")
        net.downlink("pty/out", dict(frame), "broker-a")
        rep = self.stats.get_report()
        rows = {r["broker"]: r for r in rep["pty_frames"]["broker_distribution"]}
        self.assertEqual(rows["broker-a"]["hb_frames"], 1)
        self.assertEqual(rows["broker-a"]["hb_dups"], 1)
        self.assertNotIn("hb_gap_ms", rows["broker-a"])

    def test_first_uplink_broker_not_overwritten_by_later_batches(self):
        """uplink first_packet_broker 取首个完成批次，不被后续批次覆盖。"""
        net = self.net
        net.publish_broadcast("t", {"x": 1})
        net.clients["broker-b"].ack(1)
        net.publish_broadcast("t", {"x": 2})
        net.clients["broker-a"].ack(2)
        rep = self.stats.get_report()
        self.assertEqual(rep["uplink_publish"]["first_packet_broker"],
                         "broker-b")
        # 自证来源：标出是哪一批、第几秒定下来的，而不是被后续批次覆盖
        self.assertEqual(rep["uplink_publish"]["first_packet_broker_batch"], 1)
        self.assertIsNotNone(
            rep["uplink_publish"]["first_packet_broker_at_s"])
        rows = {r["broker"]: r for r in
                rep["uplink_publish"]["broker_distribution"]}
        self.assertEqual(rows["broker-b"]["wins"], 1)
        self.assertEqual(rows["broker-a"]["wins"], 1)

    def test_stale_mid_is_reported_and_pruned_after_ttl(self):
        stats = self.stats
        now = time.perf_counter()
        # 年轻孤儿：先在报告里可见（它可能还在等自己的 ack）
        with stats._lock:
            stats._mid[("broker-a", 999)] = (424242, now)
        self.assertEqual(stats.get_report()["in_flight"]["stale_mids"], 1)
        # 超时孤儿：get_report 内部的 _prune 直接回收，不依赖上行流量
        with stats._lock:
            stats._mid[("broker-a", 999)] = (
                424242, now - stats._MID_TTL - 5)
        self.assertEqual(stats.get_report()["in_flight"]["stale_mids"], 0)
        with stats._lock:
            self.assertEqual(len(stats._mid), 0)

    def test_connectivity_counts_and_disconnected_hosts(self):
        net = self.net
        net.clients["broker-c"].is_connected = lambda: False
        c = self.stats.get_report()["connectivity"]
        self.assertEqual(c["configured"], 3)
        self.assertEqual(c["clients"], 3)
        self.assertEqual(c["connected"], 2)
        self.assertEqual(c["disconnected_hosts"], ["broker-c"])

    def test_foreign_reply_does_not_pollute_request_stats(self):
        """同一 reply topic 上其他设备的回包：只计 observed/unmatched。"""
        self.net.downlink("reply", {"req_id": "20260101_000000.000",
                                    "r": "1"}, "broker-a")
        rep = self.stats.get_report()["requests"]
        self.assertEqual(rep["n"], 0)
        self.assertEqual(rep["observed"], 1)
        self.assertEqual(rep["unmatched"], 1)
        rows = {r["broker"]: r for r in rep["broker_distribution"]}
        self.assertEqual(rows["broker-a"]["wins"], 0)
        self.assertEqual(rows["broker-a"]["unmatched"], 1)
        self.assertEqual(rep["latency_ms"]["n"], 0)

    def test_unmatched_reply_records_topic_broker_and_keys(self):
        """未匹配回包要能追到来源：topic 计数、broker、帧键名（不含内容）。"""
        net = self.net
        net.downlink("sys/device/response",
                     {"req_id": "20260101_000000.123", "ok": True,
                      "code": "print(1)"}, "broker-a")
        net.downlink("sys/device/response",
                     {"req_id": "20260101_000000.124"}, "broker-a")
        req = self.stats.get_report()["requests"]
        self.assertEqual(req["unmatched"], 2)
        self.assertEqual(req["unmatched_topics"], {"sys/device/response": 2})
        sample = req["unmatched_samples"][0]
        self.assertEqual(sample["topic"], "sys/device/response")
        self.assertEqual(sample["broker"], "broker-a")
        self.assertEqual(sample["keys"], ["code", "ok", "req_id"])
        self.assertNotIn("print(1)", str(req["unmatched_samples"]))
        rows = {r["broker"]: r for r in req["broker_distribution"]}
        self.assertEqual(rows["broker-a"]["unmatched"], 2)

    def test_unmatched_samples_are_bounded(self):
        for i in range(20):
            self.net.downlink("t/%d" % i, {"req_id": "r-%d" % i}, "broker-a")
        req = self.stats.get_report()["requests"]
        self.assertEqual(len(req["unmatched_samples"]), 5)
        self.assertLessEqual(len(req["unmatched_topics"]), 8)

    def test_prune_timeout_mid_also_releases_batch(self):
        """TTL 清理 mid 时必须同步扣 pending，否则批次永远挂着。"""
        self.net.publish_broadcast("t", {"x": 1})
        stats = self.stats
        with stats._lock:
            self.assertEqual(len(stats._batches), 1)
            old = time.perf_counter() - stats._MID_TTL - 5
            for k in list(stats._mid):
                bid, _t0 = stats._mid[k]
                stats._mid[k] = (bid, old)
            stats._prune()
            self.assertEqual(len(stats._mid), 0)
            self.assertEqual(len(stats._batches), 0)
        rep = stats.get_report()["in_flight"]
        self.assertEqual(rep, {"batches": 0, "mids": 0,
                               "pending_sum": 0, "stale_mids": 0,
                               "orphan_writes": 0})

    def test_ack_before_remaining_publishes_does_not_orphan_mid(self):
        """ack 抢在循环 publish 完其余 broker 之前返回，不得提前删批次。

        旧实现里先到的 ack 把 pending 归零后立刻 pop 批次，后续 publish
        写出的 mid 就指向不存在的批次（现场 stale_mids=3 的成因）。
        """
        net = self.net
        orig_pub = net.clients["broker-a"].publish

        def instant_publish(topic, payload=None, qos=0, retain=False,
                            properties=None):
            info = orig_pub(topic, payload, qos=qos, retain=retain,
                            properties=properties)
            net.clients["broker-a"].ack(info.mid)   # QoS0 立即写出
            return info

        net.clients["broker-a"].publish = instant_publish
        net.publish_broadcast("t", {"x": 1})
        fl = self.stats.get_report()["in_flight"]
        self.assertEqual(fl["stale_mids"], 0)
        self.assertEqual(fl["orphan_writes"], 0)
        self.assertEqual(fl["mids"], 2)          # b/c 还没 ack
        self.assertEqual(fl["pending_sum"], 2)   # 与在途 mid 数一致
        self.assertEqual(fl["batches"], 1)

    def test_batch_table_eviction_drops_its_orphan_mids(self):
        """批次表超限淘汰时，必须连同它的在途 mid 一起丢弃。"""
        stats = self.stats
        stats._BATCH_MAX = 4
        with stats._lock:
            now = time.perf_counter()
            for i in range(1, 7):
                stats._batches[i] = {"t0": now, "pending": 1, "win": None,
                                     "published": None, "done": False}
                stats._mid[("h%d" % i, 1)] = (i, now)
            stats._prune()
            self.assertEqual(len(stats._batches), 4)
            self.assertEqual(len(stats._mid), 4)
        fl = stats.get_report()["in_flight"]
        self.assertEqual(fl["stale_mids"], 0)
        self.assertEqual(fl["mids"], 4)
        self.assertEqual(fl["pending_sum"], 4)

    def test_uplink_coverage_counts_every_connected_host(self):
        self.net.publish_broadcast("t", {"x": 1})
        self.net.publish_broadcast("t", {"x": 2})
        cov = self.stats.get_report()["uplink_publish"]["coverage"]
        self.assertEqual(cov["predicted"], 6)
        self.assertEqual(cov["attempted"], 6)
        self.assertEqual(cov["seen"], 6)
        self.assertEqual(cov["blind"], 0)
        self.assertEqual(cov["blind_hosts"], {})
        self.assertEqual(cov["late"], 0)
        self.assertEqual(cov["late_hosts"], {})
        self.assertEqual(cov["offline"], 0)
        self.assertEqual(cov["offline_hosts"], {})
        self.assertEqual(cov["offline_samples"], [])
        # 恒等式：attempted == seen + blind，且 seen 永远是 attempted 的子集
        self.assertLessEqual(cov["seen"], cov["attempted"])
        self.assertEqual(cov["attempted"], cov["seen"] + cov["blind"])

    def test_uplink_offline_host_is_counted_not_blind(self):
        """在 clients 里但 is_connected() 为假：记 offline，不是 blind。"""
        net = self.net
        net.clients["broker-c"].is_connected = lambda: False
        net.downlink("pty/out", {"pty": "s1", "hb": 1}, "broker-c")
        net.publish_broadcast("t", {"x": 1})
        cov = self.stats.get_report()["uplink_publish"]["coverage"]
        self.assertEqual(cov["predicted"], 2)
        self.assertEqual(cov["attempted"], 2)
        self.assertEqual(cov["seen"], 2)
        self.assertEqual(cov["blind"], 0)
        self.assertEqual(cov["offline"], 1)
        self.assertEqual(cov["offline_hosts"], {"broker-c": 1})
        # offline 现场必须记下 host/state/socket/rx_age_s，用来区分真掉线与
        # 「刚收过下行却报离线」的状态错位。
        sample = cov["offline_samples"][0]
        self.assertEqual(sample["host"], "broker-c")
        self.assertIn("state", sample)
        self.assertIn("socket", sample)
        self.assertGreaterEqual(sample["rx_age_s"], 0.0)

    def test_uplink_late_host_keeps_seen_subset_of_attempted(self):
        """预测时离线、真正扇出时已连上：不得造成 seen > attempted。"""
        c = self.net.clients["broker-c"]
        calls = {"n": 0}

        def flaky_connected():
            calls["n"] += 1
            return calls["n"] > 1     # 预测=离线，orig 扇出=在线

        c.is_connected = flaky_connected
        self.net.publish_broadcast("t", {"x": 1})
        cov = self.stats.get_report()["uplink_publish"]["coverage"]
        self.assertEqual(cov["predicted"], 2)
        self.assertEqual(cov["seen"], 3)
        self.assertEqual(cov["attempted"], 3)   # 旧口径下这里会是 2 -> seen>attempted
        self.assertEqual(cov["blind"], 0)
        self.assertEqual(cov["late"], 1)
        self.assertEqual(cov["late_hosts"], {"broker-c": 1})
        self.assertEqual(cov["offline"], 1)     # 预测时刻确实离线
        self.assertEqual(cov["offline_hosts"], {"broker-c": 1})
        self.assertLessEqual(cov["seen"], cov["attempted"])
        self.assertEqual(cov["attempted"], cov["seen"] + cov["blind"])

    def test_uplink_blind_host_is_detected(self):
        """client 对象被重建/未包装时，publish 不再经过包装器。

        此时该 host 有下行、却永远没有上行样本；coverage.blind_hosts
        必须点名它，而不是让它从列表里静默消失。
        """
        net = self.net
        c = net.clients["broker-a"]

        def raw_publish(topic, payload=None, qos=0, retain=False,
                        properties=None):
            c.mid += 1
            return _Info(c.mid)

        # 模拟：publish 已被换回原生方法，但 _fp_wrapped 仍是 True，
        # 因此下一次 _wrap_clients() 会跳过它。
        c.publish = raw_publish
        net.publish_broadcast("t", {"x": 1})
        rep = self.stats.get_report()["uplink_publish"]
        cov = rep["coverage"]
        self.assertEqual(cov["predicted"], 3)
        self.assertEqual(cov["attempted"], 3)
        self.assertEqual(cov["seen"], 2)
        self.assertEqual(cov["blind"], 1)
        self.assertEqual(cov["blind_hosts"], {"broker-a": 1})
        self.assertEqual(cov["late"], 0)
        self.assertEqual(cov["attempted"], cov["seen"] + cov["blind"])
        # broker-a 上行样本为 0，因此它会从 broker_distribution 里整行消失；
        # 这正是需要 blind_hosts 点名它的原因。
        rows = {r["broker"]: r for r in rep["broker_distribution"]}
        self.assertNotIn("broker-a", rows)

    def test_report_prunes_stale_mid_without_uplink_traffic(self):
        """get_report 自己触发一次 _prune，长时间没有上行也要能回收超时 mid。"""
        stats = self.stats
        now = time.perf_counter()
        with stats._lock:
            stats._batches[1] = {"t0": now, "pending": 1, "win": None,
                                 "published": None, "done": True,
                                 "attemptable": set(), "seen": set(),
                                 "absent": set()}
            stats._mid[("broker-a", 1)] = (1, now - stats._MID_TTL - 10)
        fl = stats.get_report()["in_flight"]
        self.assertEqual(fl["mids"], 0)
        self.assertEqual(fl["batches"], 0)
        self.assertEqual(fl["stale_mids"], 0)

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
        cov = rep["uplink_publish"]["coverage"]
        self.assertEqual(cov["predicted"], 0)
        self.assertEqual(cov["attempted"], 0)
        self.assertEqual(cov["late_hosts"], {})
        self.assertEqual(cov["offline_samples"], [])


if __name__ == "__main__":
    unittest.main()
