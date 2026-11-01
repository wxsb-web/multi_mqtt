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
        self.next_sub_rc = 0
        self.published = []
        self.subscriptions = set()
        self.on_publish = None
        self.sync_ack = False   # True = on_publish 在 publish() 返回前触发

    def is_connected(self):
        return True

    def subscribe(self, topic, qos=0):
        if self.next_sub_rc == 0:
            self.subscriptions.add(topic)
        return self.next_sub_rc, 1

    def unsubscribe(self, topic):
        self.subscriptions.discard(topic)
        return 0, 1

    def publish(self, topic, payload=None, qos=0, retain=False, properties=None):
        self.mid += 1
        info = _Info(self.mid, rc=self.next_rc)
        self.published.append((topic, qos, info.mid))
        if self.sync_ack and self.on_publish is not None:
            # 模拟 paho 在 publish() 返回前就完成写出并回调 on_publish
            self.on_publish(self, self.host, info.mid, None, None)
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
        # manager 的规范订阅集合：统计器用它作为每条 client 的订阅计数种子。
        self.subscribed_topics = {"pty/out", "sys/device/response"}

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

    def test_subscription_count_tracks_live_filters_beside_hb_dups(self):
        """subscription_count 与 hb_dups 并列：新增/退订/失败订阅都要反映。"""
        net = self.net

        def row():
            return {r["broker"]: r for r in
                    self.stats.get_report()["pty_frames"][
                        "broker_distribution"]}["broker-a"]

        net.downlink("pty/out", {"pty": "s1", "hb": 1}, "broker-a")
        # 种子 = manager 的规范订阅集合（reply + pty 两条）
        self.assertEqual(row()["subscription_count"], 2)
        net.clients["broker-a"].subscribe("pty/#")      # 形态 A：多一个匹配 filter
        self.assertEqual(row()["subscription_count"], 3)
        net.clients["broker-a"].unsubscribe("pty/#")
        self.assertEqual(row()["subscription_count"], 2)
        net.clients["broker-a"].next_sub_rc = 1         # 订阅失败不计入
        net.clients["broker-a"].subscribe("pty/+")
        self.assertEqual(row()["subscription_count"], 2)

    def test_early_ack_is_matched_not_left_pending(self):
        """on_publish 早于登记：暂存并在登记时匹配，不再等 120s TTL。"""
        net = self.net
        net.clients["broker-a"].sync_ack = True
        net.publish_broadcast("t", {"x": 1})
        net.clients["broker-a"].sync_ack = False
        rep = self.stats.get_report()
        block = rep["in_flight"]
        self.assertEqual(block["ack_early_matched"], 1)
        self.assertEqual(block["ack_early_pending"], 0)
        self.assertEqual(block["ack_without_mid"], 0)
        self.assertEqual(block["ack_without_mid"],
                         block["ack_before_reg"] + block["ack_after_drop"])
        # broker-a 的 mid 已被早到 ack 消费，只剩另外两个 host 在途
        self.assertEqual(block["mids"], 2)
        self.assertEqual(block["mid_ttl_dropped"], 0)
        # 守恒：queued == send_ms.n + mid_ttl_dropped + mids
        queued = sum(r["queued"] for r in
                     rep["uplink_publish"]["broker_distribution"])
        self.assertEqual(queued, rep["uplink_publish"]["send_ms"]["n"]
                         + block["mid_ttl_dropped"] + block["mids"])

    def test_late_ack_after_ttl_drop_is_classified(self):
        """_mid 已被 TTL 回收后才到的 ack：记 ack_after_drop，不误判早到。"""
        net = self.net
        net.publish_broadcast("t", {"x": 1})
        stats = self.stats
        with stats._lock:
            for k in list(stats._mid):
                bid, _t0 = stats._mid[k]
                stats._mid[k] = (bid, time.perf_counter() - 300)
            stats._prune()
        net.clients["broker-a"].ack(1)
        block = stats.get_report()["in_flight"]
        self.assertEqual(block["ack_after_drop"], 1)
        self.assertEqual(block["ack_early_pending"], 0)
        self.assertEqual(block["ack_without_mid"], 1)
        self.assertEqual(block["ack_without_mid"],
                         block["ack_before_reg"] + block["ack_after_drop"])

    def test_early_ack_without_registration_expires(self):
        """早到 ack 始终等不到登记：_EARLY_TTL 后记 ack_before_reg，不泄漏。"""
        stats = self.stats
        stats._note_publish_ack("broker-a", 999)
        block = stats.get_report()["in_flight"]
        self.assertEqual(block["ack_early_pending"], 1)
        self.assertEqual(block["ack_before_reg"], 0)
        with stats._lock:
            stats._early_acks[("broker-a", 999)] = time.perf_counter() - 300
        block = stats.get_report()["in_flight"]
        self.assertEqual(block["ack_early_pending"], 0)
        self.assertEqual(block["ack_before_reg"], 1)
        self.assertEqual(block["ack_without_mid"], 1)
        self.assertEqual(block["ack_without_mid"],
                         block["ack_before_reg"] + block["ack_after_drop"])
        # 新增诊断字段：能直接看出过期 ack 属于哪个 host、多大年龄。
        self.assertEqual(block["ack_before_reg_hosts"], {"broker-a": 1})
        sample = block["ack_before_reg_samples"][0]
        self.assertEqual(sample["host"], "broker-a")
        self.assertEqual(sample["mid"], 999)
        self.assertEqual(sample["source"], "unknown")
        self.assertEqual(sample["reason"], "ttl")
        self.assertGreaterEqual(sample["age_ms"], 200000.0)
        self.assertEqual(block["early_acks_key"], "(host, mid)")
        self.assertEqual(block["early_ttl_s"], stats._EARLY_TTL)
        self.assertEqual(block["early_ack_source"],
                         {"business_matched": 0, "ping_dropped": 0,
                          "unknown_expired": 1})

    def test_foreign_publish_ack_is_discarded(self):
        """非 broadcast publish（stats QoS1 ping）的 ack 不得污染早到/未归属。"""
        c = self.net.clients["broker-a"]
        info = c.publish("multi_mqtt/ping_rtt/broker-a", b"", qos=1)
        c.ack(info.mid)
        block = self.stats.get_report()["in_flight"]
        self.assertEqual(block["ack_foreign"], 1)
        self.assertEqual(block["ack_early_pending"], 0)
        self.assertEqual(block["ack_before_reg"], 0)
        self.assertEqual(block["ack_without_mid"], 0)

    def test_ping_ack_arriving_before_foreign_marker_is_claimed(self):
        """paho 在 publish() 返回前回调 on_publish 的竞态：ping ack 先被暂存，
        随后 _note_foreign_publish 必须把它认领掉，不能留在 _early_acks 里
        过期成 ack_before_reg。"""
        c = self.net.clients["broker-a"]
        c.sync_ack = True          # on_publish 早于 _note_foreign_publish
        c.publish("multi_mqtt/ping_rtt/broker-a", b"", qos=1)
        c.sync_ack = False
        block = self.stats.get_report()["in_flight"]
        self.assertEqual(block["ack_foreign"], 1)
        self.assertEqual(block["ack_early_pending"], 0)
        self.assertEqual(block["ack_before_reg"], 0)
        self.assertEqual(block["ack_without_mid"], 0)
        self.assertEqual(block["early_ack_source"]["ping_dropped"], 1)

    def test_same_mid_across_hosts_is_isolated(self):
        """不同 broker 的相同 mid 必须各自占一个槽（key 带 host），不能串台。"""
        stats = self.stats
        stats._note_publish_ack("broker-a", 7)
        stats._note_publish_ack("broker-b", 7)
        block = stats.get_report()["in_flight"]
        self.assertEqual(block["ack_early_pending"], 2)
        # 认领 broker-b 的 ping ack：只清 broker-b 的槽，broker-a 不受影响。
        stats._note_foreign_publish("broker-b", _Info(7))
        with stats._lock:
            self.assertIn(("broker-a", 7), stats._early_acks)
            self.assertNotIn(("broker-b", 7), stats._early_acks)
        block = stats.get_report()["in_flight"]
        self.assertEqual(block["ack_early_pending"], 1)
        self.assertEqual(block["ack_foreign"], 1)
        self.assertEqual(block["ack_before_reg"], 0)

    def test_recovery_same_tick_is_separated_from_recovered(self):
        """online_latest == offline_latest.at_s：方向不可判，单列不并入 recovered。"""
        net = self.net
        net.clients["broker-a"].is_connected = lambda: False
        net.publish_broadcast("t", {"x": 1})
        with self.stats._lock:
            self.stats._last_online["broker-a"] = \
                self.stats._offline_latest["broker-a"]["at_s"]
        cov = self.stats.get_report()["uplink_publish"]["coverage"]
        self.assertEqual(cov["recovery_same_tick"], ["broker-a"])
        self.assertNotIn("broker-a", cov["recovered_since_offline"])

    def test_report_carries_process_and_subscription_provenance(self):
        """跨进程/会话排查字段：pid、forked、sessions、订阅集合。"""
        self.net.downlink("pty/out", {"pty": "s1", "seq": 1}, "broker-a")
        self.net.downlink("pty/out", {"pty": "s2", "seq": 1}, "broker-b")
        rep = self.stats.get_report()
        proc = rep["process"]
        self.assertEqual(proc["pid"], proc["current_pid"])
        self.assertFalse(proc["forked"])
        self.assertGreaterEqual(proc["object_age_s"], 0.0)
        self.assertEqual(rep["pty_frames"]["sessions"], 2)
        self.assertEqual(rep["subscriptions"]["topics"],
                         ["pty/out", "sys/device/response"])

    def test_unclassified_is_residual_of_rx(self):
        """unclassified = rx − wins − dups − hb_frames − hb_dups。"""
        net = self.net
        net.downlink("pty/out", {"pty": "s1", "seq": 1}, "broker-a")       # unique
        net.downlink("pty/out", {"pty": "s1", "seq": 1}, "broker-a")       # dup
        net.downlink("pty/out", {"pty": "s1", "hb": 1}, "broker-a")        # hb 首帧
        net.downlink("pty/out", {"pty": "s1", "hb": 1}, "broker-a")        # hb 重复
        net.downlink("pty/out", {"pty": "s1", "ctrl": 1}, "broker-a")      # 无 seq/hb
        row = {r["broker"]: r for r in
               self.stats.get_report()["pty_frames"]["broker_distribution"]}["broker-a"]
        self.assertEqual(row["rx"], 5)
        self.assertEqual(row["unclassified"], 1)
        self.assertEqual(row["unclassified"],
                         row["rx"] - row["wins"] - row["dups"]
                         - row.get("hb_frames", 0) - row.get("hb_dups", 0))

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
        self.assertEqual(
            stats.get_report()["in_flight"]["mid_ttl_dropped"], 1)
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
        self.assertGreaterEqual(sample["at_s"], 0.0)
        self.assertNotIn("print(1)", str(req["unmatched_samples"]))
        rows = {r["broker"]: r for r in req["broker_distribution"]}
        self.assertEqual(rows["broker-a"]["unmatched"], 2)

    def test_unmatched_samples_are_bounded(self):
        for i in range(20):
            self.net.downlink("t/%d" % i, {"req_id": "r-%d" % i}, "broker-a")
        req = self.stats.get_report()["requests"]
        self.assertEqual(len(req["unmatched_samples"]), 5)
        # 滚动窗口：保留最近 5 条（不是启动最初 5 条），否则稍晚的请求
        # 永远不可能出现在样本里。
        self.assertEqual(req["unmatched_samples"][-1]["req_id"], "r-19")
        self.assertEqual(req["unmatched_samples"][0]["req_id"], "r-15")
        self.assertLessEqual(len(req["unmatched_topics"]), 8)

    def test_unmatched_sample_max_is_runtime_adjustable(self):
        """样本上限可在 RPC 运行期调整，调小立刻裁剪，非法输入立即报错。"""
        stats = self.stats
        self.assertEqual(
            stats.get_report()["requests"]["unmatched_sample_max"],
            stats._UNMATCHED_SAMPLE_MAX)
        self.assertEqual(stats.set_unmatched_sample_max(10), 10)
        for i in range(20):
            self.net.downlink("t", {"req_id": "r-%d" % i}, "broker-a")
        req = stats.get_report()["requests"]
        self.assertEqual(req["unmatched_sample_max"], 10)
        self.assertEqual(len(req["unmatched_samples"]), 10)
        self.assertEqual(req["unmatched_samples"][-1]["req_id"], "r-19")
        stats.set_unmatched_sample_max(3)
        req = stats.get_report()["requests"]
        self.assertEqual([s["req_id"] for s in req["unmatched_samples"]],
                         ["r-17", "r-18", "r-19"])
        stats.set_unmatched_sample_max(0)
        self.assertEqual(stats.get_report()["requests"]["unmatched_samples"], [])
        for bad in (-1, 257, True, 1.5, "5", None):
            with self.assertRaises(ValueError):
                stats.set_unmatched_sample_max(bad)

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
                               "orphan_writes": 0, "mid_ttl_dropped": 3,
                               "ack_early_pending": 0,
                               "ack_early_matched": 0, "ack_before_reg": 0,
                               "ack_after_drop": 0, "ack_without_mid": 0,
                               "ack_foreign": 0,
                               "early_ack_source": {
                                   "business_matched": 0,
                                   "ping_dropped": 0,
                                   "unknown_expired": 0},
                               "early_acks_key": "(host, mid)",
                               "early_ttl_s": stats._EARLY_TTL,
                               "ack_before_reg_hosts": {},
                               "ack_before_reg_samples": []})

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
        self.assertEqual(set(cov["online_latest"]), set(HOSTS))
        self.assertEqual(cov["offline_latest"], {})
        self.assertEqual(cov["recovered_since_offline"], [])
        self.assertEqual(cov["recovery_same_tick"], [])
        self.assertIsNotNone(cov["broadcast_idle_s"])
        self.assertGreaterEqual(cov["broadcast_idle_s"], 0.0)
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
        self.assertEqual(cov["offline_latest"]["broker-c"]["host"], "broker-c")
        # broker-c 全程未连上 -> 没有 online 记录；另两 host 有
        self.assertNotIn("broker-c", cov["online_latest"])
        self.assertIn("broker-a", cov["online_latest"])

    def test_offline_latest_keeps_steady_state_after_timeline_rolls(self):
        """稳态离线不能被 10 条时间轴窗口挤掉：每个 host 都要留最近一次。"""
        for c in self.net.clients.values():
            c.is_connected = lambda: False
        for _ in range(12):
            self.net.publish_broadcast("t", {"x": 1})
        cov = self.stats.get_report()["uplink_publish"]["coverage"]
        self.assertEqual(cov["offline"], 36)          # 3 host × 12 批
        self.assertEqual(len(cov["offline_samples"]), 10)
        self.assertEqual(set(cov["offline_latest"]), set(HOSTS))
        for host in HOSTS:
            latest = cov["offline_latest"][host]
            self.assertEqual(latest["host"], host)
            self.assertEqual(latest["state"], "")
            self.assertFalse(latest["socket"])
        # 时间轴只剩最后 10 条（第 10-12 批），而 latest 覆盖到第 12 批
        self.assertGreaterEqual(cov["offline_latest"]["broker-a"]["at_s"],
                                cov["offline_samples"][0]["at_s"])

    def test_recovered_since_offline_detects_recovery(self):
        """先离线后在线：recovered_since_offline 必须点名该 host。"""
        c = self.net.clients["broker-b"]
        c.is_connected = lambda: False
        self.net.publish_broadcast("t", {"x": 1})      # broker-b 判离线
        time.sleep(0.3)                                # 拉开 at_s 至少 0.1s
        c.is_connected = lambda: True
        self.net.publish_broadcast("t", {"x": 2})      # broker-b 判在线
        cov = self.stats.get_report()["uplink_publish"]["coverage"]
        self.assertEqual(cov["offline_latest"]["broker-b"]["host"], "broker-b")
        self.assertGreater(cov["online_latest"]["broker-b"],
                           cov["offline_latest"]["broker-b"]["at_s"])
        self.assertEqual(cov["recovered_since_offline"], ["broker-b"])
        self.assertGreaterEqual(cov["broadcast_idle_s"], 0.0)

    def test_server_ack_records_server_from_and_agrees_with_send_first(self):
        """回包 server_from = 服务端首收 broker：observed/首包/agree 全通。"""
        net = self.net
        net.publish_broadcast("sys/device/request",
                              {"req_id": "req-1", "code": "1"})
        net.clients["broker-a"].ack(1)      # 发送侧首包先排出 socket
        with self.tr.node.lock:
            self.tr.node.pending_requests["req-1"] = {
                "start_time": time.perf_counter()}
        net.downlink("sys/device/response",
                     {"req_id": "req-1", "server_from": "broker-a"},
                     "broker-c")
        sa = self.stats.get_report()["uplink_publish"]["server_ack"]
        self.assertEqual(sa["observed"], 1)
        self.assertEqual(sa["first_packet_broker"], "broker-a")
        self.assertEqual(sa["broker_distribution"],
                         [{"broker": "broker-a", "wins": 1, "win_pct": 100.0}])
        self.assertEqual(sa["vs_send_first"],
                         {"agree": 1, "disagree": 0, "disagree_hosts": {}})

    def test_server_ack_disagree_dedups_and_degrades(self):
        """发送侧与服务端首收不同要记 disagree；重复/坏字段/未匹配都不计。"""
        net = self.net
        net.publish_broadcast("t", {"req_id": "req-2"})
        net.clients["broker-a"].ack(1)      # 发送侧首包 = broker-a
        with self.tr.node.lock:
            self.tr.node.pending_requests["req-2"] = {
                "start_time": time.perf_counter()}
            self.tr.node.pending_requests["req-3"] = {
                "start_time": time.perf_counter()}
        net.downlink("reply", {"req_id": "req-2", "server_from": "broker-b"},
                     "broker-c")
        # 同一 req_id 的第二份回包（多服务端/迟到副本）不得重复计 observed
        net.downlink("reply", {"req_id": "req-2", "server_from": "broker-a"},
                     "broker-a")
        # server_from 非字符串：跳过，不抛异常
        net.downlink("reply", {"req_id": "req-3", "server_from": 0}, "broker-a")
        # 未匹配本机 pending 的回包：不参与 server_ack
        net.downlink("reply", {"req_id": "other", "server_from": "broker-a"},
                     "broker-a")
        sa = self.stats.get_report()["uplink_publish"]["server_ack"]
        self.assertEqual(sa["observed"], 1)
        self.assertEqual(sa["first_packet_broker"], "broker-b")
        self.assertEqual(sa["broker_distribution"],
                         [{"broker": "broker-b", "wins": 1, "win_pct": 100.0}])
        self.assertEqual(sa["vs_send_first"],
                         {"agree": 0, "disagree": 1,
                          "disagree_hosts": {"broker-b": 1}})

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
        self.assertEqual(fl["mid_ttl_dropped"], 1)

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
        self.assertEqual(cov["recovered_since_offline"], [])
        self.assertIsNone(cov["broadcast_idle_s"])
        sa = rep["uplink_publish"]["server_ack"]
        self.assertEqual(sa["observed"], 0)
        self.assertEqual(sa["broker_distribution"], [])
        self.assertIsNone(sa["first_packet_broker"])
        self.assertEqual(sa["vs_send_first"],
                         {"agree": 0, "disagree": 0, "disagree_hosts": {}})
        self.assertEqual(rep["in_flight"]["mid_ttl_dropped"], 0)
        self.assertEqual(rep["in_flight"]["ack_without_mid"], 0)
        self.assertEqual(rep["in_flight"]["ack_early_pending"], 0)
        self.assertEqual(rep["in_flight"]["ack_early_matched"], 0)
        self.assertEqual(rep["in_flight"]["ack_foreign"], 0)


if __name__ == "__main__":
    unittest.main()
