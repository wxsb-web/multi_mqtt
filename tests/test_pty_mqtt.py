#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""pty_client_mqtt / RemotePty / _PTY_START_TEMPLATE 的详细测试。

分三部分：
1. RemotePtyClientTests —— 全平台可跑：用 FakeTransport 测客户端协商逻辑
   （iseq 打号、下行 seq 去重、心跳/end 帧、握手失败清理、close 行为）。
   不连任何 broker，不需要 POSIX。
2. PtyTemplateStaticTests —— 全平台可跑：模板编译与关键防护标记的静态检查。
3. PtyTemplateLiveTests —— 仅 POSIX：在 PythonExecutor 的持久命名空间里
   真实执行 _PTY_START_TEMPLATE（真 openpty + fork /bin/sh + 三个线程），
   用 FakeNet 模拟服务端 mqtt_net，覆盖：
     - 按键回显 / iseq 多副本首帧去重 / winsz / stop→end
     - 心跳帧
     - cwd 不存在回退 HOME（不报错、不退出）
     - 重复握手路由幂等（RecursionError 回归测试）
     - 旧版自裹闭环的自愈
     - 非 PTY topic 原样透传到服务端 handle_message

运行：
    cd multi_mqtt && python -m unittest tests.test_pty_mqtt -v
"""
import json
import os
import sys
import threading
import time
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from remote_cmd import (  # noqa: E402
    RemotePty, Transport, RemoteError, RemoteTimeout, RemoteOpError,
    build_pty_start_code, PTY_FRAME_MAX,
)
from rpc_executor import PythonExecutor  # noqa: E402

IS_POSIX = os.name == "posix"


# ============================ 通用 Fake ============================

def _canned_env(sid, **kw):
    env = {
        "ok": True, "sid": sid,
        "in_topic": "pty/%s/in" % sid, "out_topic": "pty/%s/out" % sid,
        "shell": "/bin/sh", "pid": 4242, "term": "xterm",
        "rows": 24, "cols": 80, "cwd": "/root", "cwd_warning": None,
        "flush_interval": 0.0, "ttl": 43200, "login": True, "heartbeat": 0.0,
    }
    env.update(kw)
    return env


class FakeTransport(Transport):
    """RemotePty 的内存传输：request 回罐头，publish/订阅全记录。"""

    def __init__(self, response=None):
        self.response = response
        self.requests = []        # [(code, timeout)]，记录调用顺序
        self.published = []       # [(topic, payload)]
        self.events = []          # 订阅/退订事件，保序
        self._handlers = {}

    def request(self, code, timeout=60):
        self.requests.append((code, timeout))
        return self.response

    def publish(self, topic, payload):
        self.published.append((topic, dict(payload)))

    def stream_subscribe(self, topic, handler):
        self.events.append(("sub", topic))
        self._handlers.setdefault(topic, []).append(handler)

    def stream_unsubscribe(self, topic, handler):
        self.events.append(("unsub", topic))
        self._handlers.get(topic, []).remove(handler)

    def emit(self, topic, data):
        """模拟 broker 推送一帧给本 topic 的所有订阅者。"""
        for h in list(self._handlers.get(topic, [])):
            h(data)


def _open_pty(transport, sid="pty-test-1", **open_kw):
    """用罐头回包完成一次 RemotePty.open，返回 (pty, env, 收到的数据列表)。"""
    env = _canned_env(sid)
    transport.response = {"r": json.dumps(env), "ok": True}
    received = []
    heartbeats = []
    pty = RemotePty(transport, timeout=5)
    ret = pty.open(
        24, 80, sid=sid,
        in_topic=env["in_topic"], out_topic=env["out_topic"],
        heartbeat=5.0,
        on_data=received.append, on_heartbeat=heartbeats.append,
        **open_kw)
    return pty, ret, received, heartbeats


# ============================ 1. 客户端 RemotePty 逻辑（全平台） ============================

class RemotePtyClientTests(unittest.TestCase):

    def test_open_subscribes_out_topic_before_sending_request(self):
        tr = FakeTransport()
        pty, env, _, _ = _open_pty(tr, "pty-order-1")
        self.addCleanup(lambda: pty.close())
        self.assertEqual(env["sid"], "pty-order-1")
        # 必须先订阅再发握手代码，否则最早的 shell 输出会丢
        self.assertEqual(tr.events[0], ("sub", "pty/pty-order-1/out"))
        self.assertEqual(len(tr.requests), 1)
        self.assertIn("_cmq_pty_start", tr.requests[0][0])
        self.assertEqual(pty.sid, "pty-order-1")

    def test_downlink_seq_dedup_keeps_first_frame_only(self):
        tr = FakeTransport()
        pty, _, got, _ = _open_pty(tr, "pty-seq-1")
        self.addCleanup(lambda: pty.close())
        out = "pty/pty-seq-1/out"
        # seq=7 来 3 份（多 broker 重复），seq=8 一份，乱序迟到的 seq=7 再一份
        tr.emit(out, {"pty": "pty-seq-1", "seq": 7, "d": "A"})
        tr.emit(out, {"pty": "pty-seq-1", "seq": 7, "d": "A"})
        tr.emit(out, {"pty": "pty-seq-1", "seq": 8, "d": "B"})
        tr.emit(out, {"pty": "pty-seq-1", "seq": 7, "d": "A"})
        self.assertEqual(b"".join(got), b"AB")

    def test_heartbeat_frames_update_liveness_without_data(self):
        tr = FakeTransport()
        pty, _, got, hbs = _open_pty(tr, "pty-hb-1")
        self.addCleanup(lambda: pty.close())
        tr.emit("pty/pty-hb-1/out", {"pty": "pty-hb-1", "hb": 1000})
        tr.emit("pty/pty-hb-1/out", {"pty": "pty-hb-1", "hb": 2000})
        self.assertEqual(hbs, [1000, 2000])
        self.assertEqual(got, [])  # 心跳不带数据

    def test_end_frame_sets_end_reason(self):
        tr = FakeTransport()
        pty, _, _, _ = _open_pty(tr, "pty-end-1")
        self.assertIsNone(pty.end_reason)
        tr.emit("pty/pty-end-1/out",
                {"pty": "pty-end-1", "end": True, "reason": "exit", "rc": 0})
        self.assertEqual(pty.end_reason, "exit")
        self.assertTrue(pty.wait_end(timeout=1))

    def test_uplink_frames_carry_strict_monotonic_iseq(self):
        tr = FakeTransport()
        pty, _, _, _ = _open_pty(tr, "pty-iseq-1")
        self.addCleanup(lambda: pty.close())
        pty.send(b"x")
        pty.resize(30, 100)
        pty.send(b"\xff\x00\x7f")  # 任意二进制经 latin-1 无损承载
        pty.detach()
        in_topic = "pty/pty-iseq-1/in"
        self.assertEqual([p[0] for p in tr.published],
                         [in_topic] * 4)
        self.assertEqual([p[1]["iseq"] for p in tr.published], [0, 1, 2, 3])
        self.assertEqual(tr.published[0][1]["k"], "x")
        self.assertEqual(tr.published[1][1]["winsz"], [30, 100])
        self.assertEqual(tr.published[2][1]["k"],
                         b"\xff\x00\x7f".decode("latin-1"))
        self.assertTrue(tr.published[3][1]["stop"])
        # 所有上行帧都没有 req_id：PTY 数据面不走 RPC 签名/去重通道
        for _, frame in tr.published:
            self.assertNotIn("req_id", frame)
            self.assertNotIn("code", frame)

    def test_send_before_open_raises(self):
        pty = RemotePty(FakeTransport(), timeout=2)
        with self.assertRaises(RemoteError):
            pty.send(b"\r")
        # resize/detach 在未 open 时静默忽略，不能炸
        pty.resize(24, 80)
        pty.detach()

    def test_open_timeout_unsubscribes(self):
        tr = FakeTransport(response=None)
        pty = RemotePty(tr, timeout=2)
        with self.assertRaises(RemoteTimeout):
            pty.open(24, 80, sid="pty-timeout-1",
                     in_topic="pty/pty-timeout-1/in",
                     out_topic="pty/pty-timeout-1/out", req_timeout=2)
        self.assertIn(("unsub", "pty/pty-timeout-1/out"), tr.events)
        self.assertIsNone(pty.sid)

    def test_open_remote_op_error_unsubscribes(self):
        tr = FakeTransport(response={
            "r": json.dumps({"ok": False, "error": "boom traceback"}),
            "ok": True})
        pty = RemotePty(tr, timeout=2)
        with self.assertRaises(RemoteOpError):
            pty.open(24, 80, sid="pty-operr-1",
                     in_topic="pty/pty-operr-1/in",
                     out_topic="pty/pty-operr-1/out", req_timeout=2)
        self.assertIn(("unsub", "pty/pty-operr-1/out"), tr.events)

    def test_close_sends_stop_and_unsubscribes_without_stopping_transport(self):
        tr = FakeTransport()
        pty, _, _, _ = _open_pty(tr, "pty-close-1")
        pty.close()
        in_topic = "pty/pty-close-1/in"
        self.assertTrue(any(f.get("stop") for _, f in tr.published
                            if _ == in_topic))
        self.assertIn(("unsub", "pty/pty-close-1/out"), tr.events)
        self.assertIsNone(pty.sid)


# ============================ 2. 模板静态检查（全平台） ============================

class PtyTemplateStaticTests(unittest.TestCase):

    def setUp(self):
        self.code = build_pty_start_code({
            "sid": "pty-static-1",
            "in_topic": "pty/pty-static-1/in",
            "out_topic": "pty/pty-static-1/out",
            "rows": 24, "cols": 80, "heartbeat": 0.0})

    def test_template_compiles(self):
        compile(self.code, "<pty-template>", "exec")

    def test_payload_is_embedded(self):
        self.assertIn("pty-static-1", self.code)
        self.assertIn("pty/pty-static-1/in", self.code)
        self.assertIn("pty/pty-static-1/out", self.code)
        # __PAYLOAD__ 占位符必须已被完全替换
        self.assertNotIn("__PAYLOAD__", self.code)

    def test_router_idempotency_marker_present(self):
        # RecursionError 回归：必须用独立布尔标记，禁止再用空 dict 判空
        self.assertIn("_cmq_pty_installed", self.code)
        self.assertNotIn('if not getattr(_net, "_cmq_pty_router", None):',
                         self.code)

    def test_router_reentry_guard_present(self):
        self.assertIn("_rlocal", self.code)

    def test_cwd_fallback_present(self):
        self.assertIn("_cwd_warn", self.code)
        self.assertIn("cwd_warning", self.code)

    def test_uplink_iseq_dedup_present(self):
        self.assertIn("_iseq_recent", self.code)
        self.assertIn('"iseq"', self.code.replace("'", '"'))


# ============================ 3. 服务端模板真机测试（仅 POSIX） ============================

if IS_POSIX:

    class FakeNet:
        """模拟服务端 gms.mqtt_net：记录广播、保存唯一消息回调。"""

        def __init__(self):
            self.message_callback = None
            self.subscribed = []
            self.published = []
            self._lock = threading.Lock()

        def set_on_message(self, cb):
            self.message_callback = cb

        def subscribe(self, topic):
            self.subscribed.append(topic)

        def publish_broadcast(self, topic, payload):
            with self._lock:
                self.published.append((topic, dict(payload)))

        def deliver(self, topic, data):
            """模拟某个 broker 把帧送到分发线程。"""
            self.message_callback(topic, data, "fakebroker")

        def frames(self, topic):
            with self._lock:
                return [(t, dict(d)) for t, d in self.published if t == topic]

    class FakeServer:
        """模拟 server_mqtt.MQTTServer：持有 mqtt_net + handle_message。"""

        def __init__(self, net):
            self.mqtt_net = net
            self.base_calls = []

        def handle_message(self, topic, data, broker):
            self.base_calls.append((topic, data, broker))

    def _wait_for(pred, timeout=5.0, step=0.05):
        deadline = time.time() + timeout
        while time.time() < deadline:
            if pred():
                return True
            time.sleep(step)
        return False

    def _out_text(net, sid):
        return "".join(d.get("d", "")
                       for _, d in net.frames("pty/%s/out" % sid)
                       if "d" in d)

    def _end_frame(net, sid):
        for _, d in net.frames("pty/%s/out" % sid):
            if d.get("end"):
                return d
        return None

    @unittest.skipUnless(IS_POSIX, "openpty/fork 只能在 POSIX 上真机测试")
    class PtyTemplateLiveTests(unittest.TestCase):

        def setUp(self):
            # 每个用例一个全新的"服务端进程"：独立持久命名空间 + net + gms
            self.net = FakeNet()
            self.gms = FakeServer(self.net)
            self.ns = {"__name__": "__rpc_exec__", "gms": self.gms}
            self._sessions = []

        def tearDown(self):
            # 用例失败也要尽量收尸，避免残留 1h TTL 的 shell
            for i, sid in enumerate(self._sessions):
                try:
                    self._send(sid, {"iseq": 900000 + i, "stop": True})
                except Exception:
                    pass

        def _start(self, sid, **over):
            in_t, out_t = "pty/%s/in" % sid, "pty/%s/out" % sid
            payload = {
                "sid": sid, "in_topic": in_t, "out_topic": out_t,
                "rows": 24, "cols": 80, "shell": "/bin/sh", "term": "xterm",
                "cwd": None, "login": False, "flush_interval": 0.0,
                "ttl": 3600, "frame_max": PTY_FRAME_MAX, "heartbeat": 0.0,
            }
            payload.update(over)
            resp = PythonExecutor(globals=self.ns).execute(
                build_pty_start_code(payload))
            self.assertTrue(resp["ok"],
                            resp.get("stdout", "") + resp.get("error", ""))
            env = json.loads(resp["r"])
            self.assertTrue(env["ok"], env.get("error"))
            self._sessions.append(sid)
            return env

        def _send(self, sid, partial):
            frame = {"pty": sid}
            frame.update(partial)
            self.net.deliver("pty/%s/in" % sid, frame)

        def _type(self, sid, iseq, text):
            self._send(sid, {"iseq": iseq, "k": text})

        def _assert_echo(self, sid, marker, iseq, timeout=5.0):
            self._type(sid, iseq, "echo %s\r" % marker)
            self.assertTrue(
                _wait_for(lambda: marker in _out_text(self.net, sid), timeout),
                "PTY 未在 %.0fs 内回显 %s，实际输出: %r"
                % (timeout, marker, _out_text(self.net, sid)))

        def _stop(self, sid, iseq=9999):
            self._send(sid, {"iseq": iseq, "stop": True})
            self.assertTrue(
                _wait_for(lambda: _end_frame(self.net, sid) is not None, 5.0),
                "stop 后未收到 end 帧")
            end = _end_frame(self.net, sid)
            self.assertEqual(end.get("reason"), "stopped")

        # ---- 基础数据面：回显、iseq 去重、winsz、stop ----

        def test_echo_roundtrip_and_stop_end_frame(self):
            sid = "pty-live-echo"
            env = self._start(sid)
            self.assertEqual(env["shell"], "/bin/sh")
            self.assertEqual(env["rows"], 24)
            self._assert_echo(sid, "ZZECHO42", iseq=0)
            self._stop(sid, iseq=1)

        def test_uplink_iseq_dedup_drops_broker_duplicates(self):
            sid = "pty-live-dedup"
            self._start(sid)
            self._assert_echo(sid, "ZZECHO43", iseq=0)
            # 同一帧（同 iseq）被 15 个 broker 各投递一次：只应执行一遍。
            # 执行一遍时，终端回显命令行 + 命令输出，marker 恰好出现 2 次。
            for _ in range(15):
                self._type(sid, 5, "echo ZZDUP42\r")
            self.assertTrue(
                _wait_for(lambda: _out_text(self.net, sid).count("ZZDUP42") >= 2),
                "首帧未放行")
            time.sleep(0.8)  # 给迟到副本留足作恶时间
            self.assertEqual(_out_text(self.net, sid).count("ZZDUP42"), 2,
                             "重复 iseq 帧被放行了：%r"
                             % _out_text(self.net, sid))
            self._stop(sid, iseq=6)

        def test_winsz_frame_is_accepted_and_session_keeps_working(self):
            sid = "pty-live-winsz"
            self._start(sid)
            self._send(sid, {"iseq": 1, "winsz": [30, 100]})
            time.sleep(0.3)
            # 改尺寸后会话必须仍然活着、能正常读写
            self._assert_echo(sid, "ZZSIZE42", iseq=2)
            self._stop(sid, iseq=3)

        def test_in_topic_is_subscribed_and_routed_off_rpc_path(self):
            sid = "pty-live-route"
            self._start(sid)
            self.assertIn("pty/%s/in" % sid, self.net.subscribed)
            # PTY topic 的帧绝不能漏到服务端 RPC 处理器
            self._type(sid, 0, "echo ZZROUTE\r")
            self.assertTrue(
                _wait_for(lambda: "ZZROUTE" in _out_text(self.net, sid)))
            time.sleep(0.3)
            self.assertEqual(self.gms.base_calls, [])
            self._stop(sid, iseq=1)

        # ---- 心跳 ----

        def test_heartbeat_frames_emitted_at_interval(self):
            sid = "pty-live-hb"
            self._start(sid, heartbeat=0.2)
            hb_count = lambda: sum(
                1 for _, d in self.net.frames("pty/%s/out" % sid)
                if d.get("hb") is not None)
            self.assertTrue(_wait_for(lambda: hb_count() >= 2, timeout=3.0),
                            "3s 内应至少收到 2 个心跳帧")
            self._stop(sid, iseq=1)

        # ---- cwd 容错 ----

        def test_missing_cwd_falls_back_instead_of_failing(self):
            sid = "pty-live-cwd"
            bad = "/no/such/cwd_xyz_%d" % os.getpid()
            env = self._start(sid, cwd=bad)
            # 会话正常建立，而不是 RemoteOpError 把客户端打退出
            self.assertTrue(env["ok"])
            self.assertTrue(env["cwd_warning"])
            self.assertIn(bad, env["cwd_warning"])
            self.assertTrue(os.path.isdir(env["cwd"]))
            # shell 真的跑在回退目录里
            self._type(sid, 0, "pwd\r")
            self.assertTrue(
                _wait_for(lambda: env["cwd"] in _out_text(self.net, sid)),
                "pwd 输出不含回退目录 %s: %r"
                % (env["cwd"], _out_text(self.net, sid)))
            self._stop(sid, iseq=1)

        # ---- RecursionError 回归：重复握手 ----

        def test_repeated_handshake_does_not_rewrap_callback(self):
            s1, s2 = "pty-live-restart-1", "pty-live-restart-2"
            self._start(s1)
            cb = self.net.message_callback
            router_dict = self.net._cmq_pty_router
            chain_base = self.net._cmq_pty_orig
            self.assertTrue(self.net._cmq_pty_installed)

            # 两个会话短暂共存：路由表里各占一项，回调仍是同一个
            self._start(s2)
            self.assertIs(self.net.message_callback, cb,
                          "第二次握手重新 set_on_message 包裹了回调")
            self.assertIs(self.net._cmq_pty_router, router_dict,
                          "路由 dict 被替换")
            self.assertIs(self.net._cmq_pty_orig, chain_base,
                          "回调链底被覆盖")
            self.assertIn("pty/%s/in" % s1, router_dict)
            self.assertIn("pty/%s/in" % s2, router_dict)

            # 两个会话互不干扰
            self._assert_echo(s1, "ZZONE42", iseq=0)
            self._assert_echo(s2, "ZZTWO42", iseq=0)

            # 非 PTY topic 必须沿链底到达服务端 RPC 处理器，且不递归
            self.net.deliver("sys/device/request",
                             {"req_id": "r1", "code": "1+1"})
            self.assertEqual(len(self.gms.base_calls), 1)
            self.assertEqual(self.gms.base_calls[0][0], "sys/device/request")

            self._stop(s1, iseq=1)
            # 旧会话收尸后路由项摘除，但回调身份依然不变
            self.assertTrue(
                _wait_for(lambda: "pty/%s/in" % s1 not in router_dict, 3.0))
            self.assertIs(self.net.message_callback, cb)
            self._stop(s2, iseq=1)
            self.net.deliver("sys/device/request",
                             {"req_id": "r2", "code": "2+2"})
            self.assertEqual(len(self.gms.base_calls), 2)

        # ---- 旧版坏链路自愈 ----

        def test_heals_legacy_self_wrapping_router_chain(self):
            sid = "pty-live-heal"

            # 手工复刻旧版 bug：空 dict 触发重装、orig 指向自己的闭环
            def buggy(topic, data, broker):
                q = self.net._cmq_pty_router.get(topic)
                if q is not None and isinstance(data, dict):
                    q.put(data)
                    return None
                return self.net._cmq_pty_orig(topic, data, broker)

            self.net.message_callback = buggy
            self.net._cmq_pty_router = {}   # 空 dict：旧版误判为"未安装"
            self.net._cmq_pty_orig = buggy  # 闭环：动态回读到自己
            # 先证明旧版故障确实可复现
            with self.assertRaises(RecursionError):
                self.net.deliver("sys/device/request",
                                 {"req_id": "old", "code": "1"})

            # 新模板握手：应一步把链底接回 gms.handle_message 并装布尔标记
            self._start(sid)
            self.assertTrue(self.net._cmq_pty_installed)
            self.assertEqual(self.net._cmq_pty_orig,
                             self.gms.handle_message)

            # 非 PTY 帧：恰好一次到达链底，不再递归
            self.net.deliver("sys/device/request",
                             {"req_id": "new", "code": "2+2"})
            self.assertEqual(len(self.gms.base_calls), 1)
            self.assertEqual(self.gms.base_calls[0][1]["req_id"], "new")

            # PTY 会话功能正常
            self._assert_echo(sid, "ZZHEAL42", iseq=0)
            self._stop(sid, iseq=1)


if __name__ == "__main__":
    unittest.main()
