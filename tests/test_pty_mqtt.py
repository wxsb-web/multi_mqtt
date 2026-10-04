#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""pty_client_mqtt / RemotePty / _PTY_START_TEMPLATE 的详细测试。

分三部分：
1. RemotePtyClientTests —— 全平台可跑：用 FakeTransport 测客户端协商逻辑
   （iseq 打号、下行 seq 去重/乱序重排/丢帧跳号、心跳/end 帧、握手失败
   清理、close 行为）。不连任何 broker，不需要 POSIX。
2. PtyTemplateStaticTests —— 全平台可跑：模板编译与关键防护标记的静态检查。
3. PtyTemplateLiveTests —— 仅 POSIX：在 PythonExecutor 的持久命名空间里
   真实执行 _PTY_START_TEMPLATE（真 openpty + fork /bin/sh + 三个线程），
   用 FakeNet 模拟服务端 mqtt_net，覆盖：
     - 按键回显 / iseq 单调闸门（多副本去重 + 旧 winsz 乱序迟到不得生效）
       / winsz / stop→end
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

from client.remote_cmd import (  # noqa: E402
    RemotePty, Transport, RemoteError, RemoteTimeout, RemoteOpError,
    build_pty_start_code, PTY_FRAME_MAX,
)
from rpc_executor import PythonExecutor  # noqa: E402
from multi_mqtt import BROKER_LIST  # noqa: E402
from client import pty_client_mqtt as pcm  # noqa: E402

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
        self.extra_responses = []  # 其他服务端的迟到握手回包
        self.prefetch = []         # request_many 期间同步下发的帧（模拟首屏）
        self.requests = []         # [(code, timeout)]，记录调用顺序
        self.published = []        # [(topic, payload)]
        self.events = []           # 订阅/退订事件，保序
        self._handlers = {}

    def request(self, code, timeout=60):
        self.requests.append((code, timeout))
        return self.response

    def request_many(self, code, timeout=60, gather=0.8):
        self.requests.append((code, timeout, gather))
        for topic, data in list(self.prefetch):
            self.emit(topic, data)
        self.prefetch = []
        return self.response, list(self.extra_responses)

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


def _wait_until(pred, timeout=2.0, step=0.02):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if pred():
            return True
        time.sleep(step)
    return False


def _open_pty(transport, sid="pty-test-1", env_extra=None, **open_kw):
    """用罐头回包完成一次 RemotePty.open，返回 (pty, env, 收到的数据列表)。"""
    env = _canned_env(sid)
    if env_extra:
        env.update(env_extra)
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
        # seq=0 来 3 份（多 broker 重复），seq=1 一份，乱序迟到的 seq=0 再一份
        tr.emit(out, {"pty": "pty-seq-1", "seq": 0, "d": "A"})
        tr.emit(out, {"pty": "pty-seq-1", "seq": 0, "d": "A"})
        tr.emit(out, {"pty": "pty-seq-1", "seq": 1, "d": "B"})
        tr.emit(out, {"pty": "pty-seq-1", "seq": 0, "d": "A"})
        self.assertEqual(b"".join(got), b"AB")

    def test_downlink_reorders_out_of_order_frames(self):
        # 不同 broker 路径延迟抖动让后发帧先到：必须按 seq 重排后再渲染，
        # 否则跨 chunk 的 CSI 转义序列会把终端状态机打坏（进度条错位）。
        tr = FakeTransport()
        pty, _, got, _ = _open_pty(tr, "pty-reorder-1")
        self.addCleanup(lambda: pty.close())
        out = "pty/pty-reorder-1/out"
        tr.emit(out, {"pty": "pty-reorder-1", "seq": 1, "d": "B"})
        tr.emit(out, {"pty": "pty-reorder-1", "seq": 0, "d": "A"})
        tr.emit(out, {"pty": "pty-reorder-1", "seq": 3, "d": "D"})
        tr.emit(out, {"pty": "pty-reorder-1", "seq": 2, "d": "C"})
        self.assertEqual(b"".join(got), b"ABCD")
        # 全部连续补齐后不应留下等待跳号的定时器
        self.assertIsNone(pty._reorder._timer)
        self.assertEqual(pty._reorder._pending, {})

    def test_downlink_gap_skips_after_timeout_instead_of_stalling(self):
        # 中间帧真被 broker 丢掉：短暂等待后跳号放行，输出不能永久卡死
        from client.remote_cmd import _PtyReorderBuffer
        _PtyReorderBuffer.GAP_TIMEOUT = 0.2
        self.addCleanup(
            setattr, _PtyReorderBuffer, "GAP_TIMEOUT", 0.75)
        tr = FakeTransport()
        pty, _, got, _ = _open_pty(tr, "pty-gap-1")
        self.addCleanup(lambda: pty.close())
        out = "pty/pty-gap-1/out"
        tr.emit(out, {"pty": "pty-gap-1", "seq": 10, "d": "X"})
        self.assertEqual(got, [])  # 缺口未补齐前先缓存
        self.assertTrue(_wait_until(lambda: bool(got), timeout=2.0))
        self.assertEqual(b"".join(got), b"X")
        # 跳号后迟到的 seq=10 副本必须丢弃
        tr.emit(out, {"pty": "pty-gap-1", "seq": 10, "d": "X"})
        tr.emit(out, {"pty": "pty-gap-1", "seq": 11, "d": "Y"})
        self.assertEqual(b"".join(got), b"XY")

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

    # ---- 多应答者归属仲裁（owner/claim） ----

    def test_open_sends_claim_and_all_uplink_frames_carry_owner(self):
        tr = FakeTransport()
        pty, _, _, _ = _open_pty(tr, "pty-owner-1",
                                 env_extra={"owner": "aaaaaa", "host": "h1",
                                            "pid": 111})
        self.addCleanup(lambda: pty.close())
        self.assertEqual(pty.owner, "aaaaaa")
        # open 一结束立即发 claim（iseq=0），影子服务端在首个按键前就自杀
        first = tr.published[0][1]
        self.assertEqual(first, {"pty": "pty-owner-1", "owner": "aaaaaa",
                                 "iseq": 0, "claim": True})
        pty.send(b"x")
        pty.resize(30, 100)
        self.assertEqual([f["owner"] for _, f in tr.published[1:]],
                         ["aaaaaa", "aaaaaa"])
        self.assertEqual([f["iseq"] for _, f in tr.published], [0, 1, 2])

    def test_foreign_owner_downlink_frames_dropped_including_end(self):
        # 影子 PTY 的输出帧和它的 end 帧都不能影响本视图
        tr = FakeTransport()
        pty, _, got, _ = _open_pty(tr, "pty-owner-2",
                                   env_extra={"owner": "win1"})
        self.addCleanup(lambda: pty.close())
        out = "pty/pty-owner-2/out"
        tr.emit(out, {"pty": "pty-owner-2", "owner": "shadow-x",
                      "seq": 0, "d": "GHOST"})
        tr.emit(out, {"pty": "pty-owner-2", "owner": "shadow-x",
                      "end": True, "reason": "claim_lost"})
        self.assertEqual(got, [])
        self.assertIsNone(pty.end_reason)
        self.assertEqual(pty.foreign_frames, 2)
        # 赢家的流照常渲染
        tr.emit(out, {"pty": "pty-owner-2", "owner": "win1",
                      "seq": 0, "d": "ok"})
        self.assertEqual(b"".join(got), b"ok")

    def test_ownerless_old_server_backward_compatible(self):
        # 旧服务端回包/帧不带 owner：不发 claim，下行照旧放行
        tr = FakeTransport()
        pty, _, got, _ = _open_pty(tr, "pty-ownerless-1")
        self.addCleanup(lambda: pty.close())
        self.assertIsNone(pty.owner)
        self.assertFalse(any(f.get("claim") for _, f in tr.published))
        tr.emit("pty/pty-ownerless-1/out",
                {"pty": "pty-ownerless-1", "seq": 0, "d": "legacy"})
        self.assertEqual(b"".join(got), b"legacy")

    def test_multiple_handshake_responders_parsed_winner_first(self):
        tr = FakeTransport()
        tr.extra_responses = [{
            "r": json.dumps(_canned_env(
                "pty-multi-1", owner="bbbbbb", host="host-b", pid=222)),
            "ok": True}]
        pty, _, _, _ = _open_pty(
            tr, "pty-multi-1",
            env_extra={"owner": "aaaaaa", "host": "host-a", "pid": 111})
        self.addCleanup(lambda: pty.close())
        self.assertEqual(
            [(r["owner"], r["host"], r["pid"], r["winner"])
             for r in pty.responders],
            [("aaaaaa", "host-a", 111, True),
             ("bbbbbb", "host-b", 222, False)])

    def test_configure_publishes_clamped_set_frame(self):
        tr = FakeTransport()
        pty, _, _, _ = _open_pty(tr, "pty-set-1",
                                 env_extra={"owner": "win-set"})
        self.addCleanup(lambda: pty.close())
        applied = pty.configure(interval=999, heartbeat=-5, ttl=5)
        self.assertEqual(applied, {"interval": 60.0, "heartbeat": 0.0,
                                   "ttl": 60.0})
        fr = tr.published[-1][1]
        self.assertEqual(fr["set"], {"interval": 60.0, "heartbeat": 0.0,
                                     "ttl": 60.0})
        self.assertEqual(fr["owner"], "win-set")
        self.assertEqual(fr["iseq"], 1)  # claim 占 0，set 帧顺延
        self.assertEqual(pty.configure(), {})  # 无参数不下发

    def test_prefetch_frames_buffered_until_owner_then_ghosts_filtered(self):
        # 握手回包到达前（属主未定）就已经在飞的首屏帧：先缓存，定主后重放，
        # 其中影子 PTY 的早期帧必须被滤掉，不进屏幕/不制造 seq 缺口。
        tr = FakeTransport()
        sid = "pty-prefetch-1"
        out = "pty/%s/out" % sid
        tr.prefetch = [
            (out, {"pty": sid, "owner": "shadow", "seq": 0, "d": "G"}),
            (out, {"pty": sid, "owner": "win", "seq": 0, "d": "W"}),
        ]
        pty, _, got, _ = _open_pty(
            tr, sid, env_extra={"owner": "win"})
        self.addCleanup(lambda: pty.close())
        self.assertEqual(b"".join(got), b"W")
        self.assertEqual(pty.foreign_frames, 1)
        self.assertEqual(pty._pending_frames, [])

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

    def test_broker_status_without_node_falls_back_to_config_count(self):
        online, total, hosts = pcm._broker_status(FakeTransport())
        self.assertEqual((online, hosts), (0, []))
        self.assertEqual(total, len(BROKER_LIST))

    def test_broker_status_counts_only_connected_clients(self):
        from types import SimpleNamespace

        class _Cli:
            def __init__(self, up):
                self._up = up

            def is_connected(self):
                return self._up

        class _Boom:
            def is_connected(self):
                raise RuntimeError("paho 内部异常也不能炸统计")

        net = SimpleNamespace(clients={
            "up-a": _Cli(True), "down-b": _Cli(False), "boom": _Boom()})
        tr = SimpleNamespace(node=SimpleNamespace(mqtt_net=net))
        online, total, hosts = pcm._broker_status(tr)
        self.assertEqual((online, total), (1, 3))
        self.assertEqual(hosts, ["up-a"])


class PtyCliPreCommandTests(unittest.TestCase):
    """SSH 式前置命令：位置参数解析与拼接。"""

    def test_no_command_defaults_empty(self):
        args = pcm.build_parser().parse_args([])
        self.assertEqual(args.command, [])
        self.assertEqual(pcm._join_pre_command(args.command), "")

    def test_quoted_command_after_options(self):
        args = pcm.build_parser().parse_args(
            ["-t", "q", "-k", "2**128", "tmux at"])
        self.assertEqual(pcm._join_pre_command(args.command), "tmux at")

    def test_unquoted_words_joined_like_ssh(self):
        args = pcm.build_parser().parse_args(["tmux", "at"])
        self.assertEqual(pcm._join_pre_command(args.command), "tmux at")

    def test_command_own_dash_options_are_not_consumed_by_client(self):
        # REMAINDER：tmux attach -d 里的 -d 是给远端命令的，不能报无法识别
        args = pcm.build_parser().parse_args(["tmux", "attach", "-d", "-t", "x"])
        self.assertEqual(pcm._join_pre_command(args.command),
                         "tmux attach -d -t x")

    def test_double_dash_separator_stripped(self):
        self.assertEqual(pcm._join_pre_command(["--", "tmux at"]), "tmux at")
        self.assertEqual(pcm._join_pre_command(None), "")


class PtyHotkeyParseTests(unittest.TestCase):
    """按键名 -> 终端字节序列，及反查显示名。"""

    def test_single_byte_control_keys(self):
        self.assertEqual(pcm._parse_key_spec("ctrl-]"), b"\x1d")
        self.assertEqual(pcm._parse_key_spec("ctrl-a"), b"\x01")
        self.assertEqual(pcm._parse_key_spec("ctrl-\\"), b"\x1c")
        # 默认值就是这个原始控制字符，必须保持兼容
        self.assertEqual(pcm._parse_key_spec("\x1d"), b"\x1d")

    def test_modified_insert_xterm_encoding(self):
        self.assertEqual(pcm._parse_key_spec("ctrl-alt-insert"),
                         b"\x1b[2;7~")
        self.assertEqual(pcm._parse_key_spec("shift+insert"), b"\x1b[2;2~")
        self.assertEqual(pcm._parse_key_spec("CTRL_ALT_INSERT"), b"\x1b[2;7~")
        self.assertEqual(pcm._parse_key_spec("ctrl-alt-shift-insert"),
                         b"\x1b[2;8~")

    def test_function_and_cursor_keys(self):
        self.assertEqual(pcm._parse_key_spec("f1"), b"\x1bOP")
        self.assertEqual(pcm._parse_key_spec("f9"), b"\x1b[20~")
        self.assertEqual(pcm._parse_key_spec("insert"), b"\x1b[2~")
        self.assertEqual(pcm._parse_key_spec("up"), b"\x1b[A")
        self.assertEqual(pcm._parse_key_spec("ctrl-left"), b"\x1b[1;5D")

    def test_disabled_and_unknown(self):
        self.assertEqual(pcm._parse_key_spec("none"), b"")
        self.assertEqual(pcm._parse_key_spec("off"), b"")
        self.assertEqual(pcm._parse_key_spec(None), b"")
        self.assertEqual(pcm._parse_key_spec("xyz"), b"xyz")

    def test_describe_roundtrip(self):
        self.assertEqual(pcm._describe_key(b"\x1d"), "ctrl-]")
        self.assertEqual(pcm._describe_key(b"\x1b[2;7~"), "ctrl-alt-insert")
        self.assertEqual(pcm._describe_key(b""), "禁用")
        self.assertEqual(pcm._parse_key_spec(
            "ctrl-alt-insert") in pcm._KEY_DISPLAY, True)


class PtyMagicBarTests(unittest.TestCase):
    """本地魔术命令栏：只调时间参数/本地动作，不碰 topic。"""

    def _ctx(self):
        tr = FakeTransport()
        pty, _, _, _ = _open_pty(tr, "pty-magic-1",
                                 env_extra={"owner": "win-magic"})
        self.addCleanup(lambda: pty.close())
        live = {"interval": 0.0, "heartbeat": 5.0, "ttl": 43200.0,
                "dead_timeout": 15.0}
        return tr, pty, {"pty": pty, "transport": tr, "live": live}

    def test_detach_aliases(self):
        for word in ("detach", "exit", "quit", "q", "d"):
            _, pty, ctx = self._ctx()
            do_detach, lines = pcm._run_magic(word, ctx)
            self.assertTrue(do_detach, word)
            self.assertTrue(lines)

    def test_help_and_blank(self):
        _, _, ctx = self._ctx()
        do_detach, lines = pcm._run_magic("help", ctx)
        self.assertFalse(do_detach)
        joined = "\n".join(lines)
        self.assertIn("detach", joined)
        self.assertIn("heartbeat", joined)
        self.assertNotIn("topic", joined)  # 刻意不提供 topic/key 设置
        self.assertEqual(pcm._run_magic("   ", ctx), (False, []))

    def test_remote_params_publish_set_and_clamp(self):
        tr, pty, ctx = self._ctx()
        n0 = len(tr.published)
        do_detach, lines = pcm._run_magic("interval 999", ctx)
        self.assertFalse(do_detach)
        self.assertEqual(ctx["live"]["interval"], 60.0)  # 上限夹取
        self.assertEqual(tr.published[n0][1]["set"], {"interval": 60.0})
        pcm._run_magic("heartbeat 0", ctx)
        self.assertEqual(ctx["live"]["heartbeat"], 0.0)
        pcm._run_magic("ttl 10", ctx)
        self.assertEqual(ctx["live"]["ttl"], 60.0)  # 下限夹取

    def test_local_dead_timeout_no_frame(self):
        tr, _, ctx = self._ctx()
        n0 = len(tr.published)
        pcm._run_magic("dead 3", ctx)
        self.assertEqual(ctx["live"]["dead_timeout"], 3.0)
        pcm._run_magic("dead 0", ctx)
        self.assertEqual(ctx["live"]["dead_timeout"], 0.0)
        # 纯本地参数不下发任何帧
        self.assertEqual(len(tr.published), n0)

    def test_bad_number_and_unknown_command(self):
        _, _, ctx = self._ctx()
        _, lines = pcm._run_magic("interval abc", ctx)
        self.assertTrue(any("数字" in x for x in lines))
        _, lines = pcm._run_magic("topic x", ctx)
        self.assertTrue(any("未知" in x for x in lines))

    def test_status_reports_session(self):
        _, pty, ctx = self._ctx()
        _, lines = pcm._run_magic("status", ctx)
        joined = "\n".join(lines)
        self.assertIn("pty-magic-1", joined)
        self.assertIn("broker", joined)


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

    def test_uplink_iseq_monotonic_gate_present(self):
        # iseq 单调闸门：既去多 broker 重复，也拦乱序迟到的旧帧（旧 winsz
        # 晚到覆盖新尺寸会让远端 PTY 比本地窗口宽，进度条全线错位）
        self.assertIn("_iseq_last", self.code)
        self.assertIn("_iq <= _iseq_last", self.code)
        self.assertNotIn("_iseq_recent", self.code)
        self.assertIn('"iseq"', self.code.replace("'", '"'))

    def test_live_set_params_present(self):
        # 会话内热调：_live 共享 dict + set 帧夹取，只允许时间参数
        self.assertIn("_live", self.code)
        self.assertIn('_fr.get("set")', self.code)
        self.assertIn('float(_live["interval"])', self.code)
        self.assertIn('_live.get("heartbeat"', self.code)
        # 不允许借 set 帧改 topic / shell 等
        self.assertNotIn('"in_topic": _ss', self.code)

    def test_owner_arbitration_present(self):
        # 多应答者归属仲裁：进程稳定 uid、下行帧盖 owner、外来 owner 帧
        # 让影子 PTY 自杀、同进程同 sid 幂等注册表
        self.assertIn("_cmq_server_uid", self.code)
        self.assertIn("_cmq_pty_sessions", self.code)
        self.assertIn("claim_lost", self.code)
        self.assertIn("_ow != _uid", self.code)
        self.assertIn('"owner": _uid', self.code)
        self.assertIn('"owner": _uid, "host": _host', self.code)


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

        def _type(self, sid, iseq, text, owner=None):
            fr = {"iseq": iseq, "k": text}
            if owner is not None:
                fr["owner"] = owner
            self._send(sid, fr)

        def _assert_echo(self, sid, marker, iseq, timeout=5.0, owner=None):
            self._type(sid, iseq, "echo %s\r" % marker, owner=owner)
            self.assertTrue(
                _wait_for(lambda: marker in _out_text(self.net, sid), timeout),
                "PTY 未在 %.0fs 内回显 %s，实际输出: %r"
                % (timeout, marker, _out_text(self.net, sid)))

        def _stop(self, sid, iseq=9999, owner=None):
            fr = {"iseq": iseq, "stop": True}
            if owner is not None:
                fr["owner"] = owner
            self._send(sid, fr)
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

        def test_winsz_out_of_order_older_size_must_not_win(self):
            # 界面错乱回归：拖窗口时慢 broker 把旧的（更大）winsz 晚投递到，
            # 服务端 iseq 单调闸门必须丢掉它，PTY 最终尺寸以新帧为准。
            sid = "pty-live-winsz-reorder"
            self._start(sid)
            self._send(sid, {"iseq": 1, "winsz": [40, 200]})
            self._send(sid, {"iseq": 2, "winsz": [20, 100]})
            # iseq=1 的旧尺寸再经慢 broker 送达（重复/乱序迟到同一处理路径）
            self._send(sid, {"iseq": 1, "winsz": [40, 200]})
            time.sleep(0.3)
            self._type(sid, 3, "stty size\r")
            self.assertTrue(
                _wait_for(lambda: "20 100" in _out_text(self.net, sid)),
                "PTY 实际尺寸不是新帧的 20x100: %r"
                % _out_text(self.net, sid))
            self.assertNotIn("40 200", _out_text(self.net, sid),
                             "旧 winsz 帧乱序迟到后覆盖了新尺寸")
            self._stop(sid, iseq=4)

        # ---- 多应答者归属仲裁（owner/claim） ----

        def test_start_env_and_downlink_frames_carry_owner(self):
            sid = "pty-live-owner"
            env = self._start(sid)
            self.assertTrue(env.get("owner"))
            self.assertEqual(len(env["owner"]), 12)  # 6 字节 urandom.hex
            self.assertTrue(env.get("host"))
            self._assert_echo(sid, "ZZOWNER42", iseq=0)
            # 输出/心跳/end 全部盖 owner，客户端才能按属主过滤影子流
            frames = self.net.frames("pty/%s/out" % sid)
            self.assertTrue(frames)
            self.assertTrue(all(d.get("owner") == env["owner"]
                                for _, d in frames))
            self._stop(sid, iseq=1, owner=env["owner"])
            self.assertEqual(_end_frame(self.net, sid).get("owner"),
                             env["owner"])

        def test_winner_claim_is_harmless_foreign_claim_kills_shadow(self):
            sid = "pty-live-claim"
            env = self._start(sid)
            owner = env["owner"]
            # 赢家自己的 claim 帧绝不能误杀会话
            self._send(sid, {"iseq": 1, "owner": owner, "claim": True})
            time.sleep(0.3)
            self.assertIsNone(_end_frame(self.net, sid))
            self._assert_echo(sid, "ZZCLAIMOK", iseq=2, owner=owner)
            # 外来 owner（同时应答的另一进程）→ 本 PTY 判定自己是影子，
            # 立即 claim_lost 结束并停止往同一 out topic 推流
            self._send(sid, {"iseq": 3, "owner": "ffffffdeadbe",
                             "claim": True})
            self.assertTrue(
                _wait_for(lambda: _end_frame(self.net, sid) is not None),
                "影子 PTY 未在收到外来 owner 帧后自杀")
            self.assertEqual(_end_frame(self.net, sid).get("reason"),
                             "claim_lost")

        def test_set_frame_hot_tunes_heartbeat_without_reconnect(self):
            sid = "pty-live-set"
            env = self._start(sid)  # 初始 heartbeat=0
            self._send(sid, {"iseq": 0, "owner": env["owner"],
                             "set": {"heartbeat": 1}})
            try:
                self.assertTrue(
                    _wait_for(
                        lambda: any(d.get("hb") is not None
                                    for _, d in self.net.frames(
                                        "pty/%s/out" % sid)),
                        timeout=5.0),
                    "set 热调 heartbeat 后未收到心跳帧")
                # 非法值被忽略，会话照常工作
                self._send(sid, {"iseq": 1, "owner": env["owner"],
                                 "set": {"heartbeat": "abc"}})
                self._assert_echo(sid, "ZZSETOK", iseq=2, owner=env["owner"])
            finally:
                self._stop(sid, iseq=3, owner=env["owner"])

        def test_duplicate_handshake_same_sid_returns_existing_session(self):
            # 同进程内同一 sid 重复握手（任何重复执行兜底）：返回既有会话，
            # 不得再开第二个往同 topic 推流的 PTY
            sid = "pty-live-dup"
            in_t, out_t = "pty/%s/in" % sid, "pty/%s/out" % sid
            payload = {
                "sid": sid, "in_topic": in_t, "out_topic": out_t,
                "rows": 24, "cols": 80, "shell": "/bin/sh", "term": "xterm",
                "cwd": None, "login": False, "flush_interval": 0.0,
                "ttl": 3600, "frame_max": PTY_FRAME_MAX, "heartbeat": 0.0,
            }
            env1 = self._start(sid)
            resp2 = PythonExecutor(globals=self.ns).execute(
                build_pty_start_code(payload))
            self.assertTrue(resp2["ok"])
            env2 = json.loads(resp2["r"])
            self.assertTrue(env2["ok"])
            self.assertEqual(env2, env1)  # 同一个会话（含同一 pid/owner）
            # 会话照常工作
            self._assert_echo(sid, "ZZDUPHAND", iseq=0, owner=env1["owner"])
            self._stop(sid, iseq=1, owner=env1["owner"])

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
