#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""socks5_client_mqtt 两轮并发/生命周期修复的端到端回归。

本文件与 test_socks5_mqtt.py 放在不带 __init__.py 的 tests/socks5/ 下：
默认的 ``python -m unittest discover -s tests`` 不会递归到本目录（普通项目
测试不自动带上 socks5）。专门运行 socks5 全套：

    python -m unittest discover -s tests/socks5 -v
    python tests/socks5/test_socks5_e2e.py

覆盖的修复点（对话全部历史，不止单轮 4 项）：

第一轮（服务端模板 nonlocal + paho 静默 rc + 客户端状态锁）
  T1 pb/ps 是 _cmq_socks5_start 的闭包变量：不得泄漏成 executor 全局名；
  T2 >2048B 数据大帧在 pb 名单到达后走 bulk 专用管理器（子集），不广播；
  T3 ps 到达前第一个大帧用闭包默认节奏即可投递（global 误用会 NameError
     打死 _bsend_loop，bulk 队列无人消费）；
  T4 paho publish() 返回 rc!=0 的静默丢弃计入 _cmq_s5_pub_err/_last，
     bulk 全败时回退主连接子集发送并同样记账（hb pe/pl 可见）；
  T5 客户端 state_lock：远端正常 closed 记 remote_closed 而非 local_error，
     对端收到 FIN；
  T6 drop 之后到达的迟到 closed/数据帧不再触发 local_error 收尾。

第二轮（交付串行化 + close 信号 + seq_lock 外发送）
  T7 多 broker 回调线程并发乱序 add，交付必须严格按 seq 0..N-1；重复副本
     不二次交付；close() 后专职交付线程退出（无 s5-deliver 残留）；
  T8 真实 socket 链路：120 帧逆序入 pending，opened(seq=0) 解锁后正序落
     本地 socket（bytes_down 计数一致，单交付线程无丢更新）；
  T9 wq 堆满时 closed 信号 None 入队失败不再被静默吞掉：强制收尾
     （close_signal_dropped），条目摘除、remote_closed 保留、不补发上行
     close；
  T10 阻塞中的 publish 不持有 seq_lock：数据帧发送被卡时 drop 立即摘除
     连接、close 帧独立打号入缓存，物理发送全部在锁外（seq 单调）；
  T11 30 连接 × 并发 deliver/drop 冲击无异常无残留。
"""
import json
import os
import socket
import sys
import threading
import time
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(
    os.path.abspath(__file__)))))

from rpc_executor import PythonExecutor  # noqa: E402
from client import socks5_client_mqtt as scm  # noqa: E402
from client.remote_cmd import Transport  # noqa: E402


def _wait_for(cond, timeout=5.0, step=0.02):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            if cond():
                return True
        except Exception:
            pass
        time.sleep(step)
    return False


# ===================== 第一轮：服务端模板侧 fakes =====================

class FakeClient:
    """假 paho client：publish 不抛异常，按 rc 返回 MQTTMessageInfo。"""

    def __init__(self, host, rc=0, connected=True):
        self.host = host
        self.rc = rc
        self.connected = connected
        self.sent = []

    def is_connected(self):
        return self.connected

    def publish(self, topic, payload, qos=0):
        self.sent.append((topic, payload))

        class _Info:
            pass

        info = _Info()
        info.rc = self.rc
        return info


# 服务端模板从 executor 全局取 MultiMQTTManager；按测试切换 client rc。
_FAKE_RC = [0]


class FakeMgr:
    instances = []

    def __init__(self, brokers, enable_stats=True, keepalive=300, **kw):
        self.brokers = list(brokers)
        self.lock = threading.Lock()
        self.clients = {h: FakeClient(h, rc=_FAKE_RC[0]) for h, _ in brokers}
        self.stop_ev = threading.Event()
        FakeMgr.instances.append(self)

    def start(self):
        pass

    def wait_connected(self, min_count=1, timeout=4.0):
        return True

    def publish_broadcast(self, topic, frame):
        pass  # 模板 bulk 路径不用它，逐个 client 发

    def stop(self):
        self.stop_ev.set()


class FakeNet:
    """模拟 gms.mqtt_net：主连接 clients + 广播记录 + 上行投递口。"""

    def __init__(self, clients=None):
        self.clients = dict(clients or {})
        self.lock = threading.Lock()
        self.message_callback = None
        self.subscribed = []
        self.published = []
        self.enable_crypto = False

    def set_on_message(self, cb):
        self.message_callback = cb

    def subscribe(self, topic):
        self.subscribed.append(topic)

    def publish_broadcast(self, topic, payload):
        with self.lock:
            self.published.append((topic, dict(payload)))

    def deliver(self, topic, data):
        self.message_callback(topic, data, "fakebroker")

    def frames(self, topic):
        with self.lock:
            return [(t, dict(d)) for t, d in self.published if t == topic]


class FakeGms:
    def __init__(self, net):
        self.mqtt_net = net


class BigSend(threading.Thread):
    """accept 后立即发 n 字节的一次性目标服务器。"""

    def __init__(self, n=4096, hold=1.5):
        super().__init__(daemon=True)
        self.n = n
        self.hold = hold
        self.ls = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.ls.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.ls.bind(("127.0.0.1", 0))
        self.ls.listen(1)
        self.port = self.ls.getsockname()[1]

    def run(self):
        try:
            self.ls.settimeout(10)
            conn, _ = self.ls.accept()
        except OSError:
            return
        try:
            conn.settimeout(5)
            conn.sendall(b"X" * self.n)
            time.sleep(self.hold)
        except OSError:
            pass
        finally:
            try:
                conn.close()
            except OSError:
                pass


class ServerTemplateFixTests(unittest.TestCase):
    """第一轮修复 1/2/3：nonlocal 闭包 + 大帧 bulk 路径。"""

    HB = 0.2

    def setUp(self):
        FakeMgr.instances = []
        _FAKE_RC[0] = 0
        self.net = FakeNet()
        self.gms = FakeGms(self.net)
        self.ns = {
            "__name__": "__rpc_exec__",
            "gms": self.gms,
            "MultiMQTTManager": FakeMgr,
            "BROKER_LIST": [("fakebroker", 1883), ("otherbroker", 1883)],
        }
        self.sid = "s5fix-%d-%d" % (id(self), int(time.time() * 1000))
        self.in_t = "s5/%s/in" % self.sid
        self.out_t = "s5/%s/out" % self.sid
        self.big = None
        self._started = False

    def tearDown(self):
        try:
            if self._started:
                self.net.deliver(self.in_t, {"s5": self.sid, "stop": True})
                _wait_for(lambda: not [
                    t for t in threading.enumerate()
                    if t.name.startswith(("s5-conn-", "s5-wr-"))
                    and t.is_alive()], timeout=5.0)
        finally:
            if self.big is not None:
                try:
                    self.big.ls.close()
                except OSError:
                    pass

    def _start(self):
        payload = {
            "sid": self.sid, "in_topic": self.in_t,
            "out_topic": self.out_t, "frame_max": 16384,
            "ttl": 300, "heartbeat": self.HB,
            "connect_timeout": 2.0, "gap_timeout": 15.0,
        }
        resp = PythonExecutor(globals=self.ns).execute(
            scm.build_socks5_start_code(payload))
        self.assertTrue(resp["ok"],
                        resp.get("stdout", "") + resp.get("error", ""))
        env = json.loads(resp["r"])
        self.assertTrue(env["ok"], env.get("error"))
        self._started = True
        return env

    def _send(self, partial):
        frame = {"s5": self.sid}
        frame.update(partial)
        self.net.deliver(self.in_t, frame)

    def _open_cid(self, cid, port):
        # 上行首帧 seq=0=open，与真实客户端打号一致
        self.net.deliver(self.in_t, {
            "s5": self.sid, "cid": cid, "seq": 0,
            "open": {"host": "127.0.0.1", "port": int(port)}})

    def _cid_broadcast(self, cid):
        return [d for _, d in self.net.frames(self.out_t)
                if d.get("cid") == cid]

    def _bulk_payloads(self):
        out = []
        for mgr in FakeMgr.instances:
            for cl in mgr.clients.values():
                out.extend(p for _, p in cl.sent)
        return out

    def _drive_big_frame(self, cid, send_ps=True, broker="fakebroker"):
        """pb(+可选 ps) 下发后打通一条大帧连接，返回目标服务器。"""
        self._send({"pb": [broker]})
        if send_ps:
            self._send({"ps": 0.05})
        self.big = BigSend()
        self.big.start()
        self._open_cid(cid, self.big.port)

    def test_t1_pb_ps_are_closure_vars_not_globals(self):
        self._start()
        self._send({"pb": ["fakebroker"]})
        self._send({"ps": 0.05})
        # global 误用会把它们写进 executor 持久命名空间（多会话共享串味）
        ok = _wait_for(lambda: any(
            isinstance(d, dict) and d.get("hb") is not None
            for _, d in self.net.frames(self.out_t)), timeout=2.0)
        self.assertTrue(ok, "服务端心跳未起来")
        for leaked in ("_pb", "_pace_intv", "_bnet", "_bhosts"):
            self.assertNotIn(leaked, self.ns,
                             "闭包变量 %s 泄漏成 executor 全局名" % leaked)

    def test_t2_big_frame_goes_bulk_not_broadcast(self):
        self._start()
        cid = 1
        self._drive_big_frame(cid, send_ps=True)
        # 4096B 必须出现在 bulk 专用连接的 client 上
        self.assertTrue(_wait_for(
            lambda: any("X" * 512 in p for p in self._bulk_payloads()),
            timeout=8.0), "大帧未走 bulk 专用连接（pb nonlocal 失效？）")
        time.sleep(0.15)  # 让可能的误广播落地
        bc = self._cid_broadcast(cid)
        self.assertTrue(any("opened" in d for d in bc),
                        "opened 控制帧应走主连接广播")
        self.assertFalse(any("d" in d for d in bc),
                         "大帧绝不能走主连接 13× 广播")
        self.assertTrue(FakeMgr.instances
                        and any(m.clients for m in FakeMgr.instances),
                        "bulk 管理器未建立")
        # stop 必须停掉 bulk 管理器
        self.net.deliver(self.in_t, {"s5": self.sid, "stop": True})
        self._started = False
        self.assertTrue(_wait_for(
            lambda: all(m.stop_ev.is_set() for m in FakeMgr.instances),
            timeout=4.0), "stop 后 bulk 管理器未停止")

    def test_t3_big_frame_before_ps_uses_closure_default_pace(self):
        self._start()
        cid = 2
        self._drive_big_frame(cid, send_ps=False)
        # global 误用下 _bsend_loop 首帧读未定义全局名 → NameError 线程死亡，
        # _bq 永远无人消费；闭包默认节奏下应正常投递。
        self.assertTrue(_wait_for(
            lambda: any("X" * 512 in p for p in self._bulk_payloads()),
            timeout=8.0), "ps 到达前大帧未投递（_pace_intv 非闭包？）")
        bulk_threads = [t for t in threading.enumerate()
                        if t.name == "s5-bulk" and t.is_alive()]
        self.assertTrue(bulk_threads, "_bsend_loop 已异常死亡")

    def test_t4_paho_rc_nonzero_is_accounted(self):
        # 主连接与 bulk 连接的 client 全部返回 rc=4（MQTT_ERR_NO_CONN）
        _FAKE_RC[0] = 4
        self.net = FakeNet(clients={"badbroker": FakeClient(
            "badbroker", rc=4, connected=True)})
        self.gms = FakeGms(self.net)
        self.ns["gms"] = self.gms
        self.ns["BROKER_LIST"] = [("badbroker", 1883)]
        self._start()
        cid = 3
        self._drive_big_frame(cid, send_ps=True, broker="badbroker")
        self.assertTrue(_wait_for(
            lambda: self.ns.get("_cmq_s5_pub_err", 0) >= 2, timeout=8.0),
            "paho rc!=0 静默失败未记账（bulk+回退至少两笔），实际 %r last=%r"
            % (self.ns.get("_cmq_s5_pub_err"),
               self.ns.get("_cmq_s5_pub_last")))
        self.assertIn("paho rc=4", self.ns.get("_cmq_s5_pub_last", ""))
        # 记账随心跳 pe/pl 暴露给客户端
        self.assertTrue(_wait_for(
            lambda: any(d.get("pe", 0) >= 1
                        for _, d in self.net.frames(self.out_t)
                        if d.get("hb") is not None), timeout=2.0),
                        "心跳未捎带 pub 失败计数")


# ===================== 第二轮：客户端侧 fakes =====================

class FakeTransport(Transport):
    """RemoteSocks5 的内存传输：request 即执行即回 env，publish 全记录。

    small_block（threading.Event）置为 clear 状态时，含 "d" 的 publish 会
    阻塞在 wait(5s) 上，用于制造"物理发送卡住但锁必须已释放"的场景。
    """

    def __init__(self, env):
        self.env = env
        self.published = []
        self.lock = threading.Lock()
        self.handlers = {}
        self.small_block = None

    def request(self, code, timeout=30.0):
        return {"ok": True, "r": json.dumps(self.env), "stdout": ""}

    def request_many(self, code, timeout, owner_gather=0.8):
        return {"ok": True, "r": json.dumps(self.env), "stdout": ""}, []

    def publish(self, topic, payload):
        ev = self.small_block
        if (ev is not None and isinstance(payload, dict)
                and "d" in payload):
            ev.wait(5.0)
        with self.lock:
            self.published.append((topic, payload))

    def stream_subscribe(self, topic, handler):
        self.handlers[topic] = handler

    def stream_unsubscribe(self, topic, handler):
        self.handlers.pop(topic, None)


class FakeClientMgr:
    """客户端 _ensure_bnet 用的假 MultiMQTTManager。"""

    def __init__(self, brokers, **kw):
        self.brokers = list(brokers)
        self.clients = {}
        self.lock = threading.Lock()
        self.stop_ev = threading.Event()

    def start(self):
        pass

    def wait_connected(self, min_count=1, timeout=4.0):
        return True

    def publish_broadcast(self, topic, frame):
        pass

    def stop(self):
        self.stop_ev.set()


def _wait_thread_gone(name, timeout=3.0):
    return _wait_for(
        lambda: not any(t.name == name and t.is_alive()
                        for t in threading.enumerate()),
        timeout=timeout)


def _recv_n(sock, n, timeout=5.0):
    sock.settimeout(timeout)
    buf = b""
    while len(buf) < n:
        try:
            chunk = sock.recv(n - len(buf))
        except socket.timeout:
            break
        if not chunk:
            break
        buf += chunk
    return buf


class ReassemblyOrderingTests(unittest.TestCase):
    """修复点 T7：_ConnReassembly 纯单元级并发保序。"""

    def test_t7_concurrent_out_of_order_strict_sequence(self):
        got = []
        glock = threading.Lock()

        def deliver(fr):
            time.sleep(0.01)  # 放大"锁外交付"竞态窗口
            if "seq" in fr:
                with glock:
                    got.append(fr["seq"])

        rb = scm._ConnReassembly(
            deliver, lambda: None, lambda seqs: None, name="T")
        n, nw = 128, 8
        seg = n // nw
        barrier = threading.Barrier(nw)

        def worker(base):
            barrier.wait()
            for s in range(base, base + seg):
                rb.add(s, {"seq": s})

        ts = [threading.Thread(target=worker, args=(i * seg,))
              for i in range(nw)]
        for t in ts:
            t.start()
        for t in ts:
            t.join()
        self.assertTrue(_wait_for(lambda: len(got) == n, timeout=6.0),
                        "交付未排空：%d/128" % len(got))
        self.assertEqual(got, list(range(n)),
                         "乱序点=%r" % [i for i, s in enumerate(got) if i != s][:5])

        # 迟到重复副本绝不二次交付
        before = len(got)
        rb.add(5, {"seq": 5})
        rb.add(0, {"seq": 0})
        time.sleep(0.1)
        self.assertEqual(len(got), before)

        rb.close()
        self.assertTrue(_wait_thread_gone("s5-deliver-T"),
                        "close() 后交付线程未退出")


class ClientSessionFixTests(unittest.TestCase):
    """修复点 T5/T6/T8/T9/T10/T11：真实 RemoteSocks5 + 真实本地 writer。"""

    def setUp(self):
        self._old_bl = scm.BROKER_LIST
        self._old_cls = scm.MultiMQTTManager
        # open() 会按 _PB_SEED 建本端 bulk 连接，名单必须全部可解析
        scm.BROKER_LIST = [(h, 1883) for h in scm._PB_SEED]
        scm.MultiMQTTManager = FakeClientMgr
        self.addCleanup(self._restore_module)

        self.logs = []
        self.tr = FakeTransport({"ok": True, "owner": "owner-1",
                                 "host": "fakehost"})
        self.sess = scm.RemoteSocks5(
            self.tr, timeout=5.0, on_log=self.logs.append)
        self.sess.open(heartbeat=0.5, ttl=60.0)
        self.srv = scm._Socks5Server(self.sess, "127.0.0.1", 0)

    def _restore_module(self):
        scm.BROKER_LIST = self._old_bl
        scm.MultiMQTTManager = self._old_cls

    def tearDown(self):
        try:
            self.sess.close()
        except Exception:
            pass

    def _conn(self, cid):
        with self.sess._lock:
            return self.sess._conns.get(cid)

    def _uplink(self, cid):
        with self.tr.lock:
            return [d for _, d in self.tr.published
                    if isinstance(d, dict) and d.get("cid") == cid]

    # ---- T5：远端正常 closed → clean EOF，不记 local_error ----

    def test_t5_remote_closed_is_clean_eof_with_fin(self):
        cid = self.sess.open_connection("127.0.0.1", 1)
        conn = self._conn(cid)
        a, b = socket.socketpair()
        self.addCleanup(lambda: (a.close(), b.close()))
        conn.local_sock = a
        conn.local_read_done.set()  # 模拟本地读线程已退出
        wt = threading.Thread(target=self.srv._local_writer,
                              args=(cid, a), daemon=True)
        wt.start()

        conn.reassembly.add(0, {"seq": 0, "opened": {"ok": True}})
        conn.reassembly.add(1, {"seq": 1, "closed": True})
        self.assertTrue(wt.join(timeout=5.0) is None and not wt.is_alive(),
                        "writer 未退出")
        self.assertEqual(_recv_n(b, 8, timeout=3.0), b"",
                         "clean EOF 应对端收到 FIN")
        self.assertTrue(any("remote_closed" in m for m in self.logs),
                        "正常远端关闭应记 remote_closed: %r" % self.logs)
        self.assertFalse(any("local_error" in m for m in self.logs),
                         "正常远端关闭被误判 local_error")
        self.assertIsNone(self._conn(cid))

    # ---- T6：drop 后的迟到帧不得触发 local_error ----

    def test_t6_late_frames_after_drop_no_local_error(self):
        cid = self.sess.open_connection("127.0.0.1", 2)
        self.sess.drop_connection(cid, reason="test_drop")
        h = self.tr.handlers[self.sess.out_topic]
        # 连接已摘除：_on_frame 直接忽略，不碰 writer/状态位
        h({"s5": self.sess.sid, "cid": cid, "seq": 0,
           "opened": {"ok": True}}, "fakebroker")
        h({"s5": self.sess.sid, "cid": cid, "seq": 1,
           "closed": True}, "fakebroker")
        time.sleep(0.2)
        self.assertFalse(any("local_error" in m for m in self.logs),
                         "迟到帧触发了 local_error: %r" % self.logs)

    # ---- T8：逆序 pending 解锁后正序落本地 socket ----

    def test_t8_reversed_frames_land_in_order_on_socket(self):
        cid = self.sess.open_connection("127.0.0.1", 3)
        conn = self._conn(cid)
        a, b = socket.socketpair()
        self.addCleanup(lambda: (a.close(), b.close()))
        conn.local_sock = a
        wt = threading.Thread(target=self.srv._local_writer,
                              args=(cid, a), daemon=True)
        wt.start()

        m = 120  # < _INGRESS_MAX(128)，全部允许 pending
        for s in range(m, 0, -1):
            conn.reassembly.add(s, {"seq": s, "d": chr(65 + s % 26)})
        # opened seq=0 连锁解锁：120 个数据帧必须按 1..120 交付
        conn.reassembly.add(0, {"seq": 0, "opened": {"ok": True}})
        expected = "".join(chr(65 + s % 26) for s in range(1, m + 1)) \
            .encode("latin-1")
        got = _recv_n(b, m, timeout=5.0)
        self.assertEqual(len(got), m, "只收到 %d/120 字节" % len(got))
        self.assertEqual(got, expected, "字节序被打乱")
        self.assertEqual(conn.bytes_down, m, "bytes_down 计数丢更新")

        # 收尾走 clean EOF
        conn.reassembly.add(m + 1, {"seq": m + 1, "closed": True})
        conn.local_read_done.set()
        self.assertTrue(wt.join(timeout=5.0) is None and not wt.is_alive())
        self.assertEqual(_recv_n(b, 8, timeout=3.0), b"")

    # ---- T9：wq 满时 closed 信号强收尾 ----

    def test_t9_closed_signal_dropped_forces_teardown(self):
        cid = self.sess.open_connection("127.0.0.1", 4)
        conn = self._conn(cid)
        for _ in range(scm._WQ_MAX):
            conn.wq.put_nowait(b"x")
        # 无 writer 消费：closed 帧的 None 必遇 queue.Full
        conn.reassembly.deliver_now({"closed": True})

        self.assertTrue(_wait_for(lambda: self._conn(cid) is None, 3.0),
                        "连接条目未摘除，writer 将永久挂死并泄漏 fd/线程")
        self.assertTrue(conn.closed)
        self.assertTrue(conn.remote_closed, "remote_closed 语义必须保留")
        self.assertTrue(any("close_signal_dropped" in m for m in self.logs),
                        "未记录 close_signal_dropped: %r" % self.logs)
        # 对端已先关：不得补发上行 close（wq 里也只有 open）
        kinds = [k for fr in self._uplink(cid)
                 for k in ("open", "close", "d") if k in fr]
        self.assertEqual(kinds, ["open"], "意外上行帧: %r" % kinds)
        self.assertTrue(_wait_thread_gone("s5-deliver-%d" % cid),
                        "交付线程残留")

    # ---- T10：物理发送阻塞不得占用 seq_lock ----

    def test_t10_blocked_publish_does_not_hold_seq_lock(self):
        cid = self.sess.open_connection("127.0.0.1", 5)
        conn = self._conn(cid)
        block = threading.Event()
        block.set()  # open 阶段不挡
        self.tr.small_block = block

        errors = []

        def send_data():
            try:
                self.sess.send_data(cid, b"abc")  # seq=1
            except Exception as e:  # noqa: BLE001
                errors.append(repr(e))

        block.clear()  # 此后含 d 的 publish 物理阻塞（wait 5s）
        dt = threading.Thread(target=send_data, daemon=True)
        dt.start()
        self.assertTrue(_wait_for(
            lambda: 1 in conn.ucache and dt.is_alive(), 2.0),
            "数据帧未在预期位置阻塞")

        # 关键 1：发送卡在 paho 时，seq_lock 必须立即可得
        self.assertTrue(conn.seq_lock.acquire(timeout=0.5),
                        "seq_lock 被阻塞中的发送持有")
        conn.seq_lock.release()

        # 关键 2：drop 不等待物理发送：立即摘除并独立给 close 打号(seq=2)
        dthread = threading.Thread(
            target=self.sess.drop_connection, args=(cid,),
            kwargs={"reason": "test_block"}, daemon=True)
        dthread.start()
        try:
            self.assertTrue(_wait_for(lambda: self._conn(cid) is None, 1.5),
                            "drop 被阻塞发送拖住，连接未及时摘除")
            self.assertTrue(_wait_for(lambda: 2 in conn.ucache, 1.5),
                            "close 帧未在锁外独立打号入缓存")
        finally:
            block.set()
        dt.join(timeout=3.0)
        dthread.join(timeout=3.0)
        self.assertFalse(dt.is_alive(), "发送线程未退出")
        self.assertFalse(errors, "send_data 异常: %r" % errors)
        # 锁外发送物理顺序不保证（close 先于被堵数据落地是设计内行为，
        # 服务端按 per-cid seq 重组）；要保证的是打号集合单调连续。
        seqs = [d["seq"] for _, d in self.tr.published
                if isinstance(d, dict) and "seq" in d]
        self.assertEqual(sorted(seqs), [0, 1, 2],
                         "上行打号不连续: %r" % seqs)
        self.assertEqual(len(seqs), len(set(seqs)), "seq 重复: %r" % seqs)

    # ---- T11：deliver/drop 并发冲击 ----

    def test_t11_concurrent_deliver_drop_hammer(self):
        nc = 30
        cids = [self.sess.open_connection("127.0.0.1", 100 + i)
                for i in range(nc)]
        h = self.tr.handlers[self.sess.out_topic]
        errors = []

        def deliverer(cid):
            try:
                for i in range(20):
                    h({"s5": self.sess.sid, "cid": cid, "seq": i,
                       "d": "x"}, "fakebroker")
            except Exception as e:  # noqa: BLE001
                errors.append(("d", cid, repr(e)))

        def dropper(cid):
            try:
                time.sleep(0.002 * (cid % 7))
                self.sess.drop_connection(cid, reason="hammer")
            except Exception as e:  # noqa: BLE001
                errors.append(("k", cid, repr(e)))

        ts = []
        for cid in cids:
            ts.append(threading.Thread(target=deliverer, args=(cid,)))
            ts.append(threading.Thread(target=dropper, args=(cid,)))
        for t in ts:
            t.start()
        for t in ts:
            t.join(timeout=5.0)
        self.assertFalse(errors, "并发冲击出现异常: %r" % errors[:3])
        self.assertTrue(_wait_for(lambda: not self.sess._conns, 3.0),
                        "连接残留: %r" % list(self.sess._conns))

    # ---- 收尾：全程不得有交付线程泄漏（按字母序最后执行） ----

    def test_zzz_no_s5_delivery_thread_residue(self):
        self.sess.abort_all()
        leftover = [t.name for t in threading.enumerate()
                    if t.name.startswith("s5-deliver-") and t.is_alive()]
        self.assertFalse(leftover, "交付线程残留: %r" % leftover)


if __name__ == "__main__":
    unittest.main(verbosity=2)
