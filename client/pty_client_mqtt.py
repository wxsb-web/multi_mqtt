#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""pty_client_mqtt —— PTY over 公共 MQTT broker（SSH 式交互终端）。

模型（和 SSH 一样）
===================
- 仿 cmd_client_mqtt 建立 MQTT 连接（默认 ``sys/device/request`` ↔ response）；
- 连接成功后，client 生成会话 id 并提出 in/out 两个**新 topic**，协商成功后
  用于实时数据传输（底层仍完全复用 multi_mqtt.MultiMQTTManager，不新建连接）；
- 服务端只要求运行通用 server_mqtt.py：PTY 全部功能代码（openpty + 常驻
  用户 shell + 读写线程）由本 client 在握手时**整段下发执行**；shell 只启动
  一次，之后每个按键都经 broker 写入同一个 PTY，绝不在每条命令上重启 bash；
- 服务端主动发送频率可用 ``--interval`` 设定（默认 0=有输出立刻实时回传；
  >0 时按该秒数攒批合并输出，适合高延迟网络减少帧数）。

用法
====
    python client/pty_client_mqtt.py                    # 交互式 PTY（默认参数）
    python client/pty_client_mqtt.py -i 0.2             # 服务端最多 0.2s 攒批
    python client/pty_client_mqtt.py --shell /bin/bash --cwd /root
    python client/pty_client_mqtt.py -t q -k 2**128 "tmux at"
                                                        # SSH 式：连上自动执行
                                                        # 前置命令，随后留在会话里

AI 常驻调用（监控窗口模型，避免每条命令重连 broker）
====================================================
本进程启动时内置一个本地 HTTP RPC 口（默认 1188）。人类先在一个终端里
开着本窗口，AI / 其他进程不新建 MQTT 连接，直接 HTTP 复用同一个 PTY，
命令回显与输出照样在本窗口实时可见：

    from client.pty_client_mqtt import ai_pty_run, ai_pty_status, ai_pty_send
    ai_pty_status()                 # {'attached': True, 'brokers_online': 13, ...}
    ai_pty_run("uname -a")          # -> {'ok': True, 'rc': 0, 'out': '...'}
    ai_pty_run("apt install -y htop", timeout=300)
    ai_pty_send("y\\n")             # 回答交互提示（密码/确认）

裸 HTTP 等价写法（POST 一段 Python 到本地口）：

    curl "http://127.0.0.1:1188/$(python -c \
"import urllib.parse;print(urllib.parse.quote('import json;p.set_data(json.dumps(ai_bridge.run(\\\"uname -a\\\"),ensure_ascii=False))'))")"

注意：AI 命令串行执行（一个 shell），重叠调用立刻返回 busy；run() 只
回收非交互命令的输出，vim/top 等全屏程序请人工在窗口里操作。仅本机
调用建议 --host 127.0.0.1。

断开
====
- 在远端 shell 里 ``exit`` / Ctrl-D（会话自然结束，客户端立即退出）；
- 本地强制脱离：Ctrl-]（默认，可用 ``--detach-key`` 修改）；
- 服务器进程被关 / 网络中断：心跳超时（``--heartbeat`` / ``--dead-timeout``）
  后客户端自动立即退出，绝不在 broker 仍在线时无限干等。

所有退出路径都学 client_mqtt.py 直接 ``os._exit``：先恢复本地终端模式、
fire-and-forget 发一帧 stop，不做 pty.close/transport.stop 那套慢清理。
"""
from __future__ import annotations

import argparse
import os
import queue
import re
import shlex
import shutil
import sys
import threading
import time

# 本文件已迁移到 client/ 子目录：把项目根目录与本目录加入 sys.path，
# 同时兼容「python client/pty_client_mqtt.py」直接运行与包导入。
_HERE = os.path.dirname(os.path.abspath(__file__))
for _p in (os.path.dirname(_HERE), _HERE):
    if _p not in sys.path:
        sys.path.insert(0, _p)
# 直接按脚本启动时没有包上下文，相对导入会失败，显式补上。
if not __package__:
    __package__ = "client"

from .cmd_client_mqtt import (              # noqa: E402
    MqttTransport, add_connection_args,
)
from .remote_cmd import (                   # noqa: E402
    RemotePty, RemoteError, DEFAULT_PTY_TTL,
)

from multi_mqtt import stime, BROKER_LIST


# ============================ 本地终端：输出 ANSI 支持 ============================

def _enable_output_vt() -> bool:
    """让本地终端能渲染远端 ANSI 输出。POSIX 原生支持；Windows 打开 VT 处理 + UTF-8。"""
    if sys.platform != "win32":
        return True
    try:
        import ctypes
        k = ctypes.windll.kernel32
        k.SetConsoleOutputCP(65001)
        h = k.GetStdHandle(-11)  # STD_OUTPUT_HANDLE
        mode = ctypes.c_uint32()
        if not k.GetConsoleMode(h, ctypes.byref(mode)):
            return False
        # ENABLE_VIRTUAL_TERMINAL_PROCESSING = 0x0004
        return bool(k.SetConsoleMode(h, mode.value | 0x0004))
    except Exception:
        return False


# ============================ 本地终端：raw 输入 ============================

class _PosixRawConsole:
    """termios raw：ISIG/ICANON/ECHO 全关，每个按键直接可读。"""

    def __init__(self):
        import termios
        self._termios = termios
        self.fd = sys.stdin.fileno()
        self.old = termios.tcgetattr(self.fd)

    def enter(self):
        import tty
        tty.setraw(self.fd, self._termios.TCSANOW)

    def read(self) -> bytes:
        return os.read(self.fd, 4096)

    def exit(self):
        self._termios.tcsetattr(self.fd, self._termios.TCSANOW, self.old)


class _WinRawConsole:
    """Windows 输入：优先 ENABLE_VIRTUAL_TERMINAL_INPUT（ReadFile 直接给 VT 序列），
    老系统 / 失败时退回 msvcrt.getwch + 按键翻译。"""

    ENABLE_VIRTUAL_TERMINAL_INPUT = 0x0200

    def __init__(self):
        import ctypes
        self.k = ctypes.windll.kernel32
        self.h = self.k.GetStdHandle(-10)  # STD_INPUT_HANDLE
        self.old_mode = None
        self.use_vt = False

    def enter(self):
        import ctypes
        mode = ctypes.c_uint32()
        if self.k.GetConsoleMode(self.h, ctypes.byref(mode)):
            self.old_mode = mode.value
            if self.k.SetConsoleMode(self.h, self.ENABLE_VIRTUAL_TERMINAL_INPUT):
                self.use_vt = True
        self.k.SetConsoleOutputCP(65001)

    def read(self) -> bytes:
        if self.use_vt:
            return os.read(0, 4096)
        return self._msvcrt_read()

    def exit(self):
        if self.old_mode is not None:
            self.k.SetConsoleMode(self.h, self.old_mode)

    @staticmethod
    def _msvcrt_read() -> bytes:
        import msvcrt
        ch = msvcrt.getwch()
        if ch in ("\x00", "\xe0"):
            code = ord(msvcrt.getwch())
            table = {
                72: b"\x1b[A",    # Up
                80: b"\x1b[B",    # Down
                77: b"\x1b[C",    # Right
                75: b"\x1b[D",    # Left
                71: b"\x1b[H",    # Home
                79: b"\x1b[F",    # End
                73: b"\x1b[5~",   # PgUp
                81: b"\x1b[6~",   # PgDn
                82: b"\x1b[2~",   # Insert
                83: b"\x1b[3~",   # Delete
                59: b"\x1bOP", 60: b"\x1bOQ", 61: b"\x1bOR", 62: b"\x1bOS",  # F1-F4
                63: b"\x1b[15~", 64: b"\x1b[17~", 65: b"\x1b[18~", 66: b"\x1b[19~",
                67: b"\x1b[20~", 68: b"\x1b[21~",                              # F5-F10
            }
            return table.get(code, b"")
        if ch == "\x08":  # Backspace -> DEL（远端 tty 约定）
            return b"\x7f"
        return ch.encode("utf-8", "replace")


def _make_raw_console():
    return _WinRawConsole() if sys.platform == "win32" else _PosixRawConsole()


# ============================ 连接详情 ============================

def _broker_status(transport):
    """返回 (在线数, 总数, 在线 broker 名列表)。拿不到状态时总数退化为配置数。"""
    net = getattr(getattr(transport, "node", None), "mqtt_net", None)
    clients = getattr(net, "clients", None)
    if not clients:
        return 0, len(BROKER_LIST), []
    hosts = []
    for host, cli in clients.items():
        try:
            if cli.is_connected():
                hosts.append(str(host))
        except Exception:
            pass
    return len(hosts), len(clients), hosts


def _info(msg):
    sys.stderr.write("[%s] %s\n" % (stime(), msg))
    sys.stderr.flush()


# ============================ AI 桥：外部进程复用本常驻 PTY ============================

# CSI / SGR / OSC / 单字符转义，用于把 AI 收回的终端字节还原成纯文本
_ANSI_RE = re.compile(rb"\x1b(?:\][^\x07\x1b]*(?:\x07|\x1b\\)|\[[0-?]*[ -/]*[@-~]|[@-Z\\-_])")


def _strip_ansi(b: bytes) -> str:
    return _ANSI_RE.sub(b"", b).replace(b"\r\n", b"\n").replace(b"\r", b"\n").decode("utf-8", "replace")


class AIBridge:
    """把这个常驻 PTY 暴露给 AI / 外部进程调用，命令全程可见。

    背景：每次 ``python cmd_client_mqtt.py`` 都是新进程，要重新并发连接
    十几个公共 broker（首个上线通常 <1s，慢时数秒）。本进程启动时已内置
    一个本地 HTTP RPC 端口（默认 1188，见 main 的 server_http），外部
    进程**不新建 MQTT 连接**，HTTP 下发一行 Python 即可复用本 PTY：

        import json
        p.set_data(json.dumps(ai_bridge.run("uname -a"), ensure_ascii=False))
        ai_bridge.send("y\\n")      # 给交互提示喂键（密码/y/n）
        ai_bridge.status()          # 会话存活 / broker 在线数 / 是否忙

    run() 把命令包在一对随机标记之间写入同一个常驻 shell，回收两个标记
    之间的输出与退出码；输入回显和输出照样实时渲染在监控窗口。PTY 只有
    一个 shell，命令用锁串行化，重叠调用立刻拿到 busy 而不是互相串台。
    """

    def __init__(self):
        self.pty = None
        self.transport = None
        self._listeners = []
        self._llock = threading.Lock()
        self._busy = threading.Lock()

    # ---- 会话侧调用（run_session 内挂载） ----

    def attach(self, pty, transport):
        self.pty = pty
        self.transport = transport

    def feed(self, chunk: bytes):
        """PTY 下行分流：主窗口渲染之外，给每个 run() 收集器一份副本。"""
        with self._llock:
            qs = list(self._listeners)
        for q in qs:
            try:
                q.put_nowait(chunk)
            except queue.Full:
                pass

    # ---- AI 侧调用（经本地 HTTP RPC 进入本进程） ----

    def status(self) -> dict:
        pty, tr = self.pty, self.transport
        online, total, hosts = _broker_status(tr) if tr is not None else (0, 0, [])
        return {
            "ok": True,
            "attached": pty is not None,
            "end_reason": getattr(pty, "end_reason", None),
            "busy": self._busy.locked(),
            "sid": getattr(pty, "sid", None),
            "brokers_online": online,
            "brokers_total": total,
            "brokers": hosts,
        }

    def send(self, data) -> dict:
        """原样发按键/字节，不做标记回收（回答密码、y/n、进 vim 后操作等）。"""
        pty = self.pty
        if pty is None or getattr(pty, "end_reason", None):
            return {"ok": False,
                    "error": "pty 未连接或已结束: %s" % getattr(pty, "end_reason", None)}
        if isinstance(data, str):
            data = data.encode("utf-8")
        pty.send(bytes(data))
        return {"ok": True, "sent": len(data)}

    def run(self, cmd, timeout: float = 60.0, acquire_timeout: float = 2.0) -> dict:
        """在常驻 shell 里跑一条命令，等结束标记，返回 rc/输出；窗口全程可见。

        - 超时只停止回收，不杀远端命令（输出继续在窗口里刷，可用 send 干预）；
        - 退出码取自 ``sh -c '<cmd>'``，外层登录 shell 需为 POSIX 系（sh/bash）。
        """
        pty = self.pty
        if pty is None:
            return {"ok": False, "error": "pty 尚未连接"}
        if pty.end_reason:
            return {"ok": False, "error": "pty 已结束: %s" % pty.end_reason}
        if not self._busy.acquire(timeout=max(0.0, float(acquire_timeout))):
            return {"ok": False, "busy": True,
                    "error": "busy：上一条 AI 命令尚未结束"}

        tag = os.urandom(4).hex()
        begin = "__AI_BEGIN_%s__" % tag
        end = "__AI_END_%s__" % tag
        q = self._add_listener()
        line = ("echo %s; sh -c %s; __ai_rc=$?; echo %s:$__ai_rc\r"
                % (begin, shlex.quote(str(cmd)), end))
        buf = b""
        timed_out = False
        m_end = None
        try:
            pty.send(line.encode("utf-8"))
            deadline = time.monotonic() + float(timeout)
            end_re = re.compile(re.escape(end.encode()) + rb":(\d+)")
            while True:
                if pty.end_reason:
                    break
                remain = deadline - time.monotonic()
                if remain <= 0:
                    timed_out = True
                    break
                try:
                    chunk = q.get(timeout=min(0.5, remain))
                except queue.Empty:
                    continue
                buf += chunk
                m_end = end_re.search(buf)
                if m_end:
                    break
        finally:
            self._remove_listener(q)
            self._busy.release()

        # 真起点是「独占一行的 BEGIN + 换行」；输入回显里 BEGIN 后面是 ';'，
        # 不会误中。结尾 END:<数字> 同理（回显里是 :$__ai_rc 字面量）。
        m_begin = re.search(re.escape(begin.encode()) + rb"\r?\n", buf)
        start = m_begin.end() if m_begin else 0
        if m_end:
            body, rc = buf[start:m_end.start()], int(m_end.group(1))
        else:
            body, rc = buf[start:], None
        return {
            "ok": rc == 0 if rc is not None else False,
            "rc": rc,
            "timed_out": timed_out,
            "out": _strip_ansi(body).strip("\n"),
            "raw_len": len(body),
            "end_reason": pty.end_reason,
        }

    def _add_listener(self) -> queue.Queue:
        q = queue.Queue(maxsize=20000)
        with self._llock:
            self._listeners.append(q)
        return q

    def _remove_listener(self, q: queue.Queue):
        with self._llock:
            try:
                self._listeners.remove(q)
            except ValueError:
                pass


# 模块级单例：必须在 main() 调 start_rpc_server 之前就存在，
# 这样 HTTP RPC 的持久命名空间拿到的是本对象引用，会话建立后 attach 即生效。
ai_bridge = AIBridge()


# ==================== AI 桥调用方便捷函数（运行在外部调用方进程，只走本机 HTTP，不连 broker） ====================
#
# 与 AIBridge 本身（上面，运行在常驻窗口进程里）相反：下面这些函数给 AI /
# 其他进程导入调用，一次 localhost HTTP 即复用常驻窗口的全部 broker 长连接。
# 只用标准库 urllib，不依赖 requests。

DEFAULT_AI_BASE = "http://127.0.0.1:1188/"


def local_rpc(code, base=DEFAULT_AI_BASE, timeout=90):
    """向常驻 pty_client_mqtt 的本地 HTTP RPC 口下发一段 Python 代码，返回响应文本。"""
    import urllib.parse
    import urllib.request
    url = str(base).rstrip("/") + "/" + urllib.parse.quote(str(code), safe="")
    # data=b"" 强制 POST；显式绕开系统代理（localhost 不该走代理）。
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    req = urllib.request.Request(url, data=b"", method="POST")
    with opener.open(req, timeout=timeout) as resp:
        return resp.read().decode("utf-8", "replace")


def _call_ai_bridge(expr, base=DEFAULT_AI_BASE, timeout=90):
    """让窗口进程把表达式结果以 JSON 写回 body，本地解析成 dict。"""
    import json
    body = local_rpc(
        "import json as _j; p.set_data(_j.dumps(%s, ensure_ascii=False))" % expr,
        base=base, timeout=timeout)
    return json.loads(body)


def ai_pty_run(cmd, timeout=60, base=DEFAULT_AI_BASE, acquire_timeout=2):
    """让常驻 PTY 窗口代跑一条 shell 命令。

    返回 ``{'ok': bool, 'rc': int|None, 'out': str, 'timed_out': bool, ...}``；
    命令回显与输出同时在窗口里可见。命令串行执行，重叠调用立即返回
    ``{'ok': False, 'busy': True}``。
    """
    return _call_ai_bridge(
        "ai_bridge.run(%r, timeout=%r, acquire_timeout=%r)"
        % (str(cmd), float(timeout), float(acquire_timeout)),
        base=base, timeout=float(timeout) + 30)


def ai_pty_send(data, base=DEFAULT_AI_BASE, timeout=30):
    """向常驻 PTY 原样喂键（str，如 ``"y\\n"`` / ``"\\x03"`` Ctrl-C），不回收输出。"""
    return _call_ai_bridge("ai_bridge.send(%r)" % (str(data),),
                           base=base, timeout=timeout)


def ai_pty_status(base=DEFAULT_AI_BASE, timeout=15):
    """查询常驻 PTY 状态：会话是否建立/已结束、是否忙、broker 在线数。"""
    return _call_ai_bridge("ai_bridge.status()", base=base, timeout=timeout)


# ============================ PTY 会话 ============================

def _hard_exit(console, code: int, message: str, pty=None):
    """学 client_mqtt.py 的退出风格：直接 ``os._exit``，干脆不拖泥带水。

    旧路径退出时要 pty.close() 干等 end 帧 1.5s + join 输入线程 2s +
    transport.stop() 逐个 broker disconnect——服务器已死时这些等待全都
    纯属浪费，界面就是这样"卡住"的。``os._exit`` 会跳过所有 finally，
    所以这里必须自己先把终端模式恢复；stop 帧只 fire-and-forget（能发
    出去就让远端顺手收尸，发不出去也不等）。
    """
    if pty is not None:
        try:
            pty.detach()
        except Exception:
            pass
    try:
        console.exit()
    except Exception:
        pass
    if message:
        try:
            sys.stderr.write(stime()+ message)
            sys.stderr.flush()
        except Exception:
            pass
    try:
        sys.stdout.buffer.flush()
    except Exception:
        pass
    os._exit(code)


def _join_pre_command(parts) -> str:
    """把 REMAINDER 位置参数拼成一行前置命令：多段按空格连接（与 ssh 一致），
    去掉 argparse REMAINDER 可能保留的 ``--`` 分隔符；无命令返回空串。"""
    if not parts:
        return ""
    items = [str(x) for x in parts]
    if items and items[0] == "--":
        items = items[1:]
    return " ".join(items).strip()


def run_session(transport: MqttTransport, args, rows: int, cols: int) -> int:
    pty = RemotePty(transport, timeout=args.timeout)
    # 挂到 AI 桥：外部经本地 HTTP RPC 调用 ai_bridge 即复用本 PTY，
    # attach 放在 open 之前，保证最早的握手回显也能被 feed 分流到。
    ai_bridge.attach(pty, transport)
    outq: "queue.Queue[bytes]" = queue.Queue()
    stop_ev = threading.Event()
    term = args.term or os.environ.get("TERM") or "xterm-256color"

    # ---- 存活检测：输出帧和心跳帧都算"服务器还活着"的信号 ----
    heartbeat = max(0.0, float(args.heartbeat))
    dead_timeout = max(0.0, float(args.dead_timeout))
    if heartbeat <= 0:
        # 没有心跳时，shell 长时间静默和"服务器已死"无法区分，不做误杀
        dead_timeout = 0.0
    elif dead_timeout > 0:
        # 至少容忍连续 3 次心跳缺失，避免公共 broker 短暂重连被误判
        dead_timeout = max(dead_timeout, heartbeat * 3)
    signal_state = {"last": time.monotonic()}

    def on_data(chunk: bytes):
        signal_state["last"] = time.monotonic()
        outq.put(chunk)
        ai_bridge.feed(chunk)

    def on_heartbeat(_ts):
        signal_state["last"] = time.monotonic()

    # 协商：订阅 out topic + 下发 PTY 启动代码（shell 在服务端只启动这一次）
    _info("正在远端启动 PTY：shell=%s cwd=%s 窗口=%dx%d，等待握手回包（超时 %.0fs）..."
          % (args.shell or "服务端登录 shell", args.cwd or "远端 HOME",
             rows, cols, args.timeout))
    env = pty.open(rows, cols, shell=(args.shell or None), term=term,
                   cwd=args.cwd, login=not args.no_login,
                   flush_interval=args.interval, ttl=args.ttl,
                   heartbeat=heartbeat, on_heartbeat=on_heartbeat,
                   on_data=on_data)

    banner = (
        f"[{stime()}] connected shell={env['shell']} pid={env['pid']} "
        f"({env['rows']}x{env['cols']}, interval={env['flush_interval']})\t"
        f"{env['in_topic']}\t{env['out_topic']}\n"
        f"[pty] Ctrl-] 本地脱离；远端 exit/Ctrl-D 结束会话\n")
    if env.get("cwd_warning"):
        # 服务端对不存在的 cwd 已自行回退（HOME→/），会话照常用；只提示不退出
        banner += (f"[pty] 注意: {env['cwd_warning']}，"
                   f"已回退到 {env['cwd']}\n")
    responders = getattr(pty, "responders", None) or []
    if len(responders) > 1:
        # 多个持相同 key 的服务端同时应答了握手：每个都开了 PTY 往同一
        # topic 推流（界面重影/重复提示符的根源）。已按首个应答者定主，
        # 影子端会在收到 claim/首个按键帧后自杀；但根因要人工清理。
        winner = next((r for r in responders if r.get("winner")), responders[0])
        banner += (
            f"[pty][WARN] 检测到 {len(responders)} 个持相同 key 的服务端同时应答！"
            f"仅保留 {winner.get('host')} pid={winner.get('pid')}，"
            f"其余影子 PTY 已被通知立即关闭。\n")
        for r in responders:
            if not r.get("winner"):
                banner += (f"           影子: {r.get('host')} pid={r.get('pid')} "
                           f"owner={r.get('owner')}\n")
        banner += ("           请停掉多余机器/容器上的旧 server_mqtt 进程，"
                   "否则每次连接都会重复拉起并短暂干扰首屏。\n")
    if dead_timeout > 0:
        banner += (f"[pty] 心跳 {heartbeat:g}s：服务器关闭/断连后最多 "
                   f"{dead_timeout:g}s 自动退出\n")
    sys.stderr.write(banner)
    sys.stderr.flush()

    console = _make_raw_console()
    detach_key = args.detach_key.encode("latin-1", "ignore") or None

    def input_loop():
        try:
            console.enter()
            while not stop_ev.is_set():
                data = console.read()
                if not data:
                    break
                if detach_key and data == detach_key:
                    sys.stderr.write("\r\n[pty] 本地脱离\r\n")
                    break
                pty.send(data)
        except OSError:
            pass
        finally:
            stop_ev.set()

    def resize_watch():
        # last=None：连 broker / 握手期间窗口可能已被拖动过，线程一启动
        # 先无条件同步一次真实尺寸。
        # 尺寸变化立即发 winsz；停稳后的几个 tick 再补发同尺寸、更高 iseq
        # 的新帧兜底——多 broker 路径乱序时，配合服务端的 iseq 单调闸门，
        # 保证最终生效的一定是当前尺寸：拖动时经慢 broker 迟到的旧（更大）
        # 尺寸要么被服务端丢弃，要么被这里的补发覆盖，远端 PTY 不会被卡在
        # 比本地窗口宽的尺寸上（否则 pip/gradle 的 \r 进度条会全线错位）。
        last = None
        resend = 0
        while not stop_ev.wait(0.25):
            cur_sz = shutil.get_terminal_size((80, 24))
            cur = (cur_sz.lines, cur_sz.columns)
            if cur != last:
                try:
                    pty.resize(cur[0], cur[1])
                except Exception:
                    pass
                last = cur
                resend = 3
            elif resend > 0:
                try:
                    pty.resize(cur[0], cur[1])
                except Exception:
                    pass
                resend -= 1

    t_in = threading.Thread(target=input_loop, name="pty-input", daemon=True)
    t_resize = threading.Thread(target=resize_watch, name="pty-resize",
                                daemon=True)
    t_in.start()
    t_resize.start()

    pre_cmd = _join_pre_command(getattr(args, "command", None))
    if pre_cmd:
        # SSH 式前置命令（ssh host "tmux at"）：握手一完成就把整行敲进常驻
        # shell。tty 行规程会先把字节缓存在内核输入队列里等 shell 读取，
        # 无需 sleep 等提示符。加 \r 提交；命令结束后会话不关闭，人继续
        # 留在 shell / tmux 里。
        try:
            pty.send(pre_cmd + "\r")
            sys.stderr.write("[pty] 已自动执行前置命令: %s\n" % pre_cmd)
            sys.stderr.flush()
        except Exception as exc:
            sys.stderr.write("[pty][WARN] 前置命令发送失败: %s\n" % exc)
            sys.stderr.flush()

    # 主线程：远端输出原样渲染；任何退出路径都走 _hard_exit 立即收场
    try:
        while not stop_ev.is_set():
            try:
                chunk = outq.get(timeout=0.3)
            except queue.Empty:
                if pty.end_reason is not None:
                    _hard_exit(
                        console, 0,
                        f"\r\n[pty] session ended: {pty.end_reason}\r\n", pty)
                if dead_timeout > 0 and (time.monotonic()
                                         - signal_state["last"]) > dead_timeout:
                    _hard_exit(
                        console, 3,
                        f"\r\n[pty] {dead_timeout:g}s 收不到任何远端帧"
                        f"（输出/心跳），服务器可能已关闭，直接断开\r\n", pty)
                continue
            sys.stdout.buffer.write(chunk)
            sys.stdout.buffer.flush()
    except KeyboardInterrupt:
        _hard_exit(console, 130, "\r\n[pty] interrupted\r\n", pty)

    # input_loop 结束：Ctrl-] 本地脱离 / 本地 stdin 关闭
    _hard_exit(console, 0, "\r\n[pty] session ended: detached\r\n", pty)
    return 0  # 不可达：_hard_exit 不返回；仅为类型/静态检查保留


# ============================ CLI ============================

def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="pty_client_mqtt",
        description="PTY over MQTT：SSH 式远程交互终端（常驻 shell，逐键过 broker）")
    # 连接参数：选项名全部引用 client_mqtt 别名表，与 cmd_client_mqtt 共用同一份
    add_connection_args(p, default_timeout=30.0)
    # PTY 参数
    p.add_argument("--shell", default="",
                   help="远端 shell，默认服务端用户登录 shell（$SHELL/passwd）")
    p.add_argument("--term", default="",
                   help="TERM，默认本地 $TERM 或 xterm-256color")
    p.add_argument("--cwd", default=None, help="启动目录，默认远端 HOME")
    p.add_argument("--interval", "-i", type=float, default=0.0,
                   help="服务端主动推送最小间隔秒：0=实时（默认），>0 攒批")
    p.add_argument("--no-login", action="store_true",
                   help="不使用 login shell（默认 argv0 带 - 前缀）")
    p.add_argument("--ttl", type=float, default=DEFAULT_PTY_TTL,
                   help="孤儿会话最长存活秒（默认 12h，上限 24h）")
    p.add_argument("--heartbeat", type=float, default=5.0,
                   help="服务端心跳间隔秒：0=关闭（默认 5s）")
    p.add_argument("--dead-timeout", type=float, default=15.0,
                   help="多久收不到任何远端帧（输出/心跳）即判定服务器已死"
                        "并直接退出（默认 15s，实际不小于 3 倍心跳；"
                        "0=不检测，心跳关闭时自动失效）")
    p.add_argument("--size", default=None,
                   help="强制窗口 ROWSxCOLS，如 24x100；默认取本地终端大小")
    p.add_argument("--detach-key", default="\x1d",
                   help="本地强制脱离键（默认 Ctrl-]）")
                   
    p.add_argument("--port", "-port", "-p", type=int, default=1188,
                   help="本地 AI 控制口 HTTP RPC 端口（默认 1188）；"
                        "外部进程经它调用 ai_bridge 复用本 PTY，不再重连 broker；0=关闭")
    p.add_argument("--host", "-host", default="0.0.0.0",
                   help="本地 AI 控制口绑定地址（默认 0.0.0.0）；仅本机调用建议 --host 127.0.0.1")
    # SSH 式可选位置参数：连接成功后自动敲进常驻 shell 的前置命令，如
    # ``pty_client_mqtt.py -t q -k *** "tmux at"``；REMAINDER 保证命令自身
    # 的 -x 选项（tmux attach -d）不会被本客户端解析。命令结束后会话继续，
    # 人仍留在远端 shell / 全屏程序里。
    p.add_argument("command", nargs=argparse.REMAINDER,
                   help="可选：连接后自动执行的前置命令（SSH 式），如 \"tmux at\"")

    return p


def main(argv=None) -> int:
    args = build_parser().parse_args(argv)

    if args.size:
        try:
            r_txt, c_txt = args.size.lower().split("x", 1)
            rows, cols = int(r_txt), int(c_txt)
        except ValueError:
            sys.stderr.write("[ERROR] --size 格式应为 ROWSxCOLS，如 24x100\n")
            return 2
    else:
        sz = shutil.get_terminal_size((80, 24))
        rows, cols = sz.lines, sz.columns

    if not sys.stdin.isatty():
        sys.stderr.write("[ERROR] PTY 需要一个交互式本地终端（stdin 不是 TTY）\n")
        return 2
    
    if args.port:
        import server_http
        # 注意：持久命名空间在此刻快照一次，ai_bridge 是模块级对象引用，
        # 会话建立后 attach/feed 对 HTTP 调用方立即生效。
        ghs = server_http.start_rpc_server(
            port=args.port, ip=args.host, globals=globals(), locals=locals())
        _info(f"本地 AI 控制口已开启：http://127.0.0.1:{args.port}/"
              f"（示例：ai_bridge.run(\"uname -a\")；命令在本窗口实时可见）")
    
    _enable_output_vt()
    signed = bool(str(args.key or "").strip())
    t_conn = time.monotonic()
    _info("正在并发连接 %d 个公共 MQTT broker（多路径冗余，首个连上即继续，通常不足 1 秒）..."
          % len(BROKER_LIST))
    _info("request_topic=%s  reply_topic=%s  请求签名=%s  允许未验签回包=%s"
          % (args.request_topic, args.reply_topic,
             "是" if signed else "否", "是" if args.allow else "否"))
    transport = MqttTransport(
        request_topic=args.request_topic, reply_topic=args.reply_topic,
        private_key=args.key, allow_no_pub=args.allow)
    online, total, hosts = _broker_status(transport)
    _info("MQTT 就绪：在线 broker %d/%d，建连耗时 %.1f 秒"
          % (online, total, time.monotonic() - t_conn))
    if hosts:
        _info("在线节点：" + ", ".join(hosts))
    elif total:
        sys.stderr.write("[WARN] 当前没有任何 broker 在线，握手大概率超时\n")
        sys.stderr.flush()
    try:
        return run_session(transport, args, rows, cols)
    except RemoteError as exc:
        sys.stderr.write(f"[ERROR] {type(exc).__name__}: {exc}\n")
        return 2
    finally:
        transport.close()


if __name__ == "__main__":
    sys.exit(main())
