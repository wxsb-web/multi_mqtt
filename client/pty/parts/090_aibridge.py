# ============================ AI 桥：外部进程复用本常驻 PTY ============================

# CSI / SGR / OSC / 单字符转义，用于把 AI 收回的终端字节还原成纯文本
_ANSI_RE = re.compile(rb"\x1b(?:\][^\x07\x1b]*(?:\x07|\x1b\\)|\[[0-?]*[ -/]*[@-~]|[@-Z\\-_])")


def _strip_ansi(b: bytes) -> str:
    return _ANSI_RE.sub(b"", b).replace(b"\r\n", b"\n").replace(b"\r", b"\n").decode("utf-8", "replace")


# ==================== AI 命令严格编码（base64 透明传输，任意特殊字符不破裂） ====================
#
# 背景（实证根因）：AIBridge.run() 是把一整行文本「敲」进远端**交互式
# 登录 shell** 的 PTY。旧实现用 ``sh -c '<cmd>'``（shlex.quote，外层单
# 引号），命令自身的单引号靠 ``'"'"'`` 拼接——在 bash 下通常无碍，但在
# BusyBox ash 或 PTY→tmux→ssh 多层嵌套等环境里，交互行编辑/多层引号
# 只要吃掉一层配对，内层命令即被截断，典型现象：
#   printf '<内容>' >> file  →  printf '' >> file（文件被创建，0 字节）。
# 而「外层单引号/双引号 + 手工转义」这类方案都必须逐字符对抗**两层 shell
# 解析 + TTY 行规程**：``$``、`` ` ``、``"``、``\``、``!``（交互 bash
# 历史展开）、裸换行与控制字节（ICANON 下直接当回车/Ctrl-C），各有各的
# 坑，无法对任意字节严格无损。
#
# 方案（成熟库严格编码）：用标准库 base64 把整条命令编码为纯 ASCII 安全
# 字符集（A-Za-z0-9+/=，不含任何 shell/TTY 元字符，也不产生裸换行），
# 远端解码后作为**单个 argv** 交给 sh -c；命令原始字节完全不经过外层交互壳
# 的引号解析，从根本上消除嵌套破裂面。外层即「双引号包裹」：
#   sh -c "$(printf '%s' <B64> | <解码管线>)"
# 双引号内命令替换的结果不再做单词拆分/glob，也不会重新解析替换文本里的
# 引号/$/反引号，所以 sh 拿到的命令字符串与编码前逐字节一致（仅去掉尾随
# 换行——对 shell 语义无影响）。解码管线全是固定字面量，内层只用单引号，
# 与外层双引号不同型、互不嵌套；解码器代码里的括号也在单引号保护内，不会
# 提前闭合 $( )。
#
# 解码器三级回退：base64（coreutils 与 BusyBox 默认都带该 applet）→
# openssl（enc -d -base64 -A，单行）→ python3/python（远端本来就运行着
# Python，最终兜底）。已在 bash/dash/ash 语法与 base64/openssl/python
# 三条解码分支上实测通过。

# 注意：本程序文本必须**不含单引号**（它被单引号裹在 python -c '...' 里），
# 也不含双引号/$/反引号（外层是 sh -c "..."）；只用字母数字与 . , ; ( )。
_AI_B64_DECODER_PY = (
    "import sys,base64;"
    "sys.stdout.buffer.write(base64.b64decode(sys.stdin.buffer.read()))"
)
_AI_B64_DECODE_PIPELINE = (
    "{ base64 -d 2>/dev/null"
    " || openssl enc -d -base64 -A 2>/dev/null"
    " || python3 -c '%s' 2>/dev/null"
    " || python -c '%s' 2>/dev/null; }"
    % (_AI_B64_DECODER_PY, _AI_B64_DECODER_PY)
)


def _build_ai_run_line(cmd: str, begin_tag: str, end_tag: str) -> str:
    """构造 run() 要敲进远端交互 PTY 的命令行（以 ``\\r`` 结尾）。

    见上方「AI 命令严格编码」块注释：``cmd`` 经 base64 透明传输，远端
    POSIX shell（sh/bash/ash/dash）解码后交给 ``sh -c``，单双引号、
    ``$``、反引号、分号、裸换行等任意字符都不会改变命令本体。前置条件
    （与旧实现相同）：外层登录 shell 为 POSIX 系，且远端至少有
    base64 / openssl / python 三者之一。
    """
    b64 = base64.b64encode(str(cmd).encode("utf-8", "replace")).decode("ascii")
    # b64 字符全部落在 shell 安全字符集（字母数字 + /+=），printf '%s'
    # 原样吐出即可，无需再包引号；模板里的 %%s 经 Python % 格式化后是字面 %s。
    return (
        'echo %s; sh -c "$(printf \'%%s\' %s | %s)"; __ai_rc=$?; '
        'echo %s:$__ai_rc\r'
        % (begin_tag, b64, _AI_B64_DECODE_PIPELINE, end_tag)
    )


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
            # 终端写线程卡滞秒数（窗口冻结的直接信号）；>2 建议 redraw
            "writer_stall": round(_OUT_WRITER.stall_seconds(), 2)
            if _OUT_WRITER is not None else 0.0,
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
        - 退出码取自 ``sh -c "<cmd>"``；命令经 base64 严格编码后透传（见
          :func:`_build_ai_run_line`），单双引号/``$``/反引号/裸换行等任意
          特殊字符都不会让外层交互 shell 的引号嵌套破裂。前置条件：外层登录
          shell 为 POSIX 系（sh/bash/ash/dash），远端有 base64/openssl/python
          任一解码器。
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
        line = _build_ai_run_line(str(cmd), begin, end)
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


