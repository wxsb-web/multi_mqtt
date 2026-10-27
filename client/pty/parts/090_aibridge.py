# ============================ AI 桥：外部进程复用本常驻 PTY ============================

# CSI / SGR / OSC / 单字符转义，用于把 AI 收回的终端字节还原成纯文本
_ANSI_RE = re.compile(rb"\x1b(?:\][^\x07\x1b]*(?:\x07|\x1b\\)|\[[0-?]*[ -/]*[@-~]|[@-Z\\-_])")


def _strip_ansi(b: bytes) -> str:
    return _ANSI_RE.sub(b"", b).replace(b"\r\n", b"\n").replace(b"\r", b"\n").decode("utf-8", "replace")


# ---- tmux 管道收流的运行参数 ----
# 远端 pipe 文件目录：tmux 的 pipe-pane 是远端 shell 执行的，路径必须是远端路径；
# 每次 run 用随机 tag 独立命名，run 结束即删（避免长会话把 /tmp 写满）。
_AI_PIPE_DIR = "/tmp"
# 单次增量读取上限（128 KiB，等于 RemoteShell 的 WIRE_BUDGET）：防止一条超大
# 输出把一次 MQTT 往返撑爆，轮询循环里会继续补齐剩余部分。取 128 KiB 而不是
# 更大值，是因为超过这个尺寸远端会改走暂存分片（每次 fetch 又是一次完整往返），
# 单次读取的实际耗时反而成倍增长。
_AI_PIPE_READ_MAX = 131072
# pipe 文件轮询间隔（秒）：0.12s 在"实时感"与"MQTT 往返开销"之间取平衡。
_AI_POLL = 0.12
# 人按 Ctrl-C 打断的识别与宽限期（秒）。
# 打断时整条 "echo BEGIN; sh -c ...; echo END" 前台作业链一并被中止，END 根本
# 不会输出；若死等到 timeout，AI 侧要白等几十秒才知道被中断。这里识别终端
# 对 Ctrl-C 的回显（行首 ^C），再给一小段宽限期收尾输出，仍未出现 END 就提前
# 返回并标记 interrupted=True。判据限定"行首"，避免匹配命令输出正文里的 ^C。
_AI_INTERRUPT_RE = re.compile(rb"(?:^|\r?\n)\^C\r?\n")
_AI_INTERRUPT_GRACE = 1.0


def _tmux_rows(data: bytes, n: int):
    """把 ``tmux -F 'a|b|c'`` 的输出切成字段列表（不足 n 列的脏行直接丢弃）。"""
    rows = []
    for line in data.decode("utf-8", "replace").splitlines():
        parts = line.split("|")
        if len(parts) >= n:
            rows.append(parts)
    return rows


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


def _build_ai_run_line_b64(cmd: str, begin_tag: str, end_tag: str) -> str:
    """构造 run() 要敲进远端交互 shell 的命令行（**不含尾部回车**）。

    见上方「AI 命令严格编码」块注释：``cmd`` 经 base64 透明传输，远端
    POSIX shell（sh/bash/ash/dash）解码后交给 ``sh -c``，单双引号、
    ``$``、反引号、分号、裸换行等任意字符都不会改变命令本体。前置条件
    （与旧实现相同）：外层登录 shell 为 POSIX 系，且远端至少有
    base64 / openssl / python 三者之一。

    尾部回车由投递方决定：走 PTY 时追加 ``\\r``，走 tmux 时另发一个
    ``Enter`` 键（见 :meth:`AIBridge._send_keys`），因为 tmux 的
    ``send-keys -l`` 把 ``\\r`` 当字面字节写进 pane，语义不如单发 Enter 清晰。
    """
    b64 = base64.b64encode(str(cmd).encode("utf-8", "replace")).decode("ascii")
    # b64 字符全部落在 shell 安全字符集（字母数字 + /+=），printf '%s'
    # 原样吐出即可，无需再包引号；模板里的 %%s 经 Python % 格式化后是字面 %s。
    return (
        'echo %s; sh -c "$(printf \'%%s\' %s | %s)"; __ai_rc=$?; '
        'echo %s:$__ai_rc'
        % (begin_tag, b64, _AI_B64_DECODE_PIPELINE, end_tag)
    )


def _build_ai_run_line_raw(cmd: str, begin_tag: str, end_tag: str) -> str:
    """构造「命令原文可见」模式的命令行（**不含尾部回车**）。

    与 b64 版的唯一区别：命令本体不进 base64，而是用 ``shlex.quote`` 做
    **一层** POSIX 单引号转义后直接交给 ``sh -c``。人切到该 tmux 窗口时
    看到的就是：

        echo __AI_BEGIN_xxxx__; sh -c '用户实际要跑的命令'; __ai_rc=$?; echo __AI_END_xxxx__:$__ai_rc

    为什么这里一层转义就够、不会重蹈旧实现「多层嵌套引号被吃」的覆辙：
    旧实现是 ``sh -c "$(printf %s B64 | base64 -d)"``，命令文本要穿过
    「外层交互 shell 的双引号 + 命令替换 + 内层 sh -c」多层；现在命令文本
    只穿过「目标 shell 的一层词法解析」，shlex.quote 产出的单引号片段在
    任何 POSIX shell（sh/bash/ash/dash）里都是严格无损的——内部的单引号
    由 ``'"'"'`` 展开，不会与外层配对出错。

    已知边界（因此 run() 会自动降级到 b64 模式）：
    - 命令含裸换行/控制字节：交互 shell 会当回车或 Ctrl-C 提交，无法作为
      一行完整输入；
    - 命令超过 pty 规范模式单行上限 MAX_CANON（Linux 通常 4096 字节）时，
      交互 shell 读走前写入端会阻塞，极端情况下被截断。
    """
    return (
        "echo %s; sh -c %s; __ai_rc=$?; echo %s:$__ai_rc"
        % (begin_tag, shlex.quote(str(cmd)), end_tag)
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
        # ---- AI 目标窗口（tmux）相关状态 ----
        # _pane_id 是**固化后**的默认目标 pane（tmux 的 pane_id，形如 %2）。
        # 为什么不存 "0:2" 这种 target：0 是 session 名、2 是 window_index，
        # 二者都会被改名/重排（renumber-windows 在窗口关闭后会重排 index、
        # base-index 可配成非 0、move-window/swap-window 会互换 index），
        # 只有 pane_id（%N）在整个 tmux server 生命周期内稳定不变。
        self._pane_id = None
        self._pane_source = ""     # auto（首次解析）/ cli（--ai-pane）/ rpc（pty_pane）
        # _tmux_ok 三态：None=尚未探测；True=可用；False=不可用（回落原生 PTY 投递）
        self._tmux_ok = None
        self._shell = None          # 延迟创建的 RemoteShell（只在 tmux 路径才需要）
        # AI 命令编码：raw=命令原文直接可见（默认）；base64=旧严格编码兜底
        self._encoding = "raw"

    # ---- 会话侧调用（run_session 内挂载） ----

    def attach(self, pty, transport):
        self.pty = pty
        self.transport = transport
        # 每次重新 attach 都要清掉上一个会话的 tmux 探测结果与固化窗口：
        # 新会话可能连的是另一台机器/另一个 tmux server，旧 pane_id 必然失效。
        self._shell = None
        self._tmux_ok = None
        self._pane_id = None
        self._pane_source = ""

    def configure(self, pane="", encoding="") -> None:
        """会话启动时注入 CLI 配置（--ai-pane / --ai-encoding）。

        pane 为空 = 保持 auto（首次 run 时解析「index 最小的窗口」并固化）；
        非空 = 用户显式指定，直接固化为 CLI 来源，不再自动解析。
        """
        if pane is not None and str(pane).strip():
            self._pane_id = str(pane).strip()
            self._pane_source = "cli"
        enc = str(encoding or "").strip().lower()
        if enc in ("raw", "base64"):
            self._encoding = enc

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
            # AI 目标窗口：pane 为 None 说明 tmux 不可用或尚未探测，
            # 此时 run() 会走原生 PTY 投递（等价改造前的行为）。
            "ai_pane": self._pane_id,
            "ai_pane_source": self._pane_source or "auto",
            "ai_encoding": self._encoding,
            "tmux_ok": self._tmux_ok,
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

    def run(self, cmd, timeout: float = 60.0, acquire_timeout: float = 2.0,
            pane=None, encoding=None) -> dict:
        """在常驻 shell / 指定 tmux 窗口里跑一条命令，返回 rc 与输出。

        两条投递通道，同一套随机 marker：

        1. **tmux 通道（tmux 可用时的默认）**：命令行经 ``send-keys`` 投递到
           固化的目标 pane，输出由 ``pipe-pane`` 落盘后按 offset 增量读。
           命令原文直接在窗口里可见、人切到该窗口能实时围观、能按键打断；
           且 pipe-pane 拿的是 pane 应用层字节，不含 tmux 客户端整屏重绘的
           回放，从根上消除了旧实现"输出里夹带历史与旧 marker"的污染。
        2. **原生 PTY 通道（tmux 不可用时的回落）**：老逻辑，命令行直接写入
           本 PTY 的 shell，输出从 feed 分流队列回收，行为与改造前一致。

        参数 ``pane``：目标窗口；``None``/空 = 用固化的默认窗口（连接后首次
        run 解析「index 最小的窗口」并固化），非空 = 本次临时指定，不改默认。
        参数 ``encoding``：``raw``（默认，命令原文可见）或 ``base64``（严格
        编码，任意特殊字符不破裂；含裸换行/控制字节时自动降级到它）。

        - 超时只停止回收，不杀远端命令（输出继续在窗口里刷，可用 send 干预）；
        - 退出码取自命令尾部 ``echo END:$?``（tmux 没有能取 pane 内子命令
          rc 的 API；``#{pane_dead_status}`` 只在 pane 主进程死亡时才有值）。
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
        try:
            target = self._pick_pane(pane)
            enc = self._pick_encoding(cmd, encoding)
            if target is None:
                return self._run_via_pty(str(cmd), float(timeout), begin, end, enc)
            return self._run_via_tmux(target, str(cmd), float(timeout),
                                      begin, end, enc, tag)
        finally:
            self._busy.release()

    # ---- 投递通道：原生 PTY（tmux 不可用时的回落，等价改造前行为） ----

    def _run_via_pty(self, cmd, timeout, begin, end, enc) -> dict:
        pty = self.pty
        q = self._add_listener()
        line = self._build_line(cmd, begin, end, enc)
        buf = b""
        timed_out = False
        m_end = None
        end_re = re.compile(re.escape(end.encode()) + rb":(\d+)")
        try:
            pty.send((line + "\r").encode("utf-8"))
            deadline = time.monotonic() + timeout
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
        return self._pack_result(buf, m_end, begin, end, timed_out,
                                 {"pane": None, "channel": "pty", "encoding": enc,
                                  "end_reason": self.pty.end_reason})

    # ---- 投递通道：tmux（send-keys 投递 + pipe-pane 增量收流） ----

    def _run_via_tmux(self, target, cmd, timeout, begin, end, enc, tag) -> dict:
        path = "%s/.pty_ai_%s.log" % (_AI_PIPE_DIR, tag)
        end_re = re.compile(re.escape(end.encode()) + rb":(\d+)")
        buf = b""
        off = 0
        timed_out = False
        interrupted = False
        interrupt_seen = False
        grace_deadline = None
        m_end = None
        # 顺序必须是「先挂管道，再敲命令」：pipe-pane 只捕获挂载之后该 pane
        # 产生的输出，反过来的话 BEGIN 附近的最早几行会丢。
        started = self._pipe_start(target, path)
        try:
            if not started:
                return {"ok": False, "pane": target, "channel": "tmux",
                        "error": "tmux pipe-pane 失败（窗口不存在或 tmux 不可用）"}
            if not self._send_keys(target, self._build_line(cmd, begin, end, enc)):
                return {"ok": False, "pane": target, "channel": "tmux",
                        "error": "tmux send-keys 失败（窗口不存在或 tmux 不可用）"}
            deadline = time.monotonic() + timeout
            while True:
                if self.pty.end_reason:
                    break
                remain = deadline - time.monotonic()
                if remain <= 0:
                    timed_out = True
                    break
                # 轮询而不是让远端长驻 tail -f：后者要额外管理生命周期与
                # 清理，而 0.12s 一次的 MQTT 往返开销远小于一次整帧传输。
                time.sleep(min(_AI_POLL, max(0.02, remain)))
                chunk, off = self._pipe_read(path, off)
                if chunk:
                    buf += chunk
                    m_end = end_re.search(buf)
                    if m_end:
                        break
                    # 尾部窗口比对：行首 ^C 可能跨两次读取，只看最后 128 字节。
                    if not interrupt_seen and _AI_INTERRUPT_RE.search(buf[-128:]):
                        interrupt_seen = True
                        grace_deadline = time.monotonic() + _AI_INTERRUPT_GRACE
                if grace_deadline is not None and time.monotonic() >= grace_deadline:
                    interrupted = True
                    break
            # 命中 END 时最后一段可能还在管道缓冲里，再补读一次。
            chunk, off = self._pipe_read(path, off)
            buf += chunk
            m_end = m_end or end_re.search(buf)
        finally:
            if started:
                self._pipe_stop(target)
            self._run_shell("rm -f " + shlex.quote(path))
        return self._pack_result(buf, m_end, begin, end, timed_out,
                                 {"pane": target, "channel": "tmux",
                                  "encoding": enc,
                                  "interrupted": interrupted,
                                  "end_reason": self.pty.end_reason})

    # ---- 目标窗口解析 ----

    def _pick_pane(self, pane):
        """决定本次命令投递到哪个 pane；返回 None 表示走原生 PTY 通道。

        优先级：显式参数 > 固化值（--ai-pane / pty_pane / 首次 auto 解析）>
        现场解析。只有现场解析失败（tmux 不可用/没有 session）才回落。
        """
        explicit = str(pane or "").strip()
        if explicit and explicit.lower() != "auto":
            return explicit
        if self._pane_id:
            return self._pane_id
        return self._resolve_pane()

    def _resolve_pane(self):
        """解析默认窗口：**session 内 window_index 最小的窗口的 active pane**。

        为什么不能写死 "0:0"：
        - ``base-index`` 可配成 1 或任意值，0 号窗口可能根本不存在；
        - ``renumber-windows`` 打开时，窗口关闭后剩余窗口的 index 会被重排，
          "0:0" 指向的 pane 会漂移；
        - ``move-window`` / ``swap-window`` 也会改 index。
        所以每次都按**当前实际列表**取最小 index，并把结果的 pane_id（%N）
        固化下来——pane_id 在整个 tmux server 生命周期内不会变。

        会话选择：优先 attached>0 且 ``session_last_attached`` 最大的会话。
        实测同一台机器有 8 个 client attach 到同一 session，``select-window``
        只改某个 client 的视图，与"人现在看哪个"不是一回事，而这个字段稳定。
        """
        r = self._tmux(["list-sessions", "-F",
                        "#{session_name}|#{session_attached}|#{session_last_attached}"])
        if r is None or r.rc != 0:
            self._tmux_ok = False
            return None
        self._tmux_ok = True
        rows = _tmux_rows(r.stdout, 3)
        if not rows:
            self._tmux_ok = False
            return None
        try:
            session = max(rows, key=lambda x: (int(x[1] or 0), int(x[2] or 0)))[0]
        except ValueError:
            session = rows[0][0]
        # -s 才是"整个 session 的所有 pane"；不带 -s 只列 target window 的 pane。
        r = self._tmux(["list-panes", "-s", "-t", session, "-F",
                        "#{window_index}|#{pane_index}|#{pane_id}|#{pane_active}"])
        if r is None or r.rc != 0:
            self._tmux_ok = False
            return None
        rows = _tmux_rows(r.stdout, 4)
        if not rows:
            self._tmux_ok = False
            return None

        def _key(x):
            # 主键 window_index 数值最小；同窗口内 active pane 优先，再按 pane_index。
            try:
                return (int(x[0]), -int(x[3] or 0), int(x[1]))
            except ValueError:
                return (1 << 30, 0, 1 << 30)

        rows.sort(key=_key)
        self._pane_id = rows[0][2]
        self._pane_source = self._pane_source or "auto"
        return self._pane_id

    def _pick_encoding(self, cmd, encoding) -> str:
        enc = str(encoding or self._encoding or "raw").strip().lower()
        if enc not in ("raw", "base64"):
            enc = "raw"
        if enc == "raw" and any(ord(ch) < 32 for ch in str(cmd)):
            # 裸换行/控制字节在交互 shell 里会被当成回车或 Ctrl-C，raw 模式
            # 必然把一条命令拆成多行提交；这类命令自动降级到 base64。
            return "base64"
        return enc

    @staticmethod
    def _build_line(cmd, begin, end, enc) -> str:
        if enc == "base64":
            return _build_ai_run_line_b64(cmd, begin, end)
        return _build_ai_run_line_raw(cmd, begin, end)

    @staticmethod
    def _pack_result(buf, m_end, begin, end, timed_out, extra) -> dict:
        # 真起点是「独占一行的 BEGIN + 换行」；输入回显里 BEGIN 后面是 ';'，
        # 不会误中。结尾 END:<数字> 同理（回显里是 :$__ai_rc 字面量）。
        m_begin = re.search(re.escape(begin.encode()) + rb"\r?\n", buf)
        start = m_begin.end() if m_begin else 0
        if m_end:
            body, rc = buf[start:m_end.start()], int(m_end.group(1))
        else:
            body, rc = buf[start:], None
        res = {
            "ok": rc == 0 if rc is not None else False,
            "rc": rc,
            "timed_out": timed_out,
            "out": _strip_ansi(body).strip("\n"),
            "raw_len": len(body),
        }
        res.update(extra or {})
        return res

    # ---- 远端 tmux 辅助（全部走 RemoteShell 通道，不占本 PTY） ----

    def _shell_obj(self):
        if self._shell is None and self.transport is not None:
            self._shell = RemoteShell(self.transport, timeout=30)
        return self._shell

    def _run_shell(self, cmd):
        """在另一条远端命令通道执行普通 shell 命令，返回 CmdResult。

        这条通道不占用本 PTY，所以不会干扰人在窗口里的输入；任何异常都吞掉
        返回 None，让调用方走回落/报错分支。
        """
        sh = self._shell_obj()
        if sh is None:
            return None
        try:
            return sh.run(cmd)
        except Exception:
            return None

    def _tmux(self, args):
        """执行一条 tmux 子命令。

        参数逐项 shlex.quote 后拼进远端 ``sh -c``：这里只有**一层** POSIX
        单引号解析（远端 sh 剥掉引号后把原样字符串作为 argv 交给 tmux），
        不存在旧实现"外层交互 shell + 命令替换 + 内层 sh -c"的多层嵌套，
        因此任意特殊字符都不会被吃掉。
        """
        return self._run_shell("tmux " + " ".join(
            shlex.quote(str(a)) for a in args))

    def _pipe_start(self, pane, path) -> bool:
        """把该 pane 的输出字节流 append 到 path（tmux 原生能力）。

        与 capture-pane 的本质区别：capture-pane 只是把"当前屏幕"拍成快照
        （受 history-limit 限制、丢失中间过程）；pipe-pane 是 tmux 在 pane
        的 pty 输出处挂一根管道，拿到的是**完整、连续的字节流**。另一个关键
        好处是它不含 tmux 客户端整屏重绘的回放字节——旧实现 run() 输出里
        夹带大量历史与旧 marker，根因正是重绘混进了 PTY 流。

        不能用 ``-o``（"仅在没有 pipe 时打开"）：每次 run 都换新文件，带 -o
        会复用上一条管道继续写旧文件；不带 -o 时 tmux 先关旧管道再开新的，
        正好是这里要的语义。
        """
        r = self._tmux(["pipe-pane", "-t", pane, "cat >> " + shlex.quote(path)])
        return r is not None and r.rc == 0

    def _pipe_stop(self, pane) -> None:
        """关闭该 pane 的管道（命令留空 = 关闭）。失败忽略：下次 run 会重开。"""
        self._tmux(["pipe-pane", "-t", pane])

    def _pipe_read(self, path, off):
        """从 path 的 off 偏移增量读取，返回 (新字节, 新偏移)。

        ``tail -c +N`` 是 1 基偏移（+0 等于从头），所以传 off+1。单次用
        ``head -c`` 截断，避免一条超大输出把一次 MQTT 往返撑爆；调用方在
        轮询里继续补齐剩余部分。
        """
        cmd = ("tail -c +%d %s 2>/dev/null | head -c %d"
               % (off + 1, shlex.quote(path), _AI_PIPE_READ_MAX))
        r = self._run_shell(cmd)
        data = r.stdout if r is not None else b""
        return data, off + len(data)

    def _send_keys(self, pane, line) -> bool:
        """把命令行投递到指定 pane。

        ``-l`` 必须加：不加时 tmux 会把参数里的 ``Enter``/``C-c`` 之类当**键名**
        解析，命令文本里恰好含有这些单词就会被误解释成按键；``-l`` 禁用键名
        解析，传入什么字符就敲什么字符。``--`` 防止命令行以 ``-`` 开头时被
        tmux 当成选项。

        回车单独发一个 ``Enter`` 键，而不是在文本尾部拼 ``\\r``：pane 里的
        shell 处于规范模式/readline，单独按键的语义最接近真人敲回车。

        注意 ``-l`` 只保证 tmux 层不做键名解析，字符进入 pane 的 pty 后仍要
        被目标 shell 做一次词法解析——这正是命令本体必须用 ``sh -c`` + 单引号
        包裹（或 base64）的原因，与"能否看到原文"无关。
        """
        r = self._tmux(["send-keys", "-t", pane, "-l", "--", line])
        if r is None or r.rc != 0:
            return False
        r = self._tmux(["send-keys", "-t", pane, "Enter"])
        return r is not None and r.rc == 0

    # ---- RPC / MCP 动态调整默认窗口 ----

    def get_pane(self) -> dict:
        return {
            "ok": True,
            "pane": self._pane_id,
            "pane_source": self._pane_source or "auto",
            "tmux_ok": self._tmux_ok,
            "encoding": self._encoding,
        }

    def set_pane(self, target="") -> dict:
        """动态调整 AI 默认窗口。

        ``""``/``auto`` = 清空固化值并立即按"index 最小"重新解析；
        其他值 = 固化用户给的 target（``%N`` 最稳，也接受 ``0:2`` / 窗口名）。
        """
        t = str(target or "").strip()
        if not t or t.lower() == "auto":
            self._pane_id = None
            self._pane_source = "auto"
            p = self._resolve_pane()
            res = self.get_pane()
            res["ok"] = p is not None
            if p is None:
                res["error"] = "tmux 不可用或没有可解析的窗口"
            return res
        self._pane_id = t
        self._pane_source = "rpc"
        return self.get_pane()

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


