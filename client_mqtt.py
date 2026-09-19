#!/usr/bin/env python3
import argparse
import time, threading, os, sys
import logging
import codeop
import importlib
import builtins as _builtins
from multi_mqtt import (
    MultiMQTTManager,
    get_req_id,
    utc_ms,
    get_standard_pem_bytes,
)

logger = logging.getLogger("Client")


# REQUEST_TOPIC = "sys/device/request"
# REPLY_TOPIC = "sys/device/response"
from server_mqtt import REQUEST_TOPIC, DEFAULT_REPLY_TOPIC as REPLY_TOPIC

DEFAULT_TIMEOUT = 30

_default_client = None
_default_client_lock = threading.Lock()


class MQTTClientNode:
    def __init__(
        self,
        client_private_key_bytes=None,
        allow_no_server_pubkey_response: bool = False,
    ):
        self.client_private_key_bytes = client_private_key_bytes
        self.allow_no_server_pubkey_response = allow_no_server_pubkey_response

        # 客户端无需配置 server_public_key_bytes
        self.mqtt_net = MultiMQTTManager(
            log_messages=False,
            client_private_key_bytes=client_private_key_bytes,
        )
        self.mqtt_net.set_on_message(self._on_message)
        self.pending_requests = {}
        self.lock = threading.Lock()
        self._subscribed_topics = set()   # 记录已订阅主题，避免重复订阅

    def start(self):
        self.mqtt_net.start()
        time.sleep(2)
        self._subscribe_once(REPLY_TOPIC)

    def _subscribe_once(self, topic):
        with self.lock:
            if topic in self._subscribed_topics:
                return
            self._subscribed_topics.add(topic)
        try:
            self.mqtt_net.subscribe(topic)
        except Exception:
            with self.lock:
                self._subscribed_topics.discard(topic)
            raise

    def _on_message(self, topic, data, rx_broker):
        received_req_id = data.get("req_id")
        if not received_req_id:
            return

        # 判断服务端是否原样退回了附带签名的 req_id (去时有，来时无)
        is_unverified_echo = isinstance(received_req_id, str) and "|" in received_req_id
        base_req_id = received_req_id.split("|", 1)[0] if is_unverified_echo else received_req_id

        with self.lock:
            # 客户端本地缓存的总是 base_req_id
            req_ctx = self.pending_requests.get(base_req_id)

        if req_ctx is None:
            return

        has_client_pri = bool(req_ctx.get("client_private_key_bytes"))
        allow_no_pub = req_ctx.get("allow_no_server_pubkey_response", False)

        # 拦截逻辑：
        # 如果客户端带有私钥发送 (has_client_pri)
        # 且服务端原样返回了带签名的 req_id (说明服务端没有公钥，未进行验签剥离)
        # 且配置不允许放行此类响应
        if has_client_pri and is_unverified_echo and not allow_no_pub:
            logger.warning(
                f"⛔ [安全拦截] req_id={received_req_id} | 服务端未验签 "
                f"(原样返回了带签名的 req_id)，且 allow_no_server_pubkey_response=False，丢弃该响应。",
            )
            return

        # 用副本清理 req_id，避免直接修改调用方传入的 data
        resp = dict(data)
        if is_unverified_echo:
            resp["req_id"] = base_req_id

        cost_ms = (time.perf_counter() - req_ctx['start_time']) * 1000
        resp["latency_ms"] = round(cost_ms, 2)
        resp["client_from"] = rx_broker

        req_ctx['response'] = resp
        req_ctx['event'].set()

    def request(
        self,
        payload: str,
        request_topic: str = REQUEST_TOPIC,
        timeout: float = DEFAULT_TIMEOUT,
        client_private_key_bytes=None,
        allow_no_server_pubkey_response: bool = None,
        reply_topic: str = REPLY_TOPIC,
    ):
        if client_private_key_bytes is None:
            client_private_key_bytes = (
                self.client_private_key_bytes
                or getattr(self.mqtt_net, "client_private_key_bytes", None)
            )

        if allow_no_server_pubkey_response is None:
            allow_no_server_pubkey_response = self.allow_no_server_pubkey_response

        # 生成基础的 req_id（不带签名）
        ms = utc_ms()
        req_id = get_req_id(ms)
        start_time = time.perf_counter()

        req_data = {
            "req_id": req_id,
            "reply_topic": reply_topic,
            "code": payload,
            "timestamp": ms,
        }

        event = threading.Event()
        req_ctx = {
            "event": event,
            "start_time": start_time,
            "response": None,
            "client_private_key_bytes": client_private_key_bytes,
            "allow_no_server_pubkey_response": allow_no_server_pubkey_response,
        }

        with self.lock:
            self.pending_requests[req_id] = req_ctx

        # 使用了非默认 reply_topic，确保网络层已订阅（去重）
        if reply_topic != REPLY_TOPIC:
            try:
                self._subscribe_once(reply_topic)
            except Exception as exc:
                logger.error(f"❌ [订阅失败] reply_topic={reply_topic} error={exc}")
                print(f"[ERROR] 订阅 reply_topic 失败: {exc}")
                with self.lock:
                    self.pending_requests.pop(req_id, None)
                return None

        try:
            # MultiMQTTManager 发送时会自动在网络层加上 `|签名`
            self.mqtt_net.publish_broadcast(
                request_topic, req_data,
                client_private_key_bytes=client_private_key_bytes,
            )
        except Exception as exc:
            with self.lock:
                self.pending_requests.pop(req_id, None)
            logger.error(f"❌ [请求发送失败] req_id={req_id} error={exc}")
            print(f"[ERROR] 请求发送失败: {exc}")
            return None

        is_success = False
        try:
            # 将长时间的一步阻塞拆分为小步轮询，避免子线程阻塞过深无法响应退出信号
            start_t = time.perf_counter()
            while time.perf_counter() - start_t < timeout:
                if event.wait(timeout=0.2):
                    is_success = True
                    break
        except KeyboardInterrupt:
            logger.warning(f"⚠️ [请求中断] req_id={req_id}")
            print("[INFO] 用户中断等待，已停止本次请求。")
            raise
        finally:
            # 解决内存泄漏：请求结束后（成功、超时或中断）清理 pending 记录
            with self.lock:
                self.pending_requests.pop(req_id, None)

        if is_success:
            return req_ctx['response']
        else:
            logger.error(f"❌ [请求超时] req_id={req_id}")
            return None

    def stop(self):
        try:
            self.mqtt_net.stop()
        except KeyboardInterrupt:
            print("[INFO] 用户中断，MQTT 连接已停止。")
            logger.warning("⚠️ [用户中断] 已停止 MQTT 连接")
        finally:
            with self.lock:
                self._subscribed_topics.clear()


def rpc(
    code: str,
    request_topic: str = REQUEST_TOPIC,
    timeout: float = DEFAULT_TIMEOUT,
    client_private_key_bytes=None,
    allow_no_server_pubkey_response: bool = False,
    reply_topic: str = REPLY_TOPIC,
):
    """Execute code through a lazily started shared MQTT client."""
    global _default_client
    with _default_client_lock:
        if _default_client is None:
            _default_client = MQTTClientNode(
                client_private_key_bytes=client_private_key_bytes,
                allow_no_server_pubkey_response=allow_no_server_pubkey_response,
            )
            _default_client.start()
        client = _default_client

    return client.request(
        code,
        request_topic=request_topic,
        timeout=timeout,
        client_private_key_bytes=client_private_key_bytes,
        allow_no_server_pubkey_response=allow_no_server_pubkey_response,
        reply_topic=reply_topic,
    )


def stop():
    """Stop the shared module-level MQTT client, if it was started."""
    global _default_client
    with _default_client_lock:
        client = _default_client
        _default_client = None
    if client is not None:
        client.stop()


# ---------------------------------------------------------------- #
#  Shell: 统一 prompt + 彩色 print + 魔术命令 + 磁盘历史             #
# ---------------------------------------------------------------- #

# prompt_toolkit 样式名 -> ANSI 转义（无 PT 时回退用）
_PT_STYLE_TO_ANSI = {
    "ansiblack":         "\033[30m",
    "ansired":           "\033[31m",
    "ansigreen":         "\033[32m",
    "ansiyellow":        "\033[33m",
    "ansiblue":          "\033[34m",
    "ansimagenta":       "\033[35m",
    "ansicyan":          "\033[36m",
    "ansiwhite":         "\033[37m",
    "ansibrightblack":   "\033[90m",
    "ansigray":          "\033[90m",
    "ansigrey":          "\033[90m",
    "ansibrightred":     "\033[91m",
    "ansibrightgreen":   "\033[92m",
    "ansibrightyellow":  "\033[93m",
    "ansibrightblue":    "\033[94m",
    "ansibrightmagenta": "\033[95m",
    "ansibrightcyan":    "\033[96m",
    "ansibrightwhite":   "\033[97m",
    "bold":              "\033[1m",
    "dim":               "\033[2m",
    "italic":            "\033[3m",
    "underline":         "\033[4m",
}


class C:
    RESET     = "\033[0m"
    BOLD      = "\033[1m"
    DIM       = "\033[2m"
    ITALIC    = "\033[3m"
    UNDERLINE = "\033[4m"
    RED       = "\033[31m"
    GREEN     = "\033[32m"
    YELLOW    = "\033[33m"
    BLUE      = "\033[34m"
    MAGENTA   = "\033[35m"
    CYAN      = "\033[36m"
    WHITE     = "\033[37m"
    GRAY      = "\033[90m"
    BRIGHT_RED     = "\033[91m"
    BRIGHT_GREEN   = "\033[92m"
    BRIGHT_YELLOW  = "\033[93m"
    BRIGHT_BLUE    = "\033[94m"
    BRIGHT_MAGENTA = "\033[95m"
    BRIGHT_CYAN    = "\033[96m"


def _get_clipboard_text() -> str:
    """尽量通用地从剪贴板取文本，用于处理 Windows 老终端的粘贴按键。"""
    # 1) pyperclip 最省事
    try:
        import pyperclip
        return pyperclip.paste() or ""
    except Exception:
        pass
    # 2) Windows 原生 API
    if sys.platform == "win32":
        try:
            import win32clipboard
            win32clipboard.OpenClipboard()
            try:
                return win32clipboard.GetClipboardData(
                    win32clipboard.CF_UNICODETEXT
                ) or ""
            finally:
                win32clipboard.CloseClipboard()
        except Exception:
            pass
    # 3) tkinter 兜底
    try:
        import tkinter
        r = tkinter.Tk()
        r.withdraw()
        try:
            return r.clipboard_get() or ""
        finally:
            r.destroy()
    except Exception:
        return ""


def _normalize_history_path(path):
    """展开 ~ 并尽力创建父目录；返回规范化后的路径或 None。"""
    if not path:
        return None
    path = os.path.expanduser(str(path))
    try:
        parent = os.path.dirname(os.path.abspath(path))
        if parent and not os.path.isdir(parent):
            os.makedirs(parent, exist_ok=True)
    except OSError:
        pass
    return path


def _build_prompt(history_path=None):
    """返回 (prompt_callable, has_prompt_toolkit_bool, hist_ctl)。

    hist_ctl 是个字典：
      - get() -> 当前历史文件路径或 None
      - set(path) -> 切换历史文件（None 表示禁用磁盘历史）；返回规范化后的路径
    有 prompt_toolkit 时使用 FileHistory 做磁盘持久化；无则仅记录路径。
    """
    try:
        prompt_toolkit = importlib.import_module("prompt_toolkit")
        pt_key_binding = importlib.import_module("prompt_toolkit.key_binding")
        pt_lexers = importlib.import_module("prompt_toolkit.lexers")
        pt_styles = importlib.import_module("prompt_toolkit.styles")
        pt_keys = importlib.import_module("prompt_toolkit.keys")
        pt_history = importlib.import_module("prompt_toolkit.history")
        pygments_lexers = importlib.import_module("pygments.lexers")
    except ImportError:
        # 无 prompt_toolkit：仅记录路径，不做持久化
        _state = {"path": _normalize_history_path(history_path)}

        def _get():
            return _state["path"]

        def _set(p):
            _state["path"] = _normalize_history_path(p)
            return _state["path"]

        return _fallback_code_input, False, {"get": _get, "set": _set}

    key_bindings = pt_key_binding.KeyBindings()

    @key_bindings.add("enter")
    def accept_on_empty_line(event):
        buffer = event.current_buffer
        if buffer.document.current_line_before_cursor.strip():
            buffer.insert_text("\n")
        else:
            # 去掉末尾换行，避免历史记录里多出一个空行
            cleaned = buffer.text.rstrip("\n")
            if cleaned != buffer.text:
                buffer.text = cleaned
            buffer.validate_and_handle()

    # 现代终端的标准 bracket paste
    @key_bindings.add(pt_keys.Keys.BracketedPaste)
    def _on_bracketed_paste(event):
        data = event.data.replace("\r\n", "\n").replace("\r", "\n")
        event.current_buffer.insert_text(data)

    # Windows 老终端把粘贴翻译成 Shift+Insert 的 xterm 序列 \x1b[2;2~
    # 不能写 @key_bindings.add("\x1b[2;2~")：prompt_toolkit 的按键解析器
    # 只接受单字符或已知按键名，否则注册时抛 ValueError: Invalid key。
    # 拆成独立参数会被编译成一个按键序列绑定。
    @key_bindings.add("escape", "[", "2", ";", "2", "~")
    def _on_shift_insert_paste(event):
        text = _get_clipboard_text()
        if text:
            text = text.replace("\r\n", "\n").replace("\r", "\n")
            event.current_buffer.insert_text(text)

    session_state = {"session": None, "path": None}

    def _rebuild(path):
        norm = _normalize_history_path(path)
        hist = pt_history.FileHistory(norm) if norm else pt_history.InMemoryHistory()
        session = prompt_toolkit.PromptSession(
            lexer=pt_lexers.PygmentsLexer(pygments_lexers.PythonLexer),
            style=pt_styles.Style.from_dict({"prompt": "ansicyan"}),
            multiline=True,
            key_bindings=key_bindings,
            history=hist,
        )
        session_state["session"] = session
        session_state["path"] = norm
        return norm

    _rebuild(history_path)

    def prompt_with_pt():
        return session_state["session"].prompt(">>> ")

    def _get():
        return session_state["path"]

    def _set(p):
        return _rebuild(p)

    return prompt_with_pt, True, {"get": _get, "set": _set}


def _make_print(has_pt: bool):
    """构造一个兼容内置 print 签名、增加 color 参数的 print。

    color 支持：
      - ""         : 无色
      - ANSI 转义  : "\\033[31m" / "\\033[38;5;208m" / "\\033[38;2;r;g;bm"
      - PT 样式名  : "ansired" / "ansigray" / "bold"（无 PT 时自动映射）
    """
    if has_pt:
        from prompt_toolkit import print_formatted_text as _pt_print
        from prompt_toolkit.formatted_text import (
            FormattedText as _pt_ft,
            ANSI as _pt_ansi,
        )
    else:
        _pt_print = _pt_ft = _pt_ansi = None

    def _print(*args, sep=' ', end='\n', file=None, flush=False, color=''):
        text = sep.join(str(a) for a in args)
        if file is None:
            file = sys.stdout

        if not color:
            if has_pt:
                _pt_print(text, end=end, file=file, flush=flush)
            else:
                _builtins.print(text, end=end, file=file, flush=flush)
            return

        if color.startswith("\033["):
            colored = f"{color}{text}\033[0m"
            if has_pt:
                _pt_print(_pt_ansi(colored), end=end, file=file, flush=flush)
            else:
                _builtins.print(colored, end=end, file=file, flush=flush)
            return

        # prompt_toolkit 样式名
        if has_pt:
            _pt_print(_pt_ft([(color, text)]), end=end, file=file, flush=flush)
        else:
            ansi = _PT_STYLE_TO_ANSI.get(color, "")
            colored = f"{ansi}{text}\033[0m" if ansi else text
            _builtins.print(colored, end=end, file=file, flush=flush)

    return _print


# ---------------- 魔术命令处理 ----------------

_MAGIC_HELP = """\
可用魔术命令：
  %topic [name]      查看/设置 request_topic；%topic -reset 恢复默认
  %reply [name]      查看/设置 reply_topic；%reply -reset 恢复默认
  %key   [path|pem]  加载客户端私钥；%key -clear 清除；不带参查看状态
  %allow [on|off]    查看/设置 allow_no_server_pubkey_response
  %his [path]        查看/切换磁盘历史文件；%his -clear 关闭磁盘历史
  %history [path]    %his 的别名，功能相同
  %status            打印当前会话状态
  %help, %?          显示本帮助
  %exit, %quit       退出

Python 代码直接输入即可；空行提交。
"""


def _handle_magic(line, state, print_fn):
    """处理 `%xxx arg` 形式的魔术命令。返回 True 表示已处理（不再走 RPC）。"""
    body = line[1:].strip()
    if not body:
        print_fn("空魔术命令，输入 %help 查看帮助", color=C.YELLOW)
        return True

    parts = body.split(None, 1)
    cmd = parts[0].lower()
    arg = parts[1].strip() if len(parts) > 1 else ""

    if cmd in ["topic", 't', 'request_topic']:
        if not arg:
            print_fn(f"request_topic = {state['request_topic']}", color=C.CYAN)
        elif arg in {"-reset", "reset"}:
            state["request_topic"] = state["default_request_topic"]
            print_fn(f"request_topic <- {state['request_topic']} (默认)", color=C.GREEN)
        else:
            state["request_topic"] = arg
            print_fn(f"request_topic <- {arg}", color=C.GREEN)

    elif cmd in ["reply", 'reply_topic']:
        if not arg:
            print_fn(f"reply_topic = {state['reply_topic']}", color=C.CYAN)
        elif arg in {"-reset", "reset"}:
            state["reply_topic"] = REPLY_TOPIC
            print_fn(f"reply_topic <- {state['reply_topic']} (默认)", color=C.GREEN)
        else:
            state["reply_topic"] = arg
            print_fn(f"reply_topic <- {arg}", color=C.GREEN)

    elif cmd in ["key", 'k', 'private-key', 'private_key']:
        if not arg:
            kb = state.get("key")
            if kb:
                print_fn(f"已设置客户端私钥（{len(kb)} bytes）", color=C.CYAN)
            else:
                print_fn("未设置客户端私钥", color=C.CYAN)
        elif arg in {"-clear", "clear", "none", "-", '-reset', 'reset'}:
            state["key"] = None
            print_fn("已清除客户端私钥", color=C.YELLOW)
        else:
            try:
                kb = get_standard_pem_bytes(arg)
            except Exception as exc:
                print_fn(f"加载私钥失败: {exc}", color=C.RED)
            else:
                state["key"] = kb
                print_fn(f"已加载客户端私钥（{len(kb)} bytes）", color=C.GREEN)

    elif cmd in ["allow", 'allow_no_server_pubkey_response', 'a']:
        if not arg:
            print_fn(
                f"allow_no_server_pubkey_response = {state['allow_no_pub']}",
                color=C.CYAN,
            )
        elif arg.lower() in {"on", "true", "1", "yes"}:
            state["allow_no_pub"] = True
            print_fn("allow_no_server_pubkey_response <- True", color=C.GREEN)
        elif arg.lower() in {"off", "false", "0", "no"}:
            state["allow_no_pub"] = False
            print_fn("allow_no_server_pubkey_response <- False", color=C.GREEN)
        else:
            print_fn(f"未知取值: {arg}（应为 on/off）", color=C.RED)

    elif cmd in ["his", "history", "hist"]:
        hist_ctl = state.get("hist_ctl")
        if not arg:
            cur = state.get("history_path")
            print_fn(
                f"history file = {cur if cur else '<disabled>'}",
                color=C.CYAN,
            )
        elif arg in {"-clear", "clear", "none", "-", "-reset", "reset", "off"}:
            if hist_ctl:
                hist_ctl["set"](None)
            state["history_path"] = None
            print_fn("history file <- <disabled>", color=C.YELLOW)
        else:
            try:
                new_path = hist_ctl["set"](arg) if hist_ctl else None
            except Exception as exc:
                print_fn(f"设置历史文件失败: {exc}", color=C.RED)
            else:
                state["history_path"] = new_path
                print_fn(f"history file <- {new_path}", color=C.GREEN)

    elif cmd == "status":
        kb = state.get("key")
        hist_path = state.get("history_path")
        print_fn(
            f"request_topic = {state['request_topic']}\n"
            f"reply_topic   = {state['reply_topic']}\n"
            f"key           = {'<set, %d bytes>' % len(kb) if kb else '<none>'}\n"
            f"allow_no_pub  = {state['allow_no_pub']}\n"
            f"history file  = {hist_path if hist_path else '<disabled>'}",
            color=C.CYAN,
        )

    elif cmd in {"help", "?"}:
        print_fn(_MAGIC_HELP, color=C.CYAN)

    elif cmd in {"exit", "quit"}:
        os._exit(0)
        # 不要交给外层 break
        # return "exit"

    else:
        print_fn(f"未知魔术命令: %{cmd}（%help 查看帮助）", color=C.RED)

    return True


def run_shell(
    client,
    timeout: float = 60.0,
    request_topic: str = REQUEST_TOPIC,
    reply_topic: str = REPLY_TOPIC,
    client_private_key_bytes=None,
    allow_no_server_pubkey_response: bool = False,
    history_path=None,
):
    """Run a small IPython-like multiline shell over MQTT."""
    prompt, has_pt, hist_ctl = _build_prompt(history_path=history_path)
    print = _make_print(has_pt)        # noqa: A001 - 故意遮蔽内置 print

    state = {
        "request_topic": request_topic,
        "default_request_topic": REQUEST_TOPIC,
        "reply_topic": reply_topic,
        "key": client_private_key_bytes,
        "allow_no_pub": allow_no_server_pubkey_response,
        "history_path": hist_ctl["get"](),
        "hist_ctl": hist_ctl,
    }

    print("输入 Python 代码，prompt_toolkit 模式支持多行；输入 exit() 或 Ctrl-D 退出。",
          color=C.CYAN)
    print("输入 %help 查看魔术命令（%topic 切换 topic，%key 设置私钥，%his 设置历史文件）。",
          color=C.BLUE)
    if state["history_path"]:
        print(f"[INFO] 历史文件: {state['history_path']}", color=C.GRAY)

    while True:
        try:
            code = prompt()
        except KeyboardInterrupt:
            print("\n[INFO] 已中断当前等待，回到命令提示符。", color=C.YELLOW)
            continue
        except EOFError:
            print("\nCtrl+D 退出", color=C.YELLOW)
            os._exit(0)

        stripped = code.strip()

        if stripped in {"exit()", "quit()"}:
            os._exit(0)
            #break
        if not stripped:
            continue

        # 魔术命令
        if stripped.startswith("%"):
            result = _handle_magic(stripped, state, print)
            # if result == "exit":
                # os._exit(0)
                #break
            continue

        # RPC
        try:
            response = client.request(
                code,
                request_topic=state["request_topic"],
                timeout=timeout,
                client_private_key_bytes=state["key"],
                allow_no_server_pubkey_response=state["allow_no_pub"],
                reply_topic=state["reply_topic"],
            )
        except KeyboardInterrupt:
            print("\n[INFO] 已中断当前请求，回到命令提示符。", color=C.YELLOW)
            continue
        except Exception as exc:
            print(f"[ERROR] 执行请求失败: {exc}", color=C.RED)
            continue

        if response is None:
            continue

        stdout = response.pop("stdout", '')
        r = response.pop("r", '')

        # 元数据用灰色
        print("# " + str(response), color=C.GRAY)

        if stdout:
            print(stdout, end="", color=C.CYAN)

        if response.get("ok"):
            if r not in (None, ""):
                print(r,)
        else:
            print(response.get("error", "remote execution failed"),
                  end="", color=C.RED)


def _fallback_code_input():
    lines = []
    prompt = ">>> "
    while True:
        try:
            line = input(prompt)
        except (EOFError, KeyboardInterrupt):
            raise

        if not line.strip():
            continue

        lines.append(line)
        source = "\n".join(lines)
        try:
            compiled = codeop.compile_command(source, "<shell>", "exec")
        except (SyntaxError, IndentationError):
            _builtins.print("\033[33m[WARN] 非法 Python 语句，已忽略，请继续输入。\033[0m")
            lines = []
            prompt = ">>> "
            continue

        if compiled is not None:
            return source
        prompt = "... "


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="MQTT RPC client")
    parser.add_argument(
        "--client-private-key",
        '--private_key',
        "--private-key",
        "--key", '-key', '-pri', '-k',
        dest="client_private_key",
        default=None,
        help="客户端私钥文件路径或 PEM 内容；启用后请求会带签名。",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=DEFAULT_TIMEOUT,
        help="命令等待远端响应的超时时间，单位秒。",
    )
    parser.add_argument(
        "--allow-no-server-pubkey-response",
        action="store_true",
        help="允许在私钥模式下接收无公钥服务器的返回。默认会拦截。",
    )
    parser.add_argument(
        "--request_topic", "-topic", "-t",
        type=str,
        default=REQUEST_TOPIC,
        dest="request_topic",
        help=f"REPL 起始 request_topic（默认 {REQUEST_TOPIC}）；运行中可用 %topic 切换。",
    )
    parser.add_argument(
        "--reply_topic", "-reply",
        type=str,
        default=REPLY_TOPIC,
        dest="reply_topic",
        help=f"REPL 起始 reply_topic（默认 {REPLY_TOPIC}）；运行中可用 %reply 切换。",
    )
    parser.add_argument(
        "--history", "--history-file", "-H",
        dest="history",
        default=os.path.join(os.path.dirname(os.path.abspath(__file__)), "client_history.db"),#默认 py 文件同目录
        help="磁盘历史记录文件路径（支持 ~ 展开）；不设置则不持久化。运行中可用 %%his 切换。",
    )
    parser.add_argument('--port', '-port', '-p', type=int, default=1166)
    parser.add_argument('--host', '-host', default='0.0.0.0')
    args = parser.parse_args()

    

    # 统一用 multi_mqtt.get_standard_pem_bytes 解析私钥（路径或 PEM 文本都能吃）
    key_bytes = None
    if args.client_private_key:
        try:
            key_bytes = get_standard_pem_bytes(args.client_private_key)
        except Exception as exc:
            print(f"[ERROR] 解析客户端私钥失败: {exc}")
            sys.exit(1)

    try:
        _default_client = MQTTClientNode(
            client_private_key_bytes=key_bytes,
            allow_no_server_pubkey_response=args.allow_no_server_pubkey_response,
        )
        _default_client.start()
        
        import server_http
        ghs = server_http.start_rpc_server(
            port=args.port,
            ip=args.host,
            globals=globals(),
            locals=locals(),
        )
        try:
            if key_bytes:
                print(f"[INFO] 已开启客户端私钥签名模式")
            print(f"[INFO] 允许未验签服务端响应: {args.allow_no_server_pubkey_response}")
            print(f"[INFO] 请求超时: {args.timeout}s  request_topic:{args.request_topic} , reply_topic:{args.reply_topic}")
            run_shell(
                _default_client,
                timeout=args.timeout,
                request_topic=args.request_topic,
                reply_topic=args.reply_topic,
                client_private_key_bytes=key_bytes,
                allow_no_server_pubkey_response=args.allow_no_server_pubkey_response,
                history_path=args.history,
            )
        finally:
            _default_client.stop()
    except KeyboardInterrupt:
        print("\n[INFO] 用户中断，程序已体面退出。")
        sys.exit(0)
    except Exception as exc:
        print(f"[ERROR] 启动 MQTT client 失败: {exc}")
        sys.exit(1)