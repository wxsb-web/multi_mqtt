#!/usr/bin/env python3
import argparse
import time, threading, os, sys
import logging
import codeop
import importlib
import builtins as _builtins
from multi_mqtt import MultiMQTTManager, get_req_id, utc_ms

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
#  Shell: 统一 prompt + 彩色 print                                 #
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

# 常用色 ANSI 字面量（可直接 print(..., color=C.RED)）
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


def _build_prompt():
    """返回一个无参函数 prompt() -> str。

    有 prompt_toolkit 就用 PromptSession（多行 + 语法高亮）；
    否则退回标准库 input 的 codeop 多行累积器。
    """
    try:
        prompt_toolkit = importlib.import_module("prompt_toolkit")
        pt_key_binding = importlib.import_module("prompt_toolkit.key_binding")
        pt_lexers = importlib.import_module("prompt_toolkit.lexers")
        pt_styles = importlib.import_module("prompt_toolkit.styles")
        pygments_lexers = importlib.import_module("pygments.lexers")
    except ImportError:
        return _fallback_code_input, False

    key_bindings = pt_key_binding.KeyBindings()

    @key_bindings.add("enter")
    def accept_on_empty_line(event):
        buffer = event.current_buffer
        if buffer.document.current_line_before_cursor.strip():
            buffer.insert_text("\n")
        else:
            buffer.validate_and_handle()

    session = prompt_toolkit.PromptSession(
        lexer=pt_lexers.PygmentsLexer(pygments_lexers.PythonLexer),
        style=pt_styles.Style.from_dict({"prompt": "ansicyan"}),
        multiline=True,
        key_bindings=key_bindings,
    )

    def prompt_with_pt():
        return session.prompt(">>> ")

    return prompt_with_pt, True


def _make_print(has_pt: bool):
    """构造一个兼容内置 print 签名、增加 color 参数的 print。

    color 支持：
      - ""         : 无色
      - ANSI 转义  : "\\033[31m" / "\\033[38;5;208m" / "\\033[38;2;r;g;bm"
      - PT 样式名  : "ansired" / "ansigray" / "bold"（无 PT 时自动映射）
    """
    if has_pt:
        from prompt_toolkit import print_formatted_text as _pt_print
        from prompt_toolkit.formatted_text import FormattedText as _pt_ft, ANSI as _pt_ansi
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


def run_shell(client, timeout: float = 60.0):
    """Run a small IPython-like multiline shell over MQTT."""
    prompt, has_pt = _build_prompt()
    print = _make_print(has_pt)        # noqa: A001 - 故意遮蔽内置 print

    print("输入 Python 代码，prompt_toolkit 模式支持多行；输入 exit() 或 Ctrl-D 退出。",
          color=C.CYAN if not has_pt else "ansicyan")

    while True:
        try:
            code = prompt()
        except KeyboardInterrupt:
            print("\n[INFO] 已中断当前等待，回到命令提示符。", color=C.YELLOW)
            continue
        except EOFError:
            break

        if code.strip() in {"exit()", "quit()"}:
            break
        if not code.strip():
            continue

        try:
            response = client.request(code, timeout=timeout)
        except KeyboardInterrupt:
            # 用户在等待远端响应时按了 Ctrl-C：提示一下，继续下一轮
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
        "--private-key",
        "--key", '-key', '-pri',
        dest="client_private_key",
        default=None,
        help="客户端私钥文件路径或 PEM 内容；启用后请求会带签名。",
    )
    parser.add_argument(
        "--timeout",
        "-t",
        type=float,
        default=DEFAULT_TIMEOUT,
        help="命令等待远端响应的超时时间，单位秒。",
    )
    parser.add_argument(
        "--allow-no-server-pubkey-response",
        action="store_true",
        help="允许在私钥模式下接收无公钥服务器的返回。默认会拦截。",
    )
    parser.add_argument('--port', '-port', '-p', type=int, default=1166)
    parser.add_argument('--host', '-host', default='0.0.0.0')
    args = parser.parse_args()

    import server_http
    ghs = server_http.start_rpc_server(
        port=args.port,
        ip=args.host,
        globals=globals(),
        locals=locals(),
    )

    # 规范化私钥读取逻辑
    key_bytes = None
    if args.client_private_key:
        try:
            is_file = os.path.isfile(args.client_private_key)
        except OSError:
            # 长字符串（PEM 内容含换行）可能会让 isfile 抛 OSError
            is_file = False
        if is_file:
            with open(args.client_private_key, "rb") as f:
                key_bytes = f.read()
        else:
            key_bytes = args.client_private_key.encode("utf-8")

    try:
        client = MQTTClientNode(
            client_private_key_bytes=key_bytes,
            allow_no_server_pubkey_response=args.allow_no_server_pubkey_response,
        )
        client.start()
        try:
            if key_bytes:
                print(f"[INFO] 已开启客户端私钥签名模式")
            print(f"[INFO] 允许未验签服务端响应: {args.allow_no_server_pubkey_response}")
            print(f"[INFO] 请求超时: {args.timeout}s")
            run_shell(client, timeout=args.timeout)
        finally:
            client.stop()
    except KeyboardInterrupt:
        print("\n[INFO] 用户中断，程序已体面退出。")
        sys.exit(0)
    except Exception as exc:
        print(f"[ERROR] 启动 MQTT client 失败: {exc}")
        sys.exit(1)