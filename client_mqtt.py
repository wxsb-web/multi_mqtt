#!/usr/bin/env python3
import argparse
import time, threading, os, sys
import logging
import codeop
import importlib
from multi_mqtt import MultiMQTTManager, get_req_id, utc_ms

logger = logging.getLogger("Client")

REQUEST_TOPIC = "sys/device/request"
RESPONSE_TOPIC = "sys/device/response"
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

    def start(self):
        self.mqtt_net.start()
        time.sleep(2)
        self.mqtt_net.subscribe(RESPONSE_TOPIC)

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

            if req_ctx is not None:
                has_client_pri = bool(req_ctx.get("client_private_key_bytes"))
                allow_no_pub = req_ctx.get("allow_no_server_pubkey_response", False)

                # 拦截逻辑：
                # 如果客户端带有私钥发送 (has_client_pri)
                # 且服务端原样返回了带签名的 req_id (说明服务端没有公钥，未进行验签剥离)
                # 且配置不允许放行此类响应
                if has_client_pri and is_unverified_echo and not allow_no_pub:
                    logger.warning(
                        f"⛔ [安全拦截] req_id={received_req_id} | 服务端未验签 (原样返回了带签名的 req_id)，且 allow_no_server_pubkey_response=False，丢弃该响应。",
                    )
                    return

                # 清理回包中的 req_id 还原为 base_req_id，方便后续统一使用
                if is_unverified_echo:
                    data["req_id"] = base_req_id

                cost_ms = (time.perf_counter() - req_ctx['start_time']) * 1000
                data["latency_ms"] = round(cost_ms, 2)
                data["client_from"] = rx_broker

                req_ctx['response'] = data
                req_ctx['event'].set()

    def request(
        self,
        payload: str,
        request_topic: str = REQUEST_TOPIC,
        timeout: float = 5.0,
        client_private_key_bytes=None,
        allow_no_server_pubkey_response: bool = None,
    ):
        client_private_key_bytes = client_private_key_bytes or self.client_private_key_bytes or getattr(self.mqtt_net, 'client_private_key_bytes', None)
        
        if allow_no_server_pubkey_response is None:
            allow_no_server_pubkey_response = self.allow_no_server_pubkey_response

        # 生成基础的 req_id（不带签名）
        req_id = get_req_id()
        start_time = time.perf_counter()

        req_data = {
            "req_id": req_id,
            "reply_topic": RESPONSE_TOPIC,
            "code": payload,
            "timestamp": utc_ms()
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

        try:
            # MultiMQTTManager 发送时会自动在网络层加上 `|签名`
            self.mqtt_net.publish_broadcast(request_topic, req_data, client_private_key_bytes=client_private_key_bytes)
        except Exception as exc:
            with self.lock:
                self.pending_requests.pop(req_id, None)
            logger.error(f"❌ [请求发送失败] req_id={req_id} error={exc}")
            print(f"[ERROR] 请求发送失败: {exc}")
            return None

        try:
            is_success = event.wait(timeout=timeout)
        except KeyboardInterrupt:
            logger.warning(f"⚠️ [请求中断] req_id={req_id}")
            print("[INFO] 用户中断等待，已停止本次请求。")
            return None
        finally:
            # 解决内存泄漏：请求结束后（成功、超时或中断）清理 pending 记录
            with self.lock:
                self.pending_requests.pop(req_id, None)

        if is_success:
            resp = req_ctx['response']
            # 不打印冗余字典，可直接依赖后续 response 解析
            return resp
        else:
            logger.error(f"❌ [请求超时/被拦截] req_id={req_id}")
            return None

    def stop(self):
        try:
            self.mqtt_net.stop()
        except KeyboardInterrupt:
            print("[INFO] 用户中断，MQTT 连接已停止。")
            logger.warning("⚠️ [用户中断] 已停止 MQTT 连接")


def rpc(
    code: str,
    request_topic: str = REQUEST_TOPIC,
    timeout: float = DEFAULT_TIMEOUT,
    client_private_key_bytes=None,
    allow_no_server_pubkey_response: bool = False,
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
    )


def stop():
    """Stop the shared module-level MQTT client, if it was started."""
    global _default_client
    with _default_client_lock:
        client = _default_client
        _default_client = None
    if client is not None:
        client.stop()


def run_shell(client, timeout: float = 60.0):
    """Run a small IPython-like multiline shell over MQTT."""
    try:
        prompt_toolkit = importlib.import_module("prompt_toolkit")
        prompt_toolkit_key_binding = importlib.import_module("prompt_toolkit.key_binding")
        prompt_toolkit_lexers = importlib.import_module("prompt_toolkit.lexers")
        prompt_toolkit_styles = importlib.import_module("prompt_toolkit.styles")
        pygments_lexers = importlib.import_module("pygments.lexers")

        key_bindings = prompt_toolkit_key_binding.KeyBindings()

        @key_bindings.add("enter")
        def accept_on_empty_line(event):
            buffer = event.current_buffer
            if buffer.document.current_line_before_cursor.strip():
                buffer.insert_text("\n")
            else:
                buffer.validate_and_handle()

        session = prompt_toolkit.PromptSession(
            lexer=prompt_toolkit_lexers.PygmentsLexer(pygments_lexers.PythonLexer),
            style=prompt_toolkit_styles.Style.from_dict({"prompt": "ansicyan"}),
            multiline=True,
            key_bindings=key_bindings,
        )
        prompt = lambda: session.prompt(">>> ")
    except ImportError:
        session = None

    print("输入 Python 代码，prompt_toolkit 模式支持多行；输入 exit() 或 Ctrl-D 退出。")
    while True:
        try:
            code = prompt() if session else _fallback_code_input()
        except KeyboardInterrupt:
            print("\n[INFO] 已中断当前等待，回到命令提示符。")
            continue
        except EOFError:
            break
        
        if code.strip() in {"exit()", "quit()"}:
            break
        if not code.strip():
            continue
            
        try:
            response = client.request(code, timeout=timeout)
        except Exception as exc:
            print(f"[ERROR] 执行请求失败: {exc}")
            continue
            
        if response is None:
            continue
        if response.get("stdout"):
            print(response["stdout"], end="")
        if response.get("ok"):
            if response.get("r") is not None:
                print(response["r"])
        else:
            print(response.get("error", "remote execution failed"), end="")


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
            print("[WARN] 非法 Python 语句，已忽略，请继续输入。")
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
        if os.path.isfile(args.client_private_key):
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