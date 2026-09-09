#!/usr/bin/env python3
import time,threading,os
import logging
import codeop
import importlib
from multi_mqtt import MultiMQTTManager, get_req_id

logger = logging.getLogger("Client")

REQUEST_TOPIC = "sys/device/request"
RESPONSE_TOPIC = "sys/device/response"

_default_client = None
_default_client_lock = threading.Lock()

class MQTTClientNode:
    def __init__(self):
        # 实例化网络层管理器 (enable_crypto 默认为 False)
        self.mqtt_net = MultiMQTTManager(log_messages=False)
        self.mqtt_net.set_on_message(self._on_message)
        self.pending_requests = {}
        self.lock = threading.Lock()

    def start(self):
        self.mqtt_net.start()
        time.sleep(2)
        self.mqtt_net.subscribe(RESPONSE_TOPIC)

    def _on_message(self, topic, data, rx_broker):
        req_id = data.get("req_id")
        if not req_id:
            return

        with self.lock:
            if req_id in self.pending_requests:
                req_ctx = self.pending_requests.pop(req_id)
                
                # 计算往返时延并注入回复字典
                cost_ms = (time.perf_counter() - req_ctx['start_time']) * 1000
                data["latency_ms"] = round(cost_ms, 2)
                data["client_from"] = rx_broker # 现有 server_from 才有client_from
                
                req_ctx['response'] = data
                req_ctx['event'].set()  # 解锁请求阻塞

    def request(self, payload: str, timeout: float = 5.0):
        req_id = get_req_id()  # 生成 formatted req_id + hash
        start_time = time.perf_counter()
        
        req_data = {
            "req_id": req_id,
            "reply_topic": RESPONSE_TOPIC,
            "code": payload,
            "timestamp": start_time
        }

        event = threading.Event()
        req_ctx = {"event": event, "start_time": start_time, "response": None}
        
        with self.lock:
            self.pending_requests[req_id] = req_ctx

        # 并发投递广播
        self.mqtt_net.publish_broadcast(REQUEST_TOPIC, req_data)

        # 等待最快节点返回
        is_success = event.wait(timeout=timeout)

        if is_success:
            resp = req_ctx['response']
            logger.info(f"{req_data} \n\t{resp}") #耗时: {resp['latency_ms']:.2f}ms
            return resp
        else:
            with self.lock:
                self.pending_requests.pop(req_id, None)
            logger.error(f"❌ [请求超时] req_id={req_id}")
            return None

    def stop(self):
        self.mqtt_net.stop()


def rpc(code: str, timeout: float = 60.0):
    """Execute code through a lazily started shared MQTT client.

    Returns the same response dictionary as ``MQTTClientNode.request``.
    """
    global _default_client
    with _default_client_lock:
        if _default_client is None:
            _default_client = MQTTClientNode()
            _default_client.start()
        client = _default_client
    return client.request(code, timeout=timeout)


def stop():
    """Stop the shared module-level MQTT client, if it was started."""
    global _default_client
    with _default_client_lock:
        client = _default_client
        _default_client = None
    if client is not None:
        client.stop()


def run_shell(client):
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

    print("输入 Python 代码，prompt_toolkit 模式支持多行和语法高亮；输入 exit() 或 Ctrl-D 退出。")
    while True:
        try:
            code = prompt() if session else _fallback_code_input()
        except (EOFError, KeyboardInterrupt):
            print('ctrl+c') # 我按 ctrl+d 退出，怎么也是走这个路径?
            os._exit(0)
            break
        if code.strip() in {"exit()", "quit()"}:
            os._exit(0)
            break
        if not code.strip():
            continue
        response = client.request(code, timeout=60)
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
        line = input(prompt)
        lines.append(line)
        source = "\n".join(lines)
        if codeop.compile_command(source, "<shell>", "exec") is not None:
            return source
        prompt = "... "

if __name__ == "__main__":
    client = MQTTClientNode()
    client.start()
    try:
        run_shell(client)
    finally:
        client.stop()