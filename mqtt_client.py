# client.py
import time
import uuid
import logging
import threading
from multi_mqtt import MultiMQTTManager

logger = logging.getLogger("Client")

REQUEST_TOPIC = "sys/device/request"
RESPONSE_TOPIC = "sys/device/response"

class MQTTClientNode:
    def __init__(self):
        self.mqtt_net = MultiMQTTManager(log_messages=False)
        self.mqtt_net.set_on_message(self._on_message)
        self.pending_requests = {}
        self.lock = threading.Lock()

    def start(self):
        self.mqtt_net.start()
        time.sleep(2)
        # 订阅客户端回复 Topic
        self.mqtt_net.subscribe(RESPONSE_TOPIC)

    def _on_message(self, topic, data):
        req_id = data.get("req_id")
        if not req_id:
            return

        with self.lock:
            # 找到正在等待的请求事件
            if req_id in self.pending_requests:
                req_ctx = self.pending_requests.pop(req_id)
                req_ctx['response'] = data
                # 激活线程锁，解锁阻塞的 client.request() 调用
                req_ctx['event'].set()

    def request(self, payload: str, timeout: float = 5.0):
        """向服务端发起请求并等待首个最快响应 (类似 HTTP GET/POST)"""
        msg_id = f"req_{int(time.time()*1000)}_{uuid.uuid4().hex[:6]}"
        
        req_data = {
            "msg_id": msg_id,
            "reply_topic": RESPONSE_TOPIC,
            "payload": payload,
            "timestamp": time.time()
        }

        # 注册等待句柄
        event = threading.Event()
        req_ctx = {"event": event, "response": None}
        
        with self.lock:
            self.pending_requests[msg_id] = req_ctx

        # 并发向多 Broker 广播发送请求
        start_t = time.time()
        self.mqtt_net.publish_broadcast(REQUEST_TOPIC, req_data)

        # 阻塞等待最快返回的结果
        is_success = event.wait(timeout=timeout)
        elapsed = (time.time() - start_t) * 1000

        if is_success:
            logger.info(f"✨ [请求成功] 耗时: {elapsed:.2f}ms")
            return req_ctx['response']
        else:
            with self.lock:
                self.pending_requests.pop(msg_id, None)
            logger.error(f"❌ [请求超时] msg_id={msg_id}")
            return None

    def stop(self):
        self.mqtt_net.stop()

if __name__ == "__main__":
    client = MQTTClientNode()
    client.start()

    # 测试多次请求
    for i in range(1, 4):
        msg = f"Hello Multi-Broker MQTT Message #{i}"
        logger.info(f"发送消息: {msg}")
        resp = client.request(payload=msg, timeout=5.0)
        print(f"收到回应 -> {resp}\n")
        time.sleep(2)

    client.stop()