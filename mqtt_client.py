# client.py
import time
import logging
import threading
from multi_mqtt import MultiMQTTManager, get_req_id

logger = logging.getLogger("Client")

REQUEST_TOPIC = "sys/device/request"
RESPONSE_TOPIC = "sys/device/response"

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
                data["client_from"] = rx_broker
                
                req_ctx['response'] = data
                req_ctx['event'].set()  # 解锁请求阻塞

    def request(self, payload: str, timeout: float = 5.0):
        req_id = get_req_id()  # 生成 formatted req_id + hash
        start_time = time.perf_counter()
        
        req_data = {
            "req_id": req_id,
            "msg_id": req_id,
            "reply_topic": RESPONSE_TOPIC,
            "payload": payload,
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
            logger.info(f"✨ [请求成功] 耗时: {resp['latency_ms']:.2f}ms")
            return resp
        else:
            with self.lock:
                self.pending_requests.pop(req_id, None)
            logger.error(f"❌ [请求超时] req_id={req_id}")
            return None

    def stop(self):
        self.mqtt_net.stop()

if __name__ == "__main__":
    client = MQTTClientNode()
    client.start()

    for i in range(99):
        msg = f"Hello Multi-Broker MQTT Message #{i}"
        logger.info(f"发送消息: {msg}")
        resp = client.request(payload=msg, timeout=60)
        print(f"收到回应 -> {resp}\n")
        time.sleep(3)

    client.stop()