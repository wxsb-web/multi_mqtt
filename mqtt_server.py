# server.py
import time
import logging
from multi_mqtt import MultiMQTTManager, stime

logger = logging.getLogger("Server")
REQUEST_TOPIC = "sys/device/request"

class MQTTServer:
    def __init__(self):
        # 实例化网络层管理器 (enable_crypto 默认为 False)
        self.mqtt_net = MultiMQTTManager(log_messages=False)
        self.mqtt_net.set_on_message(self.handle_message)

    def handle_message(self, topic, data, rx_broker):
        req_id = data.get("req_id")
        reply_topic = data.get("reply_topic")
        payload = data.get("payload")

        logger.info(f"⚡ [{stime()}] [服务端处理请求] req_id={req_id} (首发节点: {rx_broker})")

        # 构造 Response 字典
        response_data = {
            "msg_id": f"resp_{req_id}",
            "req_id": req_id,
            "status": 200,
            "echo": payload,
            "server_time": time.time(),
            "server_from": rx_broker  # 标注来自哪个公共服务器
        }

        if reply_topic:
            self.mqtt_net.publish_broadcast(reply_topic, response_data)

    def start(self):
        self.mqtt_net.start()
        time.sleep(2)
        self.mqtt_net.subscribe(REQUEST_TOPIC)
        logger.info(f"🚀 [{stime()}] 服务端已就绪，正在监听: {REQUEST_TOPIC}")

        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            self.mqtt_net.stop()

if __name__ == "__main__":
    server = MQTTServer()
    server.start()