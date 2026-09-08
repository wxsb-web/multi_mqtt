# server.py
import time
import logging
from multi_mqtt import MultiMQTTManager

logger = logging.getLogger("Server")

REQUEST_TOPIC = "sys/device/request"

class MQTTServer:
    def __init__(self):
        # 初始化多 Broker 网络层
        self.mqtt_net = MultiMQTTManager(log_messages=True)
        self.mqtt_net.set_on_message(self.handle_message)

    def handle_message(self, topic, data):
        """处理经过去重的第一个到达的请求消息"""
        msg_id = data.get("msg_id")
        reply_topic = data.get("reply_topic")
        payload = data.get("payload")

        logger.info(f"⚡ [服务端处理请求] msg_id={msg_id}, payload={payload}")

        # 构造响应
        response_data = {
            "msg_id": f"resp_{msg_id}",  # 唯一响应ID
            "req_id": msg_id,
            "status": 200,
            "echo": payload,
            "server_time": time.time()
        }

        # 通过多节点广播回发
        if reply_topic:
            self.mqtt_net.publish_broadcast(reply_topic, response_data)

    def start(self):
        self.mqtt_net.start()
        time.sleep(2)  # 等待连接建立
        self.mqtt_net.subscribe(REQUEST_TOPIC)
        logger.info(f"🚀 服务端已就绪，正在监听: {REQUEST_TOPIC}")

        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            self.mqtt_net.stop()

if __name__ == "__main__":
    server = MQTTServer()
    server.start()