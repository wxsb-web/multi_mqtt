#!/usr/bin/env python3
from multi_mqtt import MultiMQTTManager, stime
import time
import logging
from rpc_executor import PythonExecutor, format_result

logger = logging.getLogger("Server")
REQUEST_TOPIC = "sys/device/request"

class MQTTServer:
    def __init__(self,server_public_key_bytes=None,):
        # 实例化网络层管理器 (enable_crypto 默认为 False)
        self.mqtt_net = MultiMQTTManager(log_messages=False,server_public_key_bytes=server_public_key_bytes)
        self.mqtt_net.set_on_message(self.handle_message)
        self.executor = PythonExecutor()

    def handle_message(self, topic, data, rx_broker):
        req_id = data.get("req_id")
        reply_topic = data.get("reply_topic")
        code = data.get("code", data.get("payload"))

        logger.info(f"⚡ [{stime()}] [服务端处理请求] req_id={req_id} (首发节点: {rx_broker})")

        execution = self.executor.execute(code)
        server_time = time.time()
        response_data = {
            "req_id": req_id,
            "r": format_result(execution["r"]) if execution["ok"] else None,
            "stdout": execution["stdout"],
            "ok": execution["ok"],
            "server_time": server_time,
            "server_from": rx_broker,
#"latency_send":round(server_time-data.get("timestamp")*1000, 2) # client server 时间不同步，测量出不是真实值

        }
        if not execution["ok"]:
            response_data["error"] = execution["error"]

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
    import argparse,server_http
    parser = argparse.ArgumentParser(description='mqtt http rpc')
    parser.add_argument('--port', type=int, default=1177)
    parser.add_argument('--host', default='0.0.0.0')
    args = parser.parse_args()
    ghs=server_http.start_rpc_server(
        port=args.port,
        ip=args.host,
        globals=globals(),
        locals=locals(),
        # websocket_handler=editor.websocket,
        # websocket_path='/ws',
        # redirect_root='/preview_html(p)',
    )
    
    gms = MQTTServer(
server_public_key_bytes=b'ecdsa-sha2-nistp256 AAAAE2VjZHNhLXNoYTItbmlzdHAyNTYAAAAIbmlzdHAyNTYAAABBBER9c5vu215n+5gv1YjGdm78Nf99wpfqw1fIT8nXib2FLUglq4NBMe7hLp2VOkqv9z00m5Wn+uUADH4zyXLiWzI=',
    )
    print(ghs,gms)
    gms.start()