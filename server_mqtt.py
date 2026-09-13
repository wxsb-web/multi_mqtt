#!/usr/bin/env python3
from multi_mqtt import BROKER_LIST, MultiMQTTManager, stime, utc_ms, _describe_public_key
import time
import logging
from rpc_executor import PythonExecutor, format_result

logger = logging.getLogger("Server")
REQUEST_TOPIC = "sys/device/request"
DEFAULT_REPLY_TOPIC = "sys/device/response"

class MQTTServer:
    def __init__(self, server_public_key_bytes=None, brokers=BROKER_LIST,
                 request_topic=REQUEST_TOPIC, reply_topic=DEFAULT_REPLY_TOPIC):
        # 实例化网络层管理器 (enable_crypto 默认为 False)
        self.request_topic = request_topic
        self.reply_topic = reply_topic
        manager_args = {
            "log_messages": True,
            "server_public_key_bytes": server_public_key_bytes,
        }
        manager_args["brokers"] = brokers
        self.mqtt_net = MultiMQTTManager(**manager_args)
        self.mqtt_net.set_on_message(self.handle_message)
        self.executor = PythonExecutor()

    def handle_message(self, topic, data, rx_broker):
        req_id = data.get("req_id")
        reply_topic = data.get("reply_topic") or self.reply_topic
        code = data.get("code", data.get("payload"))
        server_pubkey = self.mqtt_net.server_public_key_bytes

        logger.info(
            "⚡ [%s] [服务端处理请求] req_id=%s (首发节点: %s) | has_code=%s | has_server_pubkey=%s | server_pubkey=%s",
            stime(),
            req_id,
            rx_broker,
            bool(code),
            bool(server_pubkey),
            server_pubkey,
        )

        execution = self.executor.execute(code)
        server_time = utc_ms()
        if (self.mqtt_net.server_public_key_bytes ) and '|' in req_id:
            req_id= req_id.split('|')[0]  # client 发送经过签名后 ，收到自动去除返回 代表验证执行成功
        response_data = {
            "req_id": req_id,
            "r": format_result(execution["r"]) if execution["ok"] else None,
            "stdout": execution["stdout"],
            "ok": execution["ok"],
            "server_time": server_time,
            "server_from": rx_broker,
#"latency_send":server_time-data.get("timestamp") # client server 时间不同步，测量出不是真实值

        }
        if not execution["ok"]:
            response_data["error"] = execution["error"]

        if reply_topic:
            self.mqtt_net.publish_broadcast(reply_topic, response_data)

    def start(self):
        self.mqtt_net.start()
        time.sleep(2)
        self.mqtt_net.subscribe(self.request_topic)
        logger.info(
            "🚀 [%s] 服务端已就绪，正在监听: %s | reply_topic=%s | mqtt_pub_key=%s | 验签=%s",
            stime(),
            self.request_topic,
            self.reply_topic,
            _describe_public_key(self.mqtt_net.server_public_key_bytes),
            '启用' if self.mqtt_net.server_public_key_bytes else '关闭（接收所有消息）',
        )

        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            self.mqtt_net.stop()


def start(config):# 为了导出给 Chaquopy 调用
    server = MQTTServer(
        server_public_key_bytes=str(config.get("mqtt_pub_key", "")) or None,
        request_topic=str(config.get("mqtt_request_topic", REQUEST_TOPIC)),
        reply_topic=str(config.get("mqtt_reply_topic", DEFAULT_REPLY_TOPIC)),
    )
    logger.info("🚀 启动 MQTT RPC，使用网络层 BROKER_LIST，共 %d 个 Broker", len(BROKER_LIST))
    pub_key_raw = str(config.get("mqtt_pub_key", ""))
    server_pubkey = pub_key_raw.strip()
    server.mqtt_net.start()
    time.sleep(2)
    server.mqtt_net.subscribe(server.request_topic)
    logger.info(
        "🚀 [%s] 服务端已就绪，正在监听: %s | reply_topic=%s | mqtt_pub_key=%s | 验签=%s",
        stime(),
        server.request_topic,
        server.reply_topic,
        _describe_public_key(server.mqtt_net.server_public_key_bytes),
        '启用' if server.mqtt_net.server_public_key_bytes else '关闭（接收所有消息）',
    )
    return server


if __name__ == "__main__":
    import argparse,server_http
    parser = argparse.ArgumentParser(description='mqtt http rpc')
    parser.add_argument('--port','-port','-p', type=int, default=1177)
    parser.add_argument('--host','-host', default='0.0.0.0')
    _PUB=b'ecdsa-sha2-nistp256 AAAAE2VjZHNhLXNoYTItbmlzdHAyNTYAAAAIbmlzdHAyNTYAAABBBER9c5vu215n+5gv1YjGdm78Nf99wpfqw1fIT8nXib2FLUglq4NBMe7hLp2VOkqv9z00m5Wn+uUADH4zyXLiWzI='
    #_PUB=b''
    # nargs='*' ：--pub 放到最后，其后所有空格分隔的片段会被拼回一个参数
    # --pub（后面什么都不给） 或 --pub ""  ->  b''
    parser.add_argument('--pub','--pubkey','-pub', nargs='*', default=None)
    args = parser.parse_args()
    args.pub = _PUB if args.pub is None else ' '.join(args.pub).encode('utf-8')
    
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
server_public_key_bytes=args.pub,
    )
    print(ghs,gms)
    gms.start()