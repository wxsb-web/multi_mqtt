# multi_mqtt.py
import json
import uuid
import time
import logging
import threading
from collections import OrderedDict
from paho.mqtt import client as mqtt_client
from paho.mqtt.enums import CallbackAPIVersion

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("MultiMQTT")

# 可用的免费公共 Broker 节点
BROKER_LIST = [
    ("broker-cn.emqx.io", 1883),
    ("test.mosquitto.org", 1883),
    ("broker.mqtt.cool", 1883),
    ("mqtt.tyckr.io", 1883),
    ("public-mqtt-broker.bevywise.com", 1883),
]

class TTLCache:
    """轻量级内存 TTL 缓存去重器（无第三方依赖，省电高效）"""
    def __init__(self, ttl_seconds=60):
        self.ttl = ttl_seconds
        self.cache = OrderedDict()
        self.lock = threading.Lock()

    def add_if_not_exists(self, key: str) -> bool:
        """如果 Key 不存在则添加并返回 True；如果已存在则返回 False"""
        now = time.time()
        with self.lock:
            self._cleanup(now)
            if key in self.cache:
                return False
            self.cache[key] = now
            return True

    def _cleanup(self, now: float):
        while self.cache and next(iter(self.cache.values())) < now - self.ttl:
            self.cache.popitem(last=False)

class MultiMQTTManager:
    def __init__(self, brokers=BROKER_LIST, log_messages=False):
        self.brokers = brokers
        self.clients = []
        self.log_messages = log_messages
        self.dedup_cache = TTLCache(ttl_seconds=30)
        self.message_callback = None
        self.subscribed_topics = set()
        self.lock = threading.Lock()

    def set_on_message(self, callback):
        """设置上层消息接收回调，回调签名: fn(topic, payload_dict)"""
        self.message_callback = callback

    def start(self):
        """同时启动与所有 Broker 的连接"""
        for host, port in self.brokers:
            client_id = f"multi_mqtt_{int(time.time()*1000)}_{uuid.uuid4().hex[:4]}"
            client = mqtt_client.Client(CallbackAPIVersion.VERSION2, client_id=client_id, protocol=mqtt_client.MQTTv311)
            
            # 绑定回调
            client.on_connect = self._make_on_connect(host)
            client.on_message = self._on_message_wrapper
            
            try:
                client.connect_async(host, port, keepalive=30)
                client.loop_start()
                self.clients.append(client)
                logger.info(f"已发起连接异步任务 -> {host}:{port}")
            except Exception as e:
                logger.error(f"连接初始化失败 [{host}]: {e}")

    def _make_on_connect(self, host):
        def on_connect(client, userdata, flags, rc, properties=None):
            if rc == 0:
                logger.info(f"🟢 [已连接] Broker: {host}")
                with self.lock:
                    for topic in self.subscribed_topics:
                        client.subscribe(topic)
            else:
                logger.warning(f"🔴 [连接失败] Broker: {host}, rc={rc}")
        return on_connect

    def _on_message_wrapper(self, client, userdata, msg):
        try:
            payload_str = msg.payload.decode('utf-8')
            data = json.loads(payload_str)
            msg_id = data.get("msg_id")

            # 网络首胜去重核心判断
            if msg_id:
                if not self.dedup_cache.add_if_not_exists(msg_id):
                    # 重复消息，直接丢弃（省去后续解析和处理开销）
                    return

            if self.log_messages:
                logger.info(f"📩 收到首发消息 [{msg.topic}]: {payload_str[:100]}")

            if self.message_callback:
                self.message_callback(msg.topic, data)
        except Exception as e:
            # 非 JSON 格式或解析失败处理
            pass

    def subscribe(self, topic: str):
        """订阅所有 Broker 上的指定 Topic"""
        with self.lock:
            self.subscribed_topics.add(topic)
            for c in self.clients:
                if c.is_connected():
                    c.subscribe(topic)

    def publish_broadcast(self, topic: str, payload_dict: dict):
        """向所有 Broker 广播同一条消息"""
        payload_str = json.dumps(payload_dict)
        for c in self.clients:
            if c.is_connected():
                c.publish(topic, payload_str, qos=0)

    def stop(self):
        for c in self.clients:
            c.loop_stop()
            c.disconnect()
        logger.info("所有 MQTT 连接已安全关闭")
        