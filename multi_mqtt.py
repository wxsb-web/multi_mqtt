# multi_mqtt.py
import json
import time
import os
import uuid
import hashlib
import random
import logging
import base64
import threading
from collections import OrderedDict
from paho.mqtt import client as mqtt_client
from paho.mqtt.enums import CallbackAPIVersion

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("MultiMQTT")

# 预设公共 MQTT Broker 列表
BROKER_LIST = [
    ("broker.hivemq.com", 1883),               # RTT: 220.6 ms | 建连: 6386.8 ms (建连较慢，收发极快)
    ("broker.mqtt-dashboard.com", 1883),        # RTT: 230.8 ms | 建连:  671.7 ms (综合体验佳)
    ("broker-cn.emqx.io", 1883),                # RTT: 261.4 ms | 建连:  592.4 ms (国内推荐)
    ("broker.emqx.io", 1883),                   # RTT: 279.0 ms | 建连:  662.9 ms
    ("test.mosquitto.org", 1883),               # RTT: 381.2 ms | 建连:  510.4 ms
    ("mqtt.tyckr.io", 1883),                    # RTT: 391.0 ms | 建连:  526.3 ms
    ("broker.mqtt.cool", 1883),                 # RTT: 424.7 ms | 建连:  548.0 ms
    ("mqtt.loralab.org", 1883),                 # RTT: 432.7 ms | 建连:  556.0 ms
    ("public-mqtt-broker.bevywise.com", 1883),  # RTT: 506.9 ms | 建连:  754.2 ms
]


def stime(format='%Y-%m-%d__%H.%M.%S',ms_splitor='__.'):
    """可读毫秒级时间戳"""
    ft = time.time()
    return time.strftime(format, time.localtime(ft)) + ms_splitor + f"{ft:.3f}".split('.')[1]

def get_req_id():
    """生成格式：req_YYYY-MM-DD__HH.MM.SS__.毫秒_随机Hash"""
    hash_str = hashlib.md5(f"{time.time()}_{random.random()}".encode()).hexdigest()[:6]
    return f"{stime(format='%Y%m%d_%H%M%S',ms_splitor='.')} {hash_str}"

AES_KEY = b"12345678901234567890123456789012"
def process_cipher(data, decrypt=False, enabled=False, key=AES_KEY):
    """加密 解密 浓缩函数：默认关闭 (enabled=False)。开启时使用 AES-GCM，关闭时仅转换 JSON"""
    if not enabled:
        return json.loads(data) if decrypt else json.dumps(data)
    
    # 动态导入，关闭时无需依赖 cryptography 库
    from cryptography.hazmat.primitives.ciphers.aead import AESGCM
    aesgcm = AESGCM(key)
    if decrypt:
        raw = base64.b64decode(data.encode('utf-8'))
        return json.loads(aesgcm.decrypt(raw[:12], raw[12:], None).decode('utf-8'))
    else:
        nonce = os.urandom(12)
        plaintext = json.dumps(data).encode('utf-8')
        ciphertext = aesgcm.encrypt(nonce, plaintext, None)
        return base64.b64encode(nonce + ciphertext).decode('utf-8')

class TTLCache:
    """轻量级内存去重缓存"""
    def __init__(self, ttl_seconds=30):
        self.ttl = ttl_seconds
        self.cache = OrderedDict()
        self.lock = threading.Lock()

    def add_if_not_exists(self, key: str) -> bool:
        now = time.time()
        with self.lock:
            while self.cache and next(iter(self.cache.values())) < now - self.ttl:
                self.cache.popitem(last=False)
            if key in self.cache:
                return False
            self.cache[key] = now
            return True

class MultiMQTTManager:
    def __init__(self, brokers=BROKER_LIST, log_messages=False, enable_crypto=False):
        self.brokers = brokers
        self.clients = {}
        self.log_messages = log_messages
        self.enable_crypto = enable_crypto  # 默认关闭加密
        self.dedup_cache = TTLCache(ttl_seconds=30)
        self.message_callback = None
        self.subscribed_topics = set()
        self.lock = threading.Lock()

    def set_on_message(self, callback):
        """设置上层回调，签名: fn(topic, data_dict, rx_broker)"""
        self.message_callback = callback

    def start(self):
        """启动与所有 Broker 的连接并启用后台自动断线重连"""
        for host, port in self.brokers:
            client_id = f"multi_client_{int(time.time()*1000)}_{uuid.uuid4().hex[:4]}"
            client = mqtt_client.Client(CallbackAPIVersion.VERSION2, client_id=client_id, protocol=mqtt_client.MQTTv311)
            
            # 开启自动重连退避策略 (1~60秒)
            client.reconnect_delay_set(min_delay=1, max_delay=60)
            
            client.on_connect = self._make_on_connect(host)
            client.on_disconnect = self._make_on_disconnect(host)
            client.on_message = self._make_on_message(host)

            try:
                client.connect_async(host, port, keepalive=30)
                client.loop_start()
                self.clients[host] = client
                logger.info(f"开启后台连接任务 -> {host}:{port}")
            except Exception as e:
                logger.error(f"连接初始化失败 [{host}]: {e}")

    def _make_on_connect(self, host):
        def on_connect(client, userdata, flags, rc, properties=None):
            if rc == 0:
                logger.info(f"✅ [已连接] Broker: {host}")
                with self.lock:
                    for topic in self.subscribed_topics:
                        client.subscribe(topic)
            else:
                logger.warning(f"❌ [连接失败] Broker: {host}, rc={rc}")
        return on_connect

    def _make_on_disconnect(self, host):
        def on_disconnect(client, userdata, flags, rc, properties=None):
            if rc != 0:
                logger.warning(f"⚠️ [意外断开] Broker: {host} (rc={rc})，自动尝试重连...")
        return on_disconnect

    def _make_on_message(self, host):
        def on_message(client, userdata, msg):
            try:
                raw_payload = msg.payload.decode('utf-8')
                data = process_cipher(raw_payload, decrypt=True, enabled=self.enable_crypto)
                
                # 去重判定：首胜丢弃逻辑
                req_id = data.get("req_id")
                if req_id and not self.dedup_cache.add_if_not_exists(req_id):
                    return

                if self.log_messages:
                    logger.info(f"📩 收到消息 [{msg.topic}] 来自 {host}")

                if self.message_callback:
                    self.message_callback(msg.topic, data, host)
            except Exception:
                pass
        return on_message

    def subscribe(self, topic: str):
        with self.lock:
            self.subscribed_topics.add(topic)
            for host, c in self.clients.items():
                if c.is_connected():
                    c.subscribe(topic)

    def publish_broadcast(self, topic: str, payload_dict: dict):
        """广播传输消息"""
        payload_str = process_cipher(payload_dict, decrypt=False, enabled=self.enable_crypto)
        for host, c in self.clients.items():
            if c.is_connected():
                c.publish(topic, payload_str, qos=0)

    def stop(self):
        for c in self.clients.values():
            c.loop_stop()
            c.disconnect()
        logger.info("所有 MQTT 连接已安全关闭")