# multi_mqtt.py
import importlib.util,os,subprocess,sys
def ensure_dependencies():
    packages = {
        "paho": "paho-mqtt",
        "ecdsa": "ecdsa",  # [新增] 仅追加了 ecdsa 依赖，以支持私钥签名
    }
    missing = [package for module, package in packages.items()
               if importlib.util.find_spec(module) is None]
    if not missing:
        return

    index_url = "https://pypi.tuna.tsinghua.edu.cn/simple"
    print(f"[+] 正在使用清华源安装依赖: {', '.join(missing)}")
    command = [
        sys.executable,
        "-m",
        "pip",
        "install",
        "-i",
        index_url,
        "--trusted-host",
        "pypi.tuna.tsinghua.edu.cn",
        *missing,
    ]
    try:
        subprocess.check_call(command)
    except (OSError, subprocess.CalledProcessError) as error:
        print(f"[!] 依赖安装失败: {error}", file=sys.stderr)
        sys.exit(1)
ensure_dependencies()

import json
import time
import os
import uuid
import hashlib
import random
import logging
import base64
import struct
import threading
import ecdsa  # [新增]
from collections import OrderedDict
from paho.mqtt import client as mqtt_client
from paho.mqtt.enums import CallbackAPIVersion

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("MultiMQTT")

# 预设公共 MQTT Broker 列表
BROKER_LIST = [
    ("mqtt.touchsocket.net", 1883),             # RTT:  29.5 ms | 建连:  293.4 ms (b 视频)
    ("broker.codenow.cn", 1883),                # RTT:  31.4 ms | 建连:  110.1 ms (CodeNow 国内公共MQTT)
    ("broker.emqx.io", 1883),                   # RTT: 282.9 ms | 建连:  620.4 ms (EMQX 国际)
    ("mqtt.loralab.org", 1883),                 # RTT: 349.0 ms | 建连:  524.4 ms (LoRaLab)
    ("test.mosquitto.org", 1883),               # RTT: 398.4 ms | 建连:  586.0 ms (Mosquitto 官方)
    ("broker-cn.emqx.io", 1883),                # RTT: 407.3 ms | 建连:  751.3 ms (EMQX 中国)
    ("broker.hivemq.com", 1883),                # RTT: 301.0 ms | 建连:12830.1 ms (HiveMQ 官方，建连极慢，收发快)
    ("broker.mqtt-dashboard.com", 1883),        # RTT: 307.1 ms | 建连:  465.6 ms (HiveMQ Dashboard，综合体验佳)
    ("broker.mqtt.cool", 1883),                 # RTT: 425.4 ms | 建连:  594.8 ms (MQTT.Cool)
    ("mqtt.iotbhai.io", 1883),                  # RTT: 429.3 ms | 建连:  597.7 ms (IoTbhai)
    ("mqtt.tyckr.io", 1883),                    # RTT: 436.0 ms | 建连:  596.2 ms (Tyckr)
    ("public-mqtt-broker.bevywise.com", 1883),  # RTT: 451.3 ms | 建连:  575.9 ms (Bevywise)
]

'''
这5个允许订阅 #  。泄漏所有消息
 Broker 节点               ┃     历史总 Msg / Topic 
━━━━━━━━━━━━━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━━━━━━━━
 broker.mqtt.cool          │       18,183,031 / 976 
 mqtt.loralab.org          │             40,734 / 3 
 mqtt.tyckr.io             │        1,694,738 / 436 
 public-mqtt-broker.bevyw… │           200,928 / 69 
 test.mosquitto.org        │   188,030,853 / 77,948 
───────────────────────────┴────────────────────────


逻辑还是没有清晰  ，这次不写代码。  client 发送时候有私钥 可以签名， server 启动时候只有公钥 收到  再次回复   那返回信息又没有签名。  你的代码可以正确解析这种情况吗。   如果要实现完整加密，那又太复杂了  。  https ，ssh 密钥协商

客户端发送指令时：由于你要让服务器执行代码，payload 里必然带有 "code": "import platform..."。客户端的发送函数一看到有 "code"，并且自己有私钥，就会主动触发签名。
服务端接收指令时：服务端看到有 "code"，并且自己配了公钥，就会强制触发验签。如果不通过，直接丢弃。
服务端返回结果时：服务端的回复 payload 是 {"req_id": "...", "stdout": "...", "ok": True}。里面没有 "code" 字段。此时服务端的发送函数会直接跳过签名逻辑，把原封不动的 JSON 发回去。
客户端接收结果时：客户端收到回复，看到 payload 里没有 "code" 字段，就会直接跳过验签逻辑，走原来的普通流程（去重 -> 打印结果）。
'''

def stime(format='%Y-%m-%d__%H.%M.%S',ms_splitor='__.'):
    """可读毫秒级时间戳"""
    ft = time.time()
    return time.strftime(format, time.localtime(ft)) + ms_splitor + f"{ft:.3f}".split('.')[1]

def utc_ms():
    """Return the current UTC Unix timestamp in integer milliseconds."""
    return time.time_ns() // 1_000_000

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

def get_standard_pem_bytes(key_input) -> bytes:
    """
    [新增组件] 统一私钥解析：支持 整数secexp、文件路径、OpenSSH格式、标准PEM格式(bytes/str)
    最终统一返回 ecdsa 库直接兼容的标准 PEM (SEC1) 字节流。
    """
    if not key_input:
        return None
        
    # 1. 整数或纯数字字符串当作 secexp 处理
    if isinstance(key_input, int) or (isinstance(key_input, str) and key_input.isdigit()):
        secexp = int(key_input)
        return ecdsa.SigningKey.from_secret_exponent(secexp=secexp, curve=ecdsa.NIST256p).to_pem()
        
    # 2. 如果是文件路径，读取内容；否则转为 bytes
    raw_bytes = b""
    if isinstance(key_input, str):
        if os.path.isfile(key_input):
            with open(key_input, "rb") as f:
                raw_bytes = f.read()
        else:
            raw_bytes = key_input.encode('utf-8')
    elif isinstance(key_input, bytes):
        raw_bytes = key_input
        
    if not raw_bytes:
        raise ValueError("无法解析传入的 client_private_key_bytes")

    # 3. 检查是否已经是原生 ecdsa 支持的格式
    if b"-----BEGIN EC PRIVATE KEY-----" in raw_bytes or b"-----BEGIN PRIVATE KEY-----" in raw_bytes:
        return raw_bytes
        
    # 4. 如果是 OpenSSH 格式，动态借助 cryptography 转换为 ecdsa 库兼容的标准格式
    if b"-----BEGIN OPENSSH PRIVATE KEY-----" in raw_bytes:
        try:
            from cryptography.hazmat.primitives import serialization
        except ImportError:
            raise ImportError("解析 OpenSSH 格式私钥需要 cryptography 库，请先安装。")
            
        try:
            priv_key = serialization.load_ssh_private_key(raw_bytes, password=None)
        except ValueError:
            priv_key = serialization.load_pem_private_key(raw_bytes, password=None)
            
        return priv_key.private_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PrivateFormat.TraditionalOpenSSL, # 强制转为兼容性极佳的 SEC1
            encryption_algorithm=serialization.NoEncryption()
        )
        
    return raw_bytes # 默认兜底返回

def get_standard_public_pem_bytes(key_input) -> bytes:
    """
    统一公钥解析：支持 OpenSSH 公钥、PEM 公钥、文件路径、bytes/str。
    最终返回 ecdsa 库兼容的标准 PEM 公钥字节流。
    """
    if not key_input:
        return None

    raw_bytes = b""
    if isinstance(key_input, str):
        if os.path.isfile(key_input):
            with open(key_input, "rb") as f:
                raw_bytes = f.read()
        else:
            raw_bytes = key_input.encode('utf-8')
    elif isinstance(key_input, bytes):
        raw_bytes = key_input

    if not raw_bytes:
        raise ValueError("无法解析传入的公钥")

    # 已经是 PEM 格式
    if b"-----BEGIN PUBLIC KEY-----" in raw_bytes:
        return raw_bytes

    # 尝试作为 OpenSSH 公钥解析。cryptography 是可选依赖，因此这里保留
    # 一个仅依赖 ecdsa 的解析路径。
    if raw_bytes.startswith(b"ecdsa-sha2-nistp256 "):
        try:
            encoded_key = raw_bytes.split(None, 2)[1]
            key_blob = base64.b64decode(encoded_key, validate=True)
            offset = 0

            def read_ssh_field():
                nonlocal offset
                if offset + 4 > len(key_blob):
                    raise ValueError("OpenSSH 公钥字段长度无效")
                field_length = struct.unpack(">I", key_blob[offset:offset + 4])[0]
                offset += 4
                field = key_blob[offset:offset + field_length]
                if len(field) != field_length:
                    raise ValueError("OpenSSH 公钥字段被截断")
                offset += field_length
                return field

            key_type = read_ssh_field()
            curve_name = read_ssh_field()
            point = read_ssh_field()
            if key_type != b"ecdsa-sha2-nistp256" or curve_name != b"nistp256":
                raise ValueError("仅支持 ecdsa-sha2-nistp256 公钥")
            return ecdsa.VerifyingKey.from_string(
                point, curve=ecdsa.NIST256p
            ).to_pem()
        except (ValueError, IndexError, TypeError, base64.binascii.Error) as error:
            raise ValueError(f"无法解析 OpenSSH 公钥: {error}") from error

    # 尝试借助 cryptography 解析其它 OpenSSH 公钥格式
    try:
        from cryptography.hazmat.primitives import serialization
        public_key = serialization.load_ssh_public_key(raw_bytes)
        pem = public_key.public_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PublicFormat.SubjectPublicKeyInfo
        )
        return pem
    except Exception:
        # 如果解析失败，可能已经是其他格式，直接返回
        return raw_bytes

class MultiMQTTManager:
    # [微调] 增加了 server_public_key_bytes (服务端验签用) 和 client_private_key_bytes (客户端签名用)，默认均为 None
    def __init__(self, brokers=BROKER_LIST, log_messages=False, enable_crypto=False, server_public_key_bytes=None, client_private_key_bytes=None):
        self.brokers = brokers
        self.clients = {}
        self.log_messages = log_messages
        self.enable_crypto = enable_crypto  # 默认关闭加密
        self.server_public_key_bytes = get_standard_public_pem_bytes(server_public_key_bytes)
        self.client_private_key_bytes = get_standard_pem_bytes(client_private_key_bytes) # [接入解析]
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

                req_id = data.get("req_id")
                # 去重判定：首胜丢弃逻辑 网络层行为放最前 (无论是原生 req_id 还是附带签名的 req_id，直接全量存入 Cache 用于 30 秒内去重)
                if req_id and not self.dedup_cache.add_if_not_exists(req_id):
                    return
                
                
                
                # logger.info(f"""📩 收到消息 {data} 来自 {host}  {self.server_public_key_bytes}
                # {self.log_messages}  cb{self.message_callback}""")
                # --- [新增] ECDSA 验证防重放核心逻辑 ---
                # 只有当用户启用了签名(传入了公钥) 并且当前数据是下发命令("code"存在)时，才触发验签
                if self.server_public_key_bytes and "code" in data:
                    if not req_id or "|" not in req_id:
                        logger.warning(f"⚠️ [{host}] 拒绝执行: 缺少 ECDSA 签名结构 (req_id格式不符)")
                        return
                    
                    # 剥离出真实的 req_id 和 签名Hex
                    base_req_id, sig_hex = req_id.rsplit("|", 1)
                    
                    # 验证1: 时间戳过期检测 (防超过 30s 的绝对重放)
                    msg_ts = int(data.get("timestamp", 0))
                    now_ms = utc_ms()
                    ttl_ms = self.dedup_cache.ttl * 1000
                    if abs(now_ms - msg_ts) > ttl_ms:
                        logger.warning(f"⚠️ [{host}] 拒绝执行: 消息时间戳已过期，拦截防重放 {now_ms} {msg_ts} {ttl_ms}")
                        return
                    
                    # 验证2: ECDSA 签名防篡改 (联合 hash 校验 base_req_id, code, timestamp)
                    code_str = str(data.get("code", ""))
                    ts_str = str(data.get("timestamp", ""))
                    sign_msg = f"{base_req_id}|{code_str}|{ts_str}".encode('utf-8')
                    
                    try:
                        vk = ecdsa.VerifyingKey.from_pem(self.server_public_key_bytes)
                        vk.verify(bytes.fromhex(sig_hex), sign_msg, hashfunc=hashlib.sha256)
                    except Exception:
                        logger.warning(f"⚠️ [{host}] 拒绝执行: ECDSA 签名无效")
                        return
                # ----------------------------------------


                if self.log_messages:
                    logger.info(f"📩 收到消息 [{msg.topic}] 来自 {host}")

                if self.message_callback:
                    if (self.server_public_key_bytes or self.client_private_key_bytes) and '|' in req_id:
                        data["req_id"] = req_id.split('|')[0]  # client 发送经过签名后 ，收到自动去除返回
                    self.message_callback(msg.topic, data, host)
            except Exception:
                logger.exception("处理 MQTT 消息失败 [%s]", host)
        return on_message

    def subscribe(self, topic: str):
        with self.lock:
            self.subscribed_topics.add(topic)
            for host, c in self.clients.items():
                if c.is_connected():
                    c.subscribe(topic)

    def publish_broadcast(self, topic: str, payload_dict: dict, client_private_key_bytes=None):
        """广播传输消息"""
        
        # --- [新增] ECDSA 发起请求时自动签名逻辑 ---
        # 只有在启用了签名(传入了私钥)，且当前发送的数据包含 "code" 时，才附带签名
        
        # [接入解析] 取传入的私钥，若无则使用全局私钥，统一解析格式
        current_priv_key = get_standard_pem_bytes(client_private_key_bytes) if client_private_key_bytes else self.client_private_key_bytes
        
        if current_priv_key and "code" in payload_dict:
            assert "timestamp" in payload_dict
            # 防重放所需的必要字段 就2个 ，其实可以只要一个
            
            # if "timestamp" not in payload_dict:
                # payload_dict["timestamp"] = utc_ms()
            # if "req_id" not in payload_dict:
                # payload_dict["req_id"] = get_req_id()
                
            base_req_id = str(payload_dict["req_id"])
            code_str = str(payload_dict.get("code", ""))
            ts_str = str(payload_dict["timestamp"])
            
            # 生成防篡改签名字符串并 Hash
            sign_msg = f"{base_req_id}|{code_str}|{ts_str}".encode('utf-8')
            sk = ecdsa.SigningKey.from_pem(current_priv_key)
            signature = sk.sign(sign_msg, hashfunc=hashlib.sha256)
            
            # 隐写签名：直接将签名拼接到 req_id 字段尾部 (形如 '20260910...|abc123hex...')
            payload_dict["req_id"] = f"{base_req_id}|{signature.hex()}"
        # ------------------------------------------

        payload_str = process_cipher(payload_dict, decrypt=False, enabled=self.enable_crypto)
        for host, c in self.clients.items():
            if c.is_connected():
                c.publish(topic, payload_str, qos=0)

    def stop(self):
        for c in self.clients.values():
            c.loop_stop()
            c.disconnect()
        logger.info("所有 MQTT 连接已安全关闭")