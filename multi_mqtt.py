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

import ast
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
 Broker 节点                ┃     历史总 Msg / Topic 
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


def stime(ms=0, format='%Y-%m-%d__%H.%M.%S', ms_splitor='__.'):
    """可读毫秒级时间戳。ms 传整数毫秒；不传则取当前 UTC 毫秒。"""
    if not ms:
        ms = utc_ms()
    elif isinstance(ms, float) and ms < 1e11:
        # 看起来是 time.time() 的秒级浮点，转成整数毫秒
        ms = round(ms * 1000)
    else:
        ms = int(ms)
    sec, milli = divmod(ms, 1000)
    return time.strftime(format, time.localtime(sec)) + ms_splitor + f"{milli:03d}"

def utc_ms():
    """Return the current UTC Unix timestamp in integer milliseconds."""
    return time.time_ns() // 1_000_000

def get_req_id(ms=0):
    """ get_req_id 的返回值只精确到了毫秒，如果单片机在同一毫秒内连发两条不同状态，后一条会被 TTLCache 误杀丢弃。
这是设计好行为。我不需要1毫秒发两个请求
    """
    # hash_str = hashlib.md5(f"{time.time()}_{random.random()}".encode()).hexdigest()[:6]
    return f"{stime(ms=ms,format='%Y%m%d_%H%M%S',ms_splitor='.')}"

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

def _eval_safe_int_expression(expr):
    """Safely evaluate a small Python integer expression used as a secret exponent."""
    if expr is None:
        return None
    if isinstance(expr, int):
        return expr
    if not isinstance(expr, str):
        raise ValueError("private key expression must be a string or int")

    expr = expr.strip()
    if not expr:
        return None

    try:
        node = ast.parse(expr, mode="eval")
    except SyntaxError as exc:
        raise ValueError(f"无法解析私钥表达式: {expr}") from exc

    allowed_nodes = (
        ast.Expression,
        ast.BinOp,
        ast.UnaryOp,
        ast.Constant,
        ast.Add,
        ast.Sub,
        ast.Mult,
        ast.Div,
        ast.FloorDiv,
        ast.Mod,
        ast.Pow,
        ast.UAdd,
        ast.USub,
    )

    def _eval(node):
        if not isinstance(node, allowed_nodes):
            raise ValueError(f"不允许的私钥表达式节点: {type(node).__name__}")
        if isinstance(node, ast.Constant):
            if isinstance(node.value, int) and not isinstance(node.value, bool):
                return node.value
            raise ValueError("私钥表达式必须计算出整数")
        if isinstance(node, ast.UnaryOp):
            operand = _eval(node.operand)
            if isinstance(node.op, ast.UAdd):
                return +operand
            if isinstance(node.op, ast.USub):
                return -operand
            raise ValueError("不支持的一元运算")
        if isinstance(node, ast.BinOp):
            left = _eval(node.left)
            right = _eval(node.right)
            if isinstance(node.op, ast.Add):
                return left + right
            if isinstance(node.op, ast.Sub):
                return left - right
            if isinstance(node.op, ast.Mult):
                return left * right
            if isinstance(node.op, ast.Div):
                return left // right
            if isinstance(node.op, ast.FloorDiv):
                return left // right
            if isinstance(node.op, ast.Mod):
                return left % right
            if isinstance(node.op, ast.Pow):
                return left ** right
            raise ValueError("不支持的二元运算")
        if isinstance(node, ast.Expression):
            return _eval(node.body)
        raise ValueError("无法计算私钥表达式")

    value = _eval(node)
    if not isinstance(value, int):
        raise ValueError("私钥表达式必须结果为整数")
    return value


def get_standard_pem_bytes(key_input) -> bytes:
    """
    [新增组件] 统一私钥解析：支持 整数secexp、Python 整数表达式、文件路径、OpenSSH格式、标准PEM格式(bytes/str)
    最终统一返回 ecdsa 库直接兼容的标准 PEM (SEC1) 字节流。
    """
    if not key_input:
        return None

    # 1. 整数或安全 Python 整数表达式当作 secexp 处理
    if isinstance(key_input, int):
        secexp = key_input
        return ecdsa.SigningKey.from_secret_exponent(secexp=secexp, curve=ecdsa.NIST256p).to_pem()
    if isinstance(key_input, str):
        text = key_input.strip()
        if text.isdigit() or text.startswith(("0x", "0X")) or any(ch in text for ch in "+-*/%**() "):
            try:
                secexp = _eval_safe_int_expression(text)
                return ecdsa.SigningKey.from_secret_exponent(secexp=secexp, curve=ecdsa.NIST256p).to_pem()
            except ValueError:
                pass

    # 2. 如果是文件路径，读取内容；否则转为 bytes
    raw_bytes = b""
    if isinstance(key_input, str):
        if not key_input.startswith("-----") and len(key_input) < 255 and os.path.isfile(key_input):
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
            format=serialization.PrivateFormat.TraditionalOpenSSL,
            encryption_algorithm=serialization.NoEncryption()
        )

    return raw_bytes # 默认兜底返回

def _describe_public_key(value: bytes | str | None) -> str:
    """返回可直接写入日志的公钥摘要与详细值。"""
    if not value:
        return "未配置"
    try:
        data = value if isinstance(value, (bytes, bytearray)) else str(value).encode('utf-8')
        if not data:
            return "空值"
        text = data.decode('utf-8', 'replace').strip()
        if b"BEGIN PUBLIC KEY" in data or b"BEGIN EC PUBLIC KEY" in data:
            return f"已配置(type=PEM, len={len(data)}, value={text})"
        if b"ecdsa-sha2-nistp256" in data:
            return f"已配置(type=OpenSSH, len={len(data)}, value={text})"
        return f"已配置(type=unknown, len={len(data)}, value={text})"
    except Exception:
        return f"已配置(len={len(str(value))}, value={str(value)})"


def get_standard_public_pem_bytes(key_input) -> bytes:
    """
    统一公钥解析：支持 OpenSSH 公钥、PEM 公钥、文件路径、bytes/str。
    最终返回 ecdsa 库兼容的标准 PEM 公钥字节流。
    """
    if not key_input:
        return None

    raw_bytes = b""
    if isinstance(key_input, str):
        if not key_input.startswith("-----") and len(key_input) < 255 and os.path.isfile(key_input):
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

# =========================================================================
# [新增解耦功能] 网络质量统计模型与管理模块（默认不占额外内存，极其轻量）
# =========================================================================
class BrokerStat:
    """单一节点的数据模型（完全摒弃历史数组，保证O(1)极低内存开销）"""
    def __init__(self):
        self.disconnect_count = 0
        self.last_disconnect_time = 0.0
        self.last_connect_time = 0.0
        self.max_offline_time = 0.0
        self.total_online_time = 0.0
        self.total_offline_time = 0.0
        self.is_connected = False
        
        # 延迟指标滚动记录
        self.latency_min = float('inf')
        self.latency_max = 0.0
        self.latency_sum = 0.0
        self.latency_count = 0
        
        # 发送 QoS1 Ping 时的上下文记录
        self.pending_ping_mid = None
        self.pending_ping_time = 0.0
        self.created_at = time.time()

    def update_latency(self, latency_ms):
        if latency_ms < self.latency_min: self.latency_min = latency_ms
        if latency_ms > self.latency_max: self.latency_max = latency_ms
        self.latency_sum += latency_ms
        self.latency_count += 1
        
    @property
    def avg_latency(self):
        return self.latency_sum / self.latency_count if self.latency_count > 0 else 0.0
        
    @property
    def reliability(self):
        now = time.time()
        online = self.total_online_time
        offline = self.total_offline_time
        
        if self.is_connected:
            if self.last_connect_time > 0:
                online += (now - self.last_connect_time)
        else:
            if self.last_disconnect_time > 0:
                offline += (now - self.last_disconnect_time)
            elif self.created_at > 0:
                offline += (now - self.created_at)
        
        total = online + offline
        return (online / total * 100) if total > 0 else 0.0


class ConnectionQualityStats:
    """网络连接质量统筹管理器，可方便开启与关闭"""
    def __init__(self, enabled=True, print_interval=3600):
        self.enabled = enabled
        self.stats = {}
        self.lock = threading.Lock()
        self.print_interval = print_interval
        self.running = False
        self.thread = None
        self.clients_ref = {}

    def start(self, clients_ref):
        if not self.enabled: return
        self.clients_ref = clients_ref
        self.running = True
        for host in self.clients_ref.keys():
            self.stats[host] = BrokerStat()
        
        self.thread = threading.Thread(target=self._monitor_loop, daemon=True, name="StatsPingThread")
        self.thread.start()

    def stop(self):
        self.running = False
        
    def on_connect(self, host):
        if not self.enabled: return
        with self.lock:
            stat = self.stats.setdefault(host, BrokerStat())
            now = time.time()
            
            # [修复盲区]: 只有之前是断开状态，才去结算离线时间，防止重连过程重复计算
            if not stat.is_connected:
                if stat.last_disconnect_time > 0:
                    offline_duration = now - stat.last_disconnect_time
                    stat.total_offline_time += offline_duration
                    if offline_duration > stat.max_offline_time:
                        stat.max_offline_time = offline_duration
                elif stat.created_at > 0:
                    offline_duration = now - stat.created_at
                    stat.total_offline_time += offline_duration
                    if offline_duration > stat.max_offline_time:
                        stat.max_offline_time = offline_duration

            stat.is_connected = True
            stat.last_connect_time = now

    def on_disconnect(self, host):
        if not self.enabled: return
        with self.lock:
            stat = self.stats.setdefault(host, BrokerStat())
            now = time.time()
            
            if stat.is_connected:
                # 正常从在线变为离线：结算在线时间，必然增加1次掉线
                if stat.last_connect_time > 0:
                    stat.total_online_time += (now - stat.last_connect_time)
                stat.is_connected = False
                stat.disconnect_count += 1
            else:
                # [关键修复]: 初始连接失败或持续重连失败期间，is_connected 为 False。
                # 为了既能记录掉线次数，又防止底层高频重连导致 Drops 狂刷，加入1秒的防抖机制。
                # 只要是第一次失败，或者距离上次断开超过 1 秒，就记录为一次掉线。
                if stat.last_disconnect_time == 0 or (now - stat.last_disconnect_time) > 1.0:
                    stat.disconnect_count += 1    
            stat.last_disconnect_time = now
            
    def record_ping_send(self, host, mid):
        if not self.enabled: return
        with self.lock:
            stat = self.stats.setdefault(host, BrokerStat())
            stat.pending_ping_mid = mid
            stat.pending_ping_time = time.time()

    def on_publish_ack(self, host, mid):
        """挂钩到底层 on_publish 回调计算 QoS 1 的精准 RTT 延迟"""
        if not self.enabled: return
        with self.lock:
            stat = self.stats.get(host)
            if stat and stat.pending_ping_mid == mid:
                latency_ms = (time.time() - stat.pending_ping_time) * 1000
                if latency_ms < 10000: # 剔除由于断线堆积重发导致的超长异常延迟(>10s)
                    stat.update_latency(latency_ms)
                stat.pending_ping_mid = None
                
    def get_report(self, sort="rel", reverse=True):
        """
        获取网络连接质量统计报告

        统一指标名（排序键 / 数据键 / 表头 完全一致，全部小写）：
            broker    - Broker 地址
            rel       - 可靠性(%)
            avg       - 平均延迟(ms)
            min       - 最小延迟(ms)
            max       - 最大延迟(ms)
            drops     - 掉线次数
            max_off   - 最大离线时长(s)
            last_drop - 最后一次掉线时间

        :param sort: 排序指标，取值 broker / rel / avg / min / max / drops / max_off
        :param reverse: 是否降序排列（默认 True）
        """
        # 表头列名与内部指标名完全一致
        columns = ["broker", "rel", "avg", "min", "max", "drops", "max_off", "last_drop"]

        lines = ["\n" + "=" * 90]
        lines.append(
            f"{'broker':<30} | {'rel':>6} | {'avg':>7} | {'min':>5} | "
            f"{'max':>5} | {'drops':>5} | {'max_off':>9} | {'last_drop':>10}"
        )
        lines.append("-" * 90)

        with self.lock:
            current_time = time.time()
            display_stats = []

            # 1. 数据预处理：用统一指标名构造字典
            for host, stat in self.stats.items():
                is_conn = stat.is_connected
                max_off = stat.max_offline_time
                total_off = getattr(stat, "total_offline_time", 0.0)

                # 当前处于断线状态时，把正在发生的离线时间并入
                if not is_conn and stat.last_disconnect_time > 0:
                    current_off_duration = current_time - stat.last_disconnect_time
                    max_off = max(max_off, current_off_duration)
                    total_off += current_off_duration

                # 动态计算最新可靠性
                total_time = current_time - stat.created_at
                if total_time > 0:
                    rel = max(0.0, 100.0 * (1.0 - (total_off / total_time)))
                else:
                    rel = 100.0

                display_stats.append({
                    "broker":    host,
                    "rel":       rel,
                    "avg":       stat.avg_latency if stat.latency_count > 0 else -1.0,
                    "min":       stat.latency_min if stat.latency_count > 0 else -1.0,
                    "max":       stat.latency_max if stat.latency_count > 0 else -1.0,
                    "drops":     stat.disconnect_count,
                    "max_off":   max_off,
                    "last_drop": stat.last_disconnect_time,
                    "is_conn":   is_conn,
                })

            # 2. 排序逻辑：排序键名与数据键名一致
            def sort_key(item):
                k = sort.lower()
                if k not in columns:
                    k = "rel"
                # 延迟类指标无数据(-1.0)时，让其排在末尾
                if k in ("avg", "min", "max"):
                    v = item[k]
                    if v < 0:
                        return float("-inf") if reverse else float("inf")
                    return v
                return item[k]

            # 多级排序：主指标 -> 平均延迟 -> broker 名
            sorted_stats = sorted(
                display_stats,
                key=lambda x: (sort_key(x), -x["avg"], x["broker"]),
                reverse=reverse,
            )

            # 3. 渲染输出：列名与数据键名一致
            for item in sorted_stats:
                rel_str = f"{item['rel']:.1f}"
                avg_str = f"{item['avg']:.1f}" if item["avg"] >= 0 else "-"
                min_str = f"{item['min']:.1f}" if item["min"] >= 0 else "-"
                max_str = f"{item['max']:.1f}" if item["max"] >= 0 else "-"
                drops_str = str(item["drops"])
                max_off_str = f"{item['max_off']:.1f}"

                if item["last_drop"] > 0:
                    last_drop_str = time.strftime(
                        "%H:%M:%S", time.localtime(item["last_drop"])
                    )
                else:
                    last_drop_str = "-"

                status_marker = "🟢" if item["is_conn"] else "🔴"
                row = (
                    f"{status_marker} {item['broker']:<28} | "
                    f"{rel_str:>6} | {avg_str:>7} | {min_str:>5} | {max_str:>5} | "
                    f"{drops_str:>5} | {max_off_str:>9} | {last_drop_str:>10}"
                )
                lines.append(row)

        lines.append("=" * 90)
        return "\n".join(lines)
        
    def _monitor_loop(self):
        """修复了原有的睡眠阻塞逻辑，通过秒级步进分开判断统计报告与Ping的时机"""
        sleep_step = 6
        ping_interval = 10 * 60  # 秒 发起一次极小代价的 Ping
        
        ping_counter = ping_interval # 启动时先立刻测一次
        print_counter = 0
        
        while self.running:
            time.sleep(sleep_step)
            ping_counter += sleep_step
            print_counter += sleep_step
            
            # 1. 检测是否需要发送 PING 指令来测距
            if ping_counter >= ping_interval:
                ping_counter = 0
                for host, client in self.clients_ref.items():
                    stat = self.stats.get(host)
                    if stat and stat.is_connected and client.is_connected():
                        try:
                            # 使用 QoS 1 发布空载荷到隔离 topic 测试 RTT
                            # 发送成功会触发 on_publish 抛出 PUBACK 进行秒表停止
                            msg_info = client.publish(f"multi_mqtt/ping_rtt/{host}", b"", qos=1)
                            self.record_ping_send(host, msg_info.mid)
                        except Exception:
                            pass
                        
            # 2. 检测是否需要打印输出报告（不再依赖 ping_interval 被阻塞）
            if self.print_interval > 0 and print_counter >= self.print_interval:
                print_counter = 0
                logger.info("📡 [连接质量统计报告]" + self.get_report())


# =========================================================================

class MultiMQTTManager:
    # [微调] 增加了 server_public_key_bytes, client_private_key_bytes 以及连接统计参数 (enable_stats/log_connection)
    # [新增] keepalive 与 max_reconnect_delay 参数
    def __init__(self, brokers=BROKER_LIST, log_messages=False, enable_crypto=False, server_public_key_bytes=None, client_private_key_bytes=None, enable_stats=True, log_connection=None, keepalive=60, max_reconnect_delay=3600):
        self.brokers = brokers
        self.keepalive = keepalive
        self.max_reconnect_delay = max_reconnect_delay
        self.clients = {}
        self.log_messages = log_messages
        self.enable_crypto = enable_crypto  # 默认关闭加密
        self.server_public_key_bytes = get_standard_public_pem_bytes(server_public_key_bytes)
        self.client_private_key_bytes = get_standard_pem_bytes(client_private_key_bytes) # [接入解析]
        
        # --- [统计功能新增] ---
        self.enable_stats = enable_stats
        # 如果没有显式指定，当开启统计时自动把底层刷屏连接日志关掉
        self.log_connection = log_connection if log_connection is not None else not enable_stats
        self.stats = ConnectionQualityStats(enabled=self.enable_stats)
        # ----------------------
        
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

            # [修复思路执行]: 利用 userdata 完美隔离上下文，完全抛弃闭包
            client.user_data_set(host)

            # 开启自动重连退避策略，将 max_delay 拉长至 max_reconnect_delay (默认1小时) 防止重连风暴发热
            client.reconnect_delay_set(min_delay=1, max_delay=self.max_reconnect_delay)

            # 统一绑定类方法，不再动态创建工厂函数
            client.on_connect = self._on_connect
            client.on_disconnect = self._on_disconnect
            client.on_message = self._on_message
            client.on_publish = self._on_publish

            try:
                # 传入 keepalive 参数让底层操作系统来维持 TCP 连接 (默认60s)
                client.connect_async(host, port, keepalive=self.keepalive)
                client.loop_start()
                self.clients[host] = client
                if self.log_connection:
                    logger.info(f"开启后台连接任务 -> {host}:{port}")
            except Exception as e:
                logger.error(f"连接初始化失败 [{host}]: {e}")
                
        # 启动质量统计模块
        if self.enable_stats:
            self.stats.start(self.clients)

    # ================= [基于 Userdata 的统一回调] =================
    def _on_connect(self, client, userdata, flags, rc, properties=None):
        host = userdata  # 优雅获取隔离的独立上下文
        if rc == 0:
            if self.log_connection:
                logger.info(f"✅ [已连接] Broker: {host}")
            self.stats.on_connect(host)  # [接入统计]
            with self.lock:
                for topic in self.subscribed_topics:
                    client.subscribe(topic)
        else:
            if self.log_connection:
                logger.warning(f"❌ [连接失败] Broker: {host}, rc={rc}")
            # [关键修复]: rc != 0 代表 MQTT 协议层拒绝，主动触发一次掉线统计
            self.stats.on_disconnect(host)

    def _on_disconnect(self, client, userdata, flags, rc, properties=None):
        host = userdata
        self.stats.on_disconnect(host)  # [接入统计]
        if rc != 0:
            if self.log_connection:
                logger.warning(f"⚠️ [意外断开] Broker: {host} (rc={rc})，自动尝试重连...")

    def _on_publish(self, client, userdata, mid, *args, **kwargs):
        host = userdata
        if self.enable_stats:
            self.stats.on_publish_ack(host, mid)

    def _on_message(self, client, userdata, msg):
        host = userdata
        try:
            raw_payload = msg.payload.decode('utf-8')
            data = process_cipher(raw_payload, decrypt=True, enabled=self.enable_crypto)

            req_id = data.get("req_id")
            # 去重判定：首胜丢弃逻辑 网络层行为放最前  去掉绝对时间戳校验，只依赖 req_id + TTLCache 防重放
            if req_id and not self.dedup_cache.add_if_not_exists(req_id):
                return
            
            # --- [新增] ECDSA 验证防重放核心逻辑 ---
            # 只有当用户启用了签名(传入了公钥) 并且当前数据是下发命令("code"存在)时，才触发验签
            if "code" in data:
                if not self.server_public_key_bytes:
                    logger.debug(
                                "ℹ️ [%s] 服务器未配置公钥，跳过验签检查。req_id=%s | has_code=%s",
                                host,req_id,("code" in data),  )
                else:
                    if not req_id or "|" not in req_id:
                        logger.warning(f"⚠️ [{host}] 拒绝执行: 缺少 ECDSA 签名结构 (req_id格式不符) | server_pubkey={_describe_public_key(self.server_public_key_bytes)}")
                        return

                    base_req_id, sig_hex = req_id.rsplit("|", 1)
                    logger.info(
                        "🔑 [%s] 请求已签名，开始验签: req_id=%s | base_req_id=%s | signature_len=%d | server_pubkey=%s",
                        host,
                        req_id,
                        base_req_id,
                        len(sig_hex),
                        _describe_public_key(self.server_public_key_bytes),
                    )

                    # msg_ts = int(data.get("timestamp", 0))
                    # now_ms = utc_ms()
                    # ttl_ms = self.dedup_cache.ttl * 1000
                    # if abs(now_ms - msg_ts) > ttl_ms:
                        # logger.warning(f"⚠️ [{host}] 拒绝执行: 消息时间戳已过期，拦截防重放 {now_ms} {msg_ts} {ttl_ms}")
                        # return

                    code_str = str(data.get("code", ""))
                    ts_str = str(data.get("timestamp", ""))
                    sign_msg = f"{base_req_id}|{code_str}|{ts_str}".encode('utf-8')

                    try:
                        vk = ecdsa.VerifyingKey.from_pem(self.server_public_key_bytes)
                        vk.verify(bytes.fromhex(sig_hex), sign_msg, hashfunc=hashlib.sha256)
                        logger.info("✅ [%s] ECDSA 验签成功，允许执行: req_id=%s", host, base_req_id)
                    except Exception:
                        logger.warning(f"⚠️ [{host}] 拒绝执行: ECDSA 签名无效 | req_id={req_id} | server_pubkey={_describe_public_key(self.server_public_key_bytes)}")
                        return
            
            # ----------------------------------------

            if self.log_messages:
                logger.info(f"📩 收到消息 [{msg.topic}] 来自 {host}")

            if self.message_callback:
                # 只在真正的签名校验路径中才剥离 req_id 的签名尾巴。
                # 对于普通回包，req_id 的形态本身就能表示“无公钥服务端原样返回了签名参数”或“可信服务端已去签名返回”。
                self.message_callback(msg.topic, data, host)
        except Exception:
            logger.exception("处理 MQTT 消息失败 [%s]", host)

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
        if self.enable_stats:
            self.stats.stop()
        for c in self.clients.values():
            c.loop_stop()
            c.disconnect()
        logger.info("所有 MQTT 连接已安全关闭")