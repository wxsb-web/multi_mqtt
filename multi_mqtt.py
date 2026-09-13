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

import ast,json,time,os,uuid,hashlib,random,logging,base64,struct,threading,queue
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
    
    if ms_splitor:ms_splitor=ms_splitor + f"{milli:03d}"
    return time.strftime(format, time.localtime(sec)) + ms_splitor

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
    # [轻量化] __slots__ 避免每个实例再挂一份 __dict__，配合"极其轻量"的设计目标
    __slots__ = (
        "disconnect_count", "last_disconnect_time", "last_connect_time",
        "max_offline_time", "total_online_time", "total_offline_time",
        "is_connected", "latency_min", "latency_max", "latency_sum",
        "latency_count", "pending_pings", "created_at",
    )

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
        # [修复-M4] 发送 QoS1 Ping 时的上下文记录。
        # 用 dict[mid] = send_time 保存"所有未确认的 ping"，
        # 而不是单一 mid/time 字段——避免周期重叠时相互覆盖。
        self.pending_pings = {}
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
        """
        可靠性(%)：统一口径为「1 - 累计离线时长 / 自创建以来总时长」。
        本属性是唯一口径，get_report 直接复用，避免双份实现漂移。

        [修复-⑤] 若从未成功连接过（last_connect_time==0 且当前未连接），
        直接返回 0.0，避免启动初期因 total_offline_time 尚未滚动累加，
        短暂误报为 100% 的极端盲区。
        """
        now = time.time()
        total = now - self.created_at
        if total <= 0:
            # 极端情况（时钟回拨 / 刚创建）：退化为二值判断
            return 100.0 if self.is_connected else 0.0
        # 从未成功连上过 → 可靠性就是 0%
        if self.last_connect_time == 0 and not self.is_connected:
            return 0.0
        offline = self.total_offline_time
        # 当前仍处于离线：把"正在发生的离线"也计入
        if not self.is_connected and self.last_disconnect_time > 0:
            offline += (now - self.last_disconnect_time)
        rel = 100.0 * (1.0 - (offline / total))
        return max(0.0, min(100.0, rel))


class ConnectionQualityStats:
    """网络连接质量统筹管理器，可方便开启与关闭"""
    # [可配置] Ping 周期（秒）。原代码硬编码在 _monitor_loop 里，
    # 现抽出为类属性：保留默认 600，也不影响 __init__ 签名（便于 diff）。
    PING_INTERVAL = 600
    # [修复-M4] 单条 ping 记录的最长存活时间：超过该时长仍未收到 ACK，
    # 认为该 ping 已经彻底丢失（QoS1 重传也救不回来），从待确认表中清除，
    # 防止 pending_pings 因极端网络状况无限膨胀。
    PING_TTL = 120

    # [修复-N1] __init__ 新增 lock 参数：
    #   - 传 None（默认）→ 本类自建一把锁，用于独立使用场景；
    #   - 由 MultiMQTTManager 传入 → 与 manager 共享同一把锁，
    #     保证 self.clients_ref（就是 manager.clients 同一个 dict）
    #     的读取与 manager 的 clients 写入使用同一把锁，彻底消除跨锁竞态。
    def __init__(self, enabled=True, print_interval=3600, lock=None):
        self.enabled = enabled
        self.stats = {}
        self.lock = lock if lock is not None else threading.Lock()
        self.print_interval = print_interval
        self.running = False
        self.thread = None
        self.clients_ref = {}

    def start(self, clients_ref):
        if not self.enabled: return
        with self.lock:
            if self.running:
                return  # 幂等保护：防止重复 start 导致多份监控线程
            self.clients_ref = clients_ref
            for host in self.clients_ref.keys():
                # [修复-S2] 用 setdefault 而不是直接赋值。
                # MultiMQTTManager.start() 里 client.loop_start() 是非阻塞的，
                # 极快的 broker（如 broker.codenow.cn 建连仅 110ms）可能在
                # stats.start() 之前就触发 _on_connect 创建好 BrokerStat，
                # 直接赋值会把这份"已连接"状态用全新的未连接对象覆盖，
                # 导致初次连接的状态被静默丢弃。
                self.stats.setdefault(host, BrokerStat())
            self.running = True
        self.thread = threading.Thread(target=self._monitor_loop, daemon=True, name="StatsPingThread")
        self.thread.start()

    def stop(self):
        self.running = False
        # [稳定性] 等待线程真正退出，避免关闭后仍访问已释放资源
        t = self.thread
        if t and t.is_alive():
            t.join(timeout=10.0)
        self.thread = None

    def on_connect(self, host):
        if not self.enabled: return
        with self.lock:
            stat = self.stats.setdefault(host, BrokerStat())
            now = time.time()
            # [修复首连算入离线时长的Bug]: 仅在有过真实掉线记录(last_disconnect_time > 0)时才结算离线时长
            if not stat.is_connected:
                if stat.last_disconnect_time > 0:
                    offline_duration = now - stat.last_disconnect_time
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
                # 初始连接失败或持续重连失败期间，防抖机制记录掉线
                if stat.last_disconnect_time == 0 or (now - stat.last_disconnect_time) > 1.0:
                    stat.disconnect_count += 1
            stat.last_disconnect_time = now

    def finalize_for_shutdown(self, host):
        """
        [修复-④] 服务终止时为指定 host 结算"最后一段在线时长"。
        Manager.stop() 会触发 paho 的 on_disconnect，但那属于"主动停止"：
        - 结算 total_online_time，避免丢失最后一段真实在线时长
        - 不增加 disconnect_count（主动停止 ≠ 掉线）
        """
        if not self.enabled: return
        with self.lock:
            stat = self.stats.get(host)
            if stat and stat.is_connected:
                now = time.time()
                if stat.last_connect_time > 0:
                    stat.total_online_time += (now - stat.last_connect_time)
                stat.is_connected = False
                stat.last_disconnect_time = now

    def record_ping_send(self, host, mid):
        if not self.enabled: return
        with self.lock:
            stat = self.stats.setdefault(host, BrokerStat())
            now = time.time()
            # [修复-M4] 按 mid 写入待确认表，多个 in-flight ping 互不干扰。
            # 之前的单一字段在周期重叠时会相互覆盖，导致先发 ping 的延迟样本丢失。
            stat.pending_pings[mid] = now
            # 顺手清理超时未确认的旧条目，防止极端网络下 dict 无限增长
            if len(stat.pending_pings) > 1:
                expired = [m for m, t in stat.pending_pings.items() if now - t > self.PING_TTL]
                for m in expired:
                    stat.pending_pings.pop(m, None)

    def on_publish_ack(self, host, mid):
        """挂钩到底层 on_publish 回调计算 QoS 1 的精准 RTT 延迟"""
        if not self.enabled: return
        with self.lock:
            stat = self.stats.get(host)
            if not stat: return
            # [修复-M4] 按 mid 精确匹配并原子弹出：
            # - 命中：计算本次延迟后即刻删除，避免同一 mid 被重复计入
            # - 未命中：说明该 ACK 不是我们发的 ping（例如业务消息的 QoS1 ACK），直接忽略
            sent_at = stat.pending_pings.pop(mid, None)
            if sent_at is None:
                return
            latency_ms = (time.time() - sent_at) * 1000.0
            if 0.0 <= latency_ms < 10000.0:  # 剔除由于断线堆积重发导致的超长异常延迟(>10s)
                stat.update_latency(latency_ms)

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
        columns = ["broker", "rel", "avg", "min", "max", "drops", "max_off", "last_drop"]
        lines = ["\n" + "=" * 90]
        lines.append(
            f"{'broker':<30} | {'rel':>6} | {'avg':>7} | {'min':>5} | "
            f"{'max':>5} | {'drops':>5} | {'max_off':>9} | {'last_drop':>10}"
        )
        lines.append("-" * 90)
        with self.lock:
            display_stats = []
            # 1. 数据预处理
            for host, stat in self.stats.items():
                is_conn = stat.is_connected
                rel = stat.reliability  # [统一口径] 复用 BrokerStat 的属性，避免双份实现漂移
                # max_off 额外把"当前正在发生的离线"并入展示
                max_off = stat.max_offline_time
                if not is_conn and stat.last_disconnect_time > 0:
                    max_off = max(max_off, time.time() - stat.last_disconnect_time)
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
            # 2. [修复排序Bug]: 修正无延迟数据(-1.0)在多级排序下被排在前面的问题
            def sort_key(item):
                k = sort.lower()
                if k not in columns:
                    k = "rel"
                val = item[k]
                # 主指标无数据处理：始终沉底
                if k in ("avg", "min", "max") and val < 0:
                    primary_val = float("-inf") if reverse else float("inf")
                else:
                    primary_val = val
                # 二级指标 avg 延迟处理（无数据时始终沉底）
                avg_val = item["avg"]
                if avg_val < 0:
                    secondary_val = float("-inf") if reverse else float("inf")
                else:
                    secondary_val = -avg_val if reverse else avg_val
                # 末级：已连接优先
                conn_rank = 0 if item["is_conn"] else 1
                return (primary_val, secondary_val, conn_rank, item["broker"])
            sorted_stats = sorted(display_stats, key=sort_key, reverse=reverse)
            # 3. 渲染输出
            for item in sorted_stats:
                rel_str = f"{item['rel']:.1f}"
                avg_str = f"{item['avg']:.1f}" if item["avg"] >= 0 else "-"
                min_str = f"{item['min']:.1f}" if item["min"] >= 0 else "-"
                max_str = f"{item['max']:.1f}" if item["max"] >= 0 else "-"
                drops_str = str(item["drops"])
                max_off_str = f"{item['max_off']:.1f}"
                if item["last_drop"] > 0:
                    last_drop_str = time.strftime("%H:%M:%S", time.localtime(item["last_drop"]))
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
        ping_interval = self.PING_INTERVAL   # [可配置] 使用类属性，保留默认 10 分钟
        # [稳定性] 使用绝对时间戳驱动，避免 sleep 漂移导致周期累积误差
        now = time.time()
        next_ping_at = now                    # 启动后立刻先测一次
        next_print_at = (now + self.print_interval) if self.print_interval > 0 else None
        while self.running:
            time.sleep(sleep_step)
            if not self.running:
                break
            now = time.time()
            # 1. 检测是否需要发送 PING 指令来测距
            if now >= next_ping_at:
                next_ping_at = now + ping_interval
                self._send_pings()
            # 2. 检测是否需要打印输出报告
            if next_print_at is not None and now >= next_print_at:
                next_print_at = now + self.print_interval
                try:
                    logger.info("📡 [连接质量统计报告]" + self.get_report())
                except Exception:
                    logger.exception("生成连接质量统计报告失败")

    def _send_pings(self):
        """[原逻辑抽离] 遍历已连接的 broker 发送 QoS1 Ping，单点异常不互相影响"""
        # [修复-N1] 使用 self.lock（在 MultiMQTTManager 场景下就是 manager.lock），
        # 与 manager.stop() 里 `with self.lock: self.clients.clear()` 使用同一把锁。
        # 两边互斥，不会再出现"另一个线程正在 clear 时这边 list() 迭代"的 RuntimeError。
        with self.lock:
            clients_snapshot = list(self.clients_ref.items())
        for host, client in clients_snapshot:
            stat = self.stats.get(host)
            if not stat or not stat.is_connected:
                continue
            try:
                if not client.is_connected():
                    continue
                msg_info = client.publish(f"multi_mqtt/ping_rtt/{host}", b"", qos=1)
                if msg_info is not None and msg_info.mid is not None:
                    self.record_ping_send(host, msg_info.mid)
            except Exception:
                # 单点异常不影响其他 broker
                logger.debug("Ping 发送失败 [%s]", host, exc_info=True)


# =========================================================================
class MultiMQTTManager:
    # [修复-③] 消息分发队列上限：满则丢弃并告警，绝不阻塞 paho 网络线程
    MSG_QUEUE_MAXSIZE = 10000

    def __init__(self, brokers=BROKER_LIST, log_messages=False, enable_crypto=False, server_public_key_bytes=None, client_private_key_bytes=None, enable_stats=True, log_connection=None, keepalive=60, max_reconnect_delay=3600):
        self.brokers = brokers
        self.keepalive = keepalive
        self.max_reconnect_delay = max_reconnect_delay
        self.clients = {}
        self.log_messages = log_messages
        self.enable_crypto = enable_crypto  # 默认关闭加密
        self.server_public_key_bytes = get_standard_public_pem_bytes(server_public_key_bytes)
        self.client_private_key_bytes = get_standard_pem_bytes(client_private_key_bytes)
        # ------------------------------------------------------------------
        # [修复-⑥ 严重性能瓶颈] ECDSA 密钥对象仅在初始化时解析一次并缓存。
        # 原实现在 _on_message / publish_broadcast 里每次都调用 from_pem，
        # 而 PEM 解析属于 CPU-Bound 的椭圆曲线底层数学运算，一旦并发上来，
        # CPU 会被瞬间打满并引发严重延迟。这里只解析一次，后续直接复用对象。
        # ------------------------------------------------------------------
        # [修复-S3] 区分两种"没有 server_vk"的情况：
        #   (a) 从未配置公钥 → 跳过验签（向下兼容，允许无签名模式）
        #   (b) 配置了公钥但解析失败 → fail-closed，拒绝所有 code 请求
        # 用 _server_vk_invalid 标志区分，避免拼错 PEM 时静默失去验签能力。
        self._server_vk_invalid = False
        try:
            self.server_vk = (
                ecdsa.VerifyingKey.from_pem(self.server_public_key_bytes)
                if self.server_public_key_bytes else None
            )
        except Exception:
            logger.exception("[S3] 解析服务端公钥失败：所有带 code 的请求将被拒绝 (fail-closed)")
            self.server_vk = None
            if self.server_public_key_bytes:
                self._server_vk_invalid = True
        try:
            self.client_sk = (
                ecdsa.SigningKey.from_pem(self.client_private_key_bytes)
                if self.client_private_key_bytes else None
            )
        except Exception:
            logger.exception("解析客户端私钥失败，签名功能将被禁用")
            self.client_sk = None
        # ------------------------------------------------------------------
        # [修复-N1] 先创建统一锁，并把它传给 stats，两处共享同一把锁。
        # manager.clients 与 stats.clients_ref 是同一个 dict，
        # 使用同一把锁后，任何一方读写都会正确互斥，消除跨锁竞态。
        self.lock = threading.Lock()
        # --- [统计功能] ---
        self.enable_stats = enable_stats
        self.log_connection = log_connection if log_connection is not None else not enable_stats
        self.stats = ConnectionQualityStats(enabled=self.enable_stats, lock=self.lock)
        # ------------------
        self.dedup_cache = TTLCache(ttl_seconds=30)
        self.message_callback = None
        self.subscribed_topics = set()
        # [修复-N2/N3] 用 threading.Event 代替 _stopping 布尔标志：
        #   - _dispatch_loop 用它作为退出信号（不再依赖哨兵 None）；
        #   - _on_disconnect 用它判断"主动停止 / 意外断开"；
        #   - 停止后 start() 会 clear()，可安全复用于"停→启"场景。
        self._stop_event = threading.Event()
        # [修复-N4] 消息处理异常日志的限流状态：host -> (last_log_time, suppressed_count)
        self._msg_err_log_state = {}
        # [修复-③] 异步分发相关：业务回调不再运行在 paho 网络线程里
        # 注意：需确保文件顶部有 `import queue`
        self._msg_queue = queue.Queue(maxsize=self.MSG_QUEUE_MAXSIZE)
        self._dispatch_thread = None

    def set_on_message(self, callback):
        """设置上层回调，签名: fn(topic, data_dict, rx_broker)"""
        self.message_callback = callback

    # [修复-N4] 消息处理异常的限流日志：每 host 每秒最多 1 条。
    # 被抑制的条数会在下次放行时以"过去1秒内另有 N 条同类异常被省略"输出。
    def _log_msg_error(self, host, exc):
        now = time.time()
        state = self._msg_err_log_state.get(host)
        if state is None:
            # 首次：立即打印并建立状态
            self._msg_err_log_state[host] = (now, 0)
            logger.error("处理消息失败 [%s]: %s", host, exc)
            return
        last_time, suppressed = state
        if now - last_time >= 1.0:
            # 距上次打印已满 1 秒：放行；若有抑制条数则一并输出
            if suppressed > 0:
                logger.error("处理消息失败 [%s]: %s (过去1秒内另有 %d 条同类异常被省略)",
                             host, exc, suppressed)
            else:
                logger.error("处理消息失败 [%s]: %s", host, exc)
            self._msg_err_log_state[host] = (now, 0)
        else:
            # 1 秒内重复：抑制，仅累加计数
            self._msg_err_log_state[host] = (last_time, suppressed + 1)

    def start(self):
        """启动与所有 Broker 的连接并启用后台自动断线重连"""
        with self.lock:
            if self.clients:
                logger.warning("MultiMQTTManager.start() 重复调用，已忽略")
                return
            # [修复-N2/N3] 清除停止信号，保证"停→启"可复用同一 manager
            self._stop_event.clear()
        # [修复-③] 先启动分发线程，再启动底层连接，避免消息入队而无人消费
        self._dispatch_thread = threading.Thread(
            target=self._dispatch_loop, daemon=True, name="MQTTMsgDispatch"
        )
        self._dispatch_thread.start()
        for host, port in self.brokers:
            client_id = f"multi_client_{int(time.time()*1000)}_{uuid.uuid4().hex[:4]}"
            client = mqtt_client.Client(CallbackAPIVersion.VERSION2, client_id=client_id, protocol=mqtt_client.MQTTv311)
            client.user_data_set(host)
            client.reconnect_delay_set(min_delay=1, max_delay=self.max_reconnect_delay)
            client.on_connect = self._on_connect
            client.on_disconnect = self._on_disconnect
            client.on_message = self._on_message
            client.on_publish = self._on_publish
            try:
                client.connect_async(host, port, keepalive=self.keepalive)
                client.loop_start()
                self.clients[host] = client
                if self.log_connection:
                    logger.info(f"开启后台连接任务 -> {host}:{port}")
            except Exception as e:
                # [稳定性] 半构造的 client 需要回收，避免 socket / 线程泄漏
                try:
                    client.loop_stop()
                except Exception:
                    pass
                logger.error(f"连接初始化失败 [{host}]: {e}")
        if self.enable_stats:
            self.stats.start(self.clients)

    # ================= [基于 Userdata 的统一回调] =================
    def _on_connect(self, client, userdata, flags, rc, properties=None):
        host = userdata
        if rc == 0:
            if self.log_connection:
                logger.info(f"✅ [已连接] Broker: {host}")
            self.stats.on_connect(host)
            # [修复-M1] 锁内只做快照，锁外遍历 subscribe。
            # client.subscribe() 是 paho 的内部操作，若 socket 发送缓冲区满
            # 可能产生阻塞；持锁调用会卡住 stats.get_report()、
            # publish_broadcast、stop() 等所有需要 self.lock 的路径。
            with self.lock:
                topics_snapshot = list(self.subscribed_topics)
            for topic in topics_snapshot:
                client.subscribe(topic)
        else:
            if self.log_connection:
                logger.warning(f"❌ [连接失败] Broker: {host}, rc={rc}")
            # [修复重复掉线统计]: paho-mqtt 在 rc!=0 时会自动触发 _on_disconnect 产生回调，此处移除手动调用

    def _on_disconnect(self, client, userdata, flags, rc, properties=None):
        host = userdata
        # [修复-④][修复-N2/N3] 主动 stop 场景：结算"最后一段在线时长"，但不计入掉线。
        # 用 _stop_event 替代 _stopping 布尔标志，读写都在 Event 内部加锁，线程安全。
        if self._stop_event.is_set():
            self.stats.finalize_for_shutdown(host)
            return
        self.stats.on_disconnect(host)
        if rc != 0:
            if self.log_connection:
                logger.warning(f"⚠️ [意外断开] Broker: {host} (rc={rc})，自动尝试重连...")

    def _on_publish(self, client, userdata, mid, *args, **kwargs):
        host = userdata
        if self.enable_stats:
            self.stats.on_publish_ack(host, mid)

    def _on_message(self, client, userdata, msg):
        """
        注意：此回调运行在 paho-mqtt 的后台网络循环线程中。
        因此这里只做轻量的解析 / 验签 / 去重 / 入队，把业务回调丢到独立线程消费。
        """
        host = userdata
        try:
            raw_payload = msg.payload.decode('utf-8')
            data = process_cipher(raw_payload, decrypt=True, enabled=self.enable_crypto)
            # [增加类型校验防御]: 确保 data 为字典类型
            if not isinstance(data, dict):
                logger.warning(f"⚠️ [{host}] 收到无效非字典消息格式，忽略处理")
                return
            # ------------------------------------------------------------------
            # [修复-①] req_id 类型归一化
            # MQTT payload 属于不可信外部输入。若恶意节点发送 {"req_id": 12345}，
            # 直接执行 `"|" not in req_id` 会抛 TypeError 阻塞本线程。
            # 统一策略：None -> None；str -> 原样；其他类型 -> str() 后参与后续判断。
            # ------------------------------------------------------------------
            raw_req_id = data.get("req_id")
            if raw_req_id is None:
                req_id = None
            elif isinstance(raw_req_id, str):
                req_id = raw_req_id
            else:
                req_id = str(raw_req_id)
            # --- ECDSA 验证防重放核心逻辑 ---
            if "code" in data:
                # [修复-S3] fail-closed：公钥配置了但解析失败 → 直接拒绝
                if self._server_vk_invalid:
                    logger.warning(
                        f"⚠️ [{host}] 拒绝执行: 服务端公钥配置无效 (fail-closed) | "
                        f"server_pubkey={_describe_public_key(self.server_public_key_bytes)}"
                    )
                    return
                if self.server_vk is None:
                    # [修复-⑥] 使用 __init__ 中已缓存的验签对象
                    logger.debug(
                        "ℹ️ [%s] 服务器未配置公钥，跳过验签检查。req_id=%s | has_code=%s",
                        host, req_id, ("code" in data),
                    )
                else:
                    # 归一化后仍非字符串（例如 raw 为 None）或缺少签名分隔符 -> 拒绝
                    if not isinstance(req_id, str) or "|" not in req_id:
                        logger.warning(
                            f"⚠️ [{host}] 拒绝执行: 缺少 ECDSA 签名结构 "
                            f"(req_id 类型={type(raw_req_id).__name__}) | "
                            f"server_pubkey={_describe_public_key(self.server_public_key_bytes)}"
                        )
                        return
                    base_req_id, sig_hex = req_id.rsplit("|", 1)
                    logger.info(
                        "🔑 [%s] 请求已签名，开始验签: req_id=%s | base_req_id=%s | signature_len=%d | server_pubkey=%s",
                        host, req_id, base_req_id, len(sig_hex),
                        _describe_public_key(self.server_public_key_bytes),
                    )
                    code_str = str(data.get("code", ""))
                    # ------------------------------------------------------
                    # [修复-⑦] 安全整型清洗 timestamp：
                    # 收发两端统一为 str(int(float(ts)))，避免浮点字符串如
                    # "1699999999.5" 导致签名端/验签端字面量不一致。
                    # 非数字 input 一律回退为 "0"（验签必然失败，等价于拒绝）。
                    # ------------------------------------------------------
                    try:
                        ts_str = str(int(float(data.get("timestamp", 0))))
                    except (ValueError, TypeError):
                        ts_str = "0"
                    sign_msg = f"{base_req_id}|{code_str}|{ts_str}".encode('utf-8')
                    try:
                        # [修复-⑥] 直接复用缓存对象，不再每次 from_pem
                        self.server_vk.verify(bytes.fromhex(sig_hex), sign_msg, hashfunc=hashlib.sha256)
                        logger.info("✅ [%s] ECDSA 验签成功，允许执行: req_id=%s", host, base_req_id)
                    except Exception:
                        logger.warning(f"⚠️ [{host}] 拒绝执行: ECDSA 签名无效 | req_id={req_id} | server_pubkey={_describe_public_key(self.server_public_key_bytes)}")
                        return
            # [安全] 去重放到验签之后，避免伪造 req_id 污染缓存造成 DoS
            if req_id and not self.dedup_cache.add_if_not_exists(req_id):
                return
            if self.log_messages:
                logger.info(f"📩 收到消息 [{msg.topic}] 来自 {host}")
            # ------------------------------------------------------------------
            # [修复-③] 不再同步调用 self.message_callback（会阻塞 paho 网络线程）。
            # 改为入队，交由独立的 MQTTMsgDispatch 线程串行消费。
            # 队列满时立即丢弃并告警 —— 宁可丢消息，也不能卡住网络心跳。
            # ------------------------------------------------------------------
            if self.message_callback:
                try:
                    self._msg_queue.put_nowait((msg.topic, data, host))
                except queue.Full:
                    logger.warning(
                        "⚠️ [%s] 消息分发队列已满(%d)，丢弃消息 topic=%s",
                        host, self.MSG_QUEUE_MAXSIZE, msg.topic,
                    )
        except Exception as e:
            # [修复-N4] 高频异常日志限流：每 host 每秒最多 1 条。
            # 不再打印堆栈（logger.exception），避免 5 个公共 broker 的
            # 大量畸形消息把 ERROR + 堆栈刷爆磁盘，反向阻塞 paho 网络线程。
            self._log_msg_error(host, e)

    def _dispatch_loop(self):
        """
        [修复-N2/N3] 独立的分发线程：串行消费队列，把业务回调与 paho 网络线程解耦。
        退出机制改用 threading.Event：
        - 不再需要 stop() 里 put_nowait(None) 的哨兵（避免队列满时哨兵丢失）；
        - stop() 里的 join() 可以无限等待，因为 _stop_event 设置后本线程最多
          等一次 queue.get(timeout=1.0) 超时就会退出，不会出现僵尸线程；
        - 因此也不会出现"旧线程未退、start() 又起新线程共享同一队列"的双线程抢消息。
        """
        while not self._stop_event.is_set():
            try:
                item = self._msg_queue.get(timeout=1.0)
            except queue.Empty:
                # 队列空闲：回到循环顶部重新检查 _stop_event
                continue
            # [修复-⑧] 拿到消息后先看停止信号：一旦要求停止，立刻抛弃并退出
            if self._stop_event.is_set():
                return
            topic, data, host = item
            try:
                if self.message_callback:
                    self.message_callback(topic, data, host)
            except Exception:
                # 单条消息回调异常不能影响其他消息，也不能杀死分发线程
                logger.exception("消息分发回调执行失败 [%s] topic=%s", host, topic)

    def subscribe(self, topic: str):
        # [修复-M2] 锁内只做状态更新 + 快照，锁外遍历 subscribe。
        # 与 M1 同源：持锁调用 c.subscribe() 一旦阻塞，
        # stop() 里 `with self.lock: self.clients.clear()` 会被卡住，
        # 导致整个进程无法优雅退出。
        with self.lock:
            self.subscribed_topics.add(topic)
            clients_snapshot = list(self.clients.items())
        for host, c in clients_snapshot:
            if c.is_connected():
                c.subscribe(topic)

    def publish_broadcast(self, topic: str, payload_dict: dict, client_private_key_bytes=None):
        """广播传输消息"""
        # [修复副作用Bug]: 对传入的 payload_dict 进行浅拷贝，防止修改上层数据
        out_payload = payload_dict.copy()
        # ------------------------------------------------------------------
        # [修复-⑥ 严重性能瓶颈] 签名对象也从缓存里取，不再每次 from_pem。
        # 若调用方临时传入 client_private_key_bytes，则视为"一次性覆盖"，
        # 这种情况密钥可能不同，无法复用缓存，仍需现场解析一次（属于罕见路径）。
        # ------------------------------------------------------------------
        if client_private_key_bytes:
            try:
                sk = ecdsa.SigningKey.from_pem(get_standard_pem_bytes(client_private_key_bytes))
            except Exception:
                logger.exception("解析调用方临时私钥失败，本次广播将不做签名")
                sk = None
        else:
            sk = self.client_sk
        if sk is not None and "code" in out_payload:
            # ------------------------------------------------------------------
            # [修复-②] 原先使用 payload_dict["req_id"] 裸下标，
            # 上层业务漏传 req_id 时直接抛 KeyError 导致整进程崩溃。
            # 改为显式校验 + 抛出带上下文的 ValueError，调用方可精准捕获。
            # 另外：原实现签名写回的是 payload_dict（入参）而不是 out_payload（副本），
            # 既等于"浅拷贝了却没用"，也污染了调用方的原始字典 —— 一并修正。
            # ------------------------------------------------------------------
            if "req_id" not in out_payload:
                raise ValueError(
                    "publish_broadcast: 报文包含 'code' 且已配置私钥时，必须提供 'req_id' 字段"
                )
            if "timestamp" not in out_payload:
                raise ValueError(
                    "publish_broadcast: 报文包含 'code' 且已配置私钥时，必须提供 'timestamp' 字段"
                )
            base_req_id = str(out_payload["req_id"])
            code_str = str(out_payload.get("code", ""))
            # ------------------------------------------------------
            # [修复-⑦] 发送端做与接收端一致的整型清洗，
            # 保证两侧拼出的 sign_msg 完全一致，避免浮点尾差导致验签失败。
            # ------------------------------------------------------
            try:
                ts_str = str(int(float(out_payload["timestamp"])))
            except (ValueError, TypeError):
                ts_str = "0"
            sign_msg = f"{base_req_id}|{code_str}|{ts_str}".encode('utf-8')
            # [修复-⑥] 直接复用缓存对象，不再每次 from_pem
            signature = sk.sign(sign_msg, hashfunc=hashlib.sha256)
            # 写回 out_payload（副本），绝不修改调用方原始的 payload_dict
            out_payload["req_id"] = f"{base_req_id}|{signature.hex()}"
        payload_str = process_cipher(out_payload, decrypt=False, enabled=self.enable_crypto)
        # [修复-S1] 遍历前先快照，避免与 stop() 里的 self.clients.clear() 竞态。
        # 否则并发场景下会命中 RuntimeError: dictionary changed size during iteration。
        with self.lock:
            clients_snapshot = list(self.clients.items())
        for host, c in clients_snapshot:
            if c.is_connected():
                c.publish(topic, payload_str, qos=0)

    def stop(self):
        # [修复-④][修复-N2/N3] 用 _stop_event 替代 _stopping：
        # - Event 读写线程安全；
        # - _dispatch_loop 用 wait/get 感知它，无需再发哨兵 None；
        # - 停止后 start() 会 clear()，可安全复用于"停→启"。
        self._stop_event.set()
        if self.enable_stats:
            self.stats.stop()
        with self.lock:
            clients_snapshot = list(self.clients.values())
            self.clients.clear()
        for c in clients_snapshot:
            try:
                c.loop_stop()
                c.disconnect()
            except Exception:
                logger.debug("关闭 MQTT client 时出现异常", exc_info=True)
        # [修复-N2/N3] 无限等待：_stop_event 已设置，分发线程最多 1 次 get 超时就退出，
        # 不会死锁；也不用再担心"旧线程成为僵尸 + start() 起新线程共享同一队列"。
        t = self._dispatch_thread
        if t and t.is_alive():
            t.join()
        self._dispatch_thread = None
        logger.info("所有 MQTT 连接已安全关闭")