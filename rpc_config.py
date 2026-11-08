"""rpc.json 配置层：默认值、校验/归一化、容错读、原子读-改-写、统一锁。

从 server_mqtt.py / server_http.py / client/client_mqtt.py 抽离出来的
rpc.json 读写逻辑，行为与抽离前逐条保持一致：

- 默认值模板 + 逐键校验/归一化（脏 rpc.json 只丢该项、回落默认值，绝不阻断启动）；
- 严格读（``_read_rpc_config_document``）与容错读（``load_rpc_config``）；
- 读-改-写回（``_write_rpc_config_updates``）：只持久化用户显式修改的项；
- 原子写（``write_json_document``）：同目录临时文件 + 保留原权限位 + fsync
  + ``os.replace``，server_mqtt 的配置写回与 server_http 的别名持久化共用；
- 全部读-改-写共用同一把 ``CONFIG_LOCK``（``setdefault`` 保 reload 安全）：
  旧实现 server_mqtt / server_http 各持一把锁，并发写同一文件时读-改-写
  会互相吞掉对方刚写的键，统一锁后跨模块写回串行，消除丢失更新；
- broker 直读通道（``load_broker_list`` / ``filter_broker_list``）：server_mqtt
  与 client 启动经同一份合成实现读到同样的 broker；``broker_list_no``
  （可选顶层键，如 ``{"192.168.1.20:1883": ["pty"]}``）支持按模块屏蔽条目。

路径相关接口（``load_rpc_config`` 等）``path=None`` 时默认读写本模块的
``DEFAULT_RPC_CONFIG_PATH``；server_mqtt / client_mqtt 保留原函数名的薄封装，
在调用时解析各自模块的 ``RPC_CONFIG_PATH`` / ``CLIENT_RPC_CONFIG_PATH``
（测试 / 部署会打补丁注入路径，必须晚绑定），接口与行为全部不变。
"""

import copy
import json
import logging
import os
import stat
import tempfile
import threading
from pathlib import Path
from urllib.parse import urlsplit

from ecdsa import VerifyingKey

from multi_mqtt import (
    BROKER_LIST,
    ConnectionQualityStats,
    get_standard_public_pem_bytes,
)

# 告警沿用 "Server" 通道：重构前后日志归属不变
logger = logging.getLogger("Server")

DEFAULT_RPC_CONFIG_PATH = Path(__file__).with_name("rpc.json")

# 读-改-写的统一锁：server_mqtt 配置写回与 server_http 别名持久化共用同一把，
# 跨模块写同一文件时串行，消除两把锁时的丢失更新竞态。
# reload 会重造新锁对象：正攥旧锁写 rpc.json 的线程与拿新锁的线程同时进临界区，
# setdefault 保住同一把锁跨 reload。
CONFIG_LOCK = globals().setdefault('CONFIG_LOCK', threading.RLock())

REQUEST_TOPIC = "sys/device/request"
DEFAULT_PUB_KEY = 'ecdsa-sha2-nistp256 AAAAE2VjZHNhLXNoYTItbmlzdHAyNTYAAAAIbmlzdHAyNTYAAABBBOf+GndQauvHHEICc3HTrEeV9bv5S/FTiI64tnMAUPCAaofEbRDmXa8d5R54k8tYOPjt+zdnZJaoSMmJ2pnp4ys='

# --- [global_topic_list 监听 topic 定期报告] 默认配置 ---
# 命名说明：stats.get_report 是网络连接质量报告；本组是"监听 topic 列表"报告，
# 定期发布到 global_topic_list，供新订阅者经 Retain 消息发现服务端监听 topic
# 全部参数均可在 rpc.json 中按同名键覆盖，运行期经 rpc_config(...) 热更新
DEFAULT_GLOBAL_TOPIC_LIST = "global_topic_list"          # 报告发布到的 topic
DEFAULT_GLOBAL_TOPIC_LIST_INTERVAL = 3600.0              # 发布间隔：空闲期每小时一次
DEFAULT_GLOBAL_TOPIC_LIST_IDLE_THRESHOLD = 86400.0       # 距上次下发指令超过 24 小时视为空闲（可配置）
DEFAULT_GLOBAL_TOPIC_LIST_CHECK_INTERVAL = ConnectionQualityStats.PING_INTERVAL  # 默认复用连接质量 ping 间隔（600s），与 ping 同节拍统一时间驱动避免交错唤醒；可配置为更小值如 15 便于测试
# 发布参数（经 **ka 一路透传到 paho-mqtt）：QoS1+Retain，
# broker 会连同 QoS 一起保存保留消息，新订阅者上线即收到最新 topic 列表
DEFAULT_GLOBAL_TOPIC_LIST_PUBLISH_KWARGS = {"qos": 1, "retain": True}
# 远端上报：URL 模板数组（global_topic_url_list），发送时把占位符 REPORT_BODY
# 替换成百分号编码后的报告 JSON（末尾带 %0A，保持远端文件一行一条）。
# 模板可指向任意站点，例如 "https://xxx.example/report/REPORT_BODY"；
# 配成空数组 [] 即完全不上报，不改代码。
REPORT_BODY_TOKEN = "REPORT_BODY"
DEFAULT_GLOBAL_TOPIC_URL_LIST = [
"https://qmm.pythonanywhere.com/with%20open('topics','a')%20as%20f:f.write(r'''" +REPORT_BODY_TOKEN + "''');r=f.tell()",
# 'https://textdb.online/update/?key=ime_feed&value='+REPORT_BODY_TOKEN,
# 阿里云 SLS：走 Web Tracking（logstore 需 enable_tracking=true），GET 免签名，
# 报告 JSON 落在 new_logstore 的 report 字段；仍只用标准库 urllib。
"https://log-service-1148372724923069-cn-hangzhou.cn-hangzhou.log.aliyuncs.com/logstores/new_logstore/track?APIVersion=0.6.0&report=" + REPORT_BODY_TOKEN,

]

# --- [连接质量统计周期] 默认配置 ---
# 命名统一为 stats_print_interval / stats_ping_interval：
#   stats_print_interval - 报告打印间隔（默认 3600s，0=关闭周期报告）
#   stats_ping_interval  - 周期 Ping 间隔（默认 600s，0=停用周期 Ping）
# 两个键均可在 rpc.json 覆盖，并在运行期经 rpc_config(...) 热更新。
DEFAULT_STATS_PRINT_INTERVAL = ConnectionQualityStats.PRINT_INTERVAL
DEFAULT_STATS_PING_INTERVAL = ConnectionQualityStats.PING_INTERVAL

# --- [字段别名] 与 client/client_mqtt.py 的 alias_xxx 同风格 ---
# 所有 rpc_config(...) 入口（模块级 rpc_config / 实例级 gms.rpc_config /
# HTTP RPC / MQTT RPC）都先经 normalize_rpc_updates() 归一化，因此别名在
# 任何入口都等价于规范键。broker_list_add/del 还接受 "host:port" 字符串
# 或单条 (host, port, auth)，口径与 client 的 --broker_add 一致。
RPC_CONFIG_ALIASES = {
    "broker_list": ("brokers", "bl"),
    "broker_list_add": ("add_broker", "add_brokers", "broker_add", "badd", "add"),
    "broker_list_del": ("del_broker", "del_brokers", "broker_del", "bdel", "del"),
    "request_topic": ("topic", "t", "q", "request"),
    "pub_key": ("public_key", "pubkey", "pub"),
    "global_topic_url_list": ("report_urls", "urls", "gul"),
    "global_topic_list": ("report_topic", "gtl"),
    "global_topic_list_interval": ("gtl_interval",),
    "global_topic_list_idle_threshold": ("gtl_idle_threshold", "idle_threshold"),
    "global_topic_list_check_interval": ("gtl_check_interval", "gtl_tick"),
    "global_topic_list_publish_kwargs": ("gtl_publish_kwargs", "publish_kwargs"),
    "stats_print_interval": ("print_interval", "stats_print", "spi"),
    "stats_ping_interval": ("ping_interval", "stats_ping", "sgi"),
    "client_stats_print_interval": ("client_print_interval", "cspi"),
}


def _default_rpc_config():
    return {
        "broker_list": copy.deepcopy(BROKER_LIST),
        "broker_list_add": [],
        "broker_list_del": [],
        "request_topic": REQUEST_TOPIC,
        "pub_key": DEFAULT_PUB_KEY,
        "global_topic_url_list": list(DEFAULT_GLOBAL_TOPIC_URL_LIST),
        # [global_topic_list 监听 topic 报告] 参数（同名键可在 rpc.json 覆盖）
        # 该报告默认常开（tick 并入 _dispatch_loop 周期任务槽）；唯一可关闭的是
        # 远端上报（global_topic_url_list=[]）
        "global_topic_list": DEFAULT_GLOBAL_TOPIC_LIST,
        "global_topic_list_interval": DEFAULT_GLOBAL_TOPIC_LIST_INTERVAL,
        "global_topic_list_idle_threshold": DEFAULT_GLOBAL_TOPIC_LIST_IDLE_THRESHOLD,
        "global_topic_list_check_interval": DEFAULT_GLOBAL_TOPIC_LIST_CHECK_INTERVAL,
        "global_topic_list_publish_kwargs": dict(DEFAULT_GLOBAL_TOPIC_LIST_PUBLISH_KWARGS),
        # [连接质量统计周期] 同名键可在 rpc.json 覆盖
        "stats_print_interval": DEFAULT_STATS_PRINT_INTERVAL,
        "stats_ping_interval": DEFAULT_STATS_PING_INTERVAL,
        # [客户端专属] 仅 client_*.py 读取；服务端不应用。null/缺省时客户端
        # 依次回退 stats_print_interval，再回退 Python 原有默认行为（3600s）。
        "client_stats_print_interval": None,
    }


def _normalize_broker_list(brokers, name, allow_empty=False):
    if not isinstance(brokers, list) or (not brokers and not allow_empty):
        suffix = "列表" if allow_empty else "非空列表"
        raise ValueError(f"{name} 必须是{suffix}")
    normalized_brokers = []
    for index, entry in enumerate(brokers):
        if not isinstance(entry, (list, tuple)) or len(entry) not in (2, 3):
            raise ValueError(f"{name}[{index}] 必须包含 host、port 和可选认证信息")
        host, port = entry[:2]
        if not isinstance(host, str) or not host.strip():
            raise ValueError(f"{name}[{index}] 的 host 必须是非空字符串")
        if isinstance(port, bool) or not isinstance(port, (int, str)):
            raise ValueError(f"{name}[{index}] 的 port 必须是整数")
        if isinstance(port, str) and not port.isdecimal():
            raise ValueError(f"{name}[{index}] 的 port 必须是整数")
        try:
            port = int(port)
        except (TypeError, ValueError) as error:
            raise ValueError(f"{name}[{index}] 的 port 必须是整数") from error
        if not 1 <= port <= 65535:
            raise ValueError(f"{name}[{index}] 的 port 超出有效范围")
        broker = [host.strip(), port]
        if len(entry) == 3:
            auth = entry[2]
            if isinstance(auth, (list, tuple)) and not auth:
                auth = None   # 空认证列表 [] 等价于不带认证的两元素条目
            if auth is not None:
                if not (
                    isinstance(auth, str)
                    or isinstance(auth, (list, tuple)) and len(auth) >= 2
                ):
                    raise ValueError(f"{name}[{index}] 的认证信息格式无效")
                broker.append(list(auth) if isinstance(auth, tuple) else auth)
        normalized_brokers.append(tuple(broker))
    return normalized_brokers


def _broker_identity(broker):
    return broker[0].casefold(), broker[1]


def _normalize_positive_seconds(value, name):
    """正数秒校验：接受 int/float/纯数字字符串，统一归一为 float。"""
    if isinstance(value, bool) or not isinstance(value, (int, float, str)):
        raise ValueError(f"{name} 必须是正数（秒）")
    try:
        number = float(value.strip() if isinstance(value, str) else value)
    except ValueError as error:
        raise ValueError(f"{name} 必须是正数（秒）") from error
    if not (number > 0.0) or number == float("inf"):   # 同时挡住 0/负数/NaN/inf
        raise ValueError(f"{name} 必须是正数（秒）")
    return number


def _normalize_non_negative_seconds(value, name):
    """非负秒校验：接受 0（表示停用对应周期任务）以及正数。"""
    if isinstance(value, bool) or not isinstance(value, (int, float, str)):
        raise ValueError(f"{name} 必须是 >=0 的秒数")
    try:
        number = float(value.strip() if isinstance(value, str) else value)
    except ValueError as error:
        raise ValueError(f"{name} 必须是 >=0 的秒数") from error
    if number < 0.0 or number != number or number == float("inf"):
        raise ValueError(f"{name} 必须是 >=0 的秒数")
    return number


def _normalize_global_topic_url_list(urls):
    """校验远端上报 URL 模板数组；空数组表示关闭上报。

    模板里的 REPORT_BODY 在发送时才替换，所以校验时先用一个占位值代入，
    确保替换后的 URL 仍是合法的 http(s) 地址（scheme/主机不能来自正文）。
    """
    if not isinstance(urls, (list, tuple)):
        raise ValueError("global_topic_url_list 必须是 URL 模板数组（可为空数组）")
    normalized = []
    for index, url in enumerate(urls):
        if not isinstance(url, str) or not url.strip():
            raise ValueError(f"global_topic_url_list[{index}] 必须是非空 URL 模板字符串")
        url = url.strip()
        check_url = url.replace(REPORT_BODY_TOKEN, "REPORT_BODY_VALUE")
        parsed_url = urlsplit(check_url)
        if parsed_url.scheme not in ("http", "https") or not parsed_url.netloc:
            raise ValueError(
                f"global_topic_url_list[{index}] 必须是非空 HTTP(S) URL 模板"
            )
        normalized.append(url)
    return normalized


def _default_global_topic_url_list():
    """默认 URL 模板数组的干净版本；默认值本身写坏时降级为 []（关闭上报）。"""
    try:
        return _normalize_global_topic_url_list(DEFAULT_GLOBAL_TOPIC_URL_LIST)
    except Exception as error:
        logger.warning(
            "默认 global_topic_url_list 非法，已关闭远端上报（不影响主流程）：%s", error)
        return []


def _safe_global_topic_url_list(value, source="global_topic_url_list"):
    """[边缘统计] URL 模板数组的容错归一化（永不抛异常）。

    关闭远端上报的唯一方式是显式给空数组 []；其余情况都尽量保留合法项：
      - 数组里混有非法项：只丢非法项，同数组里其它合法 URL 照常生效；
      - 全部非法、或根本不是数组：回落代码默认值（默认值也坏才关闭上报）。
    """
    if isinstance(value, (list, tuple)) and len(value) == 0:
        return []                       # 显式 []：关闭远端上报（不碰默认值）
    defaults = _default_global_topic_url_list()
    if not isinstance(value, (list, tuple)):
        logger.warning("%s 不是 URL 模板数组，已回落代码默认值：%r", source, value)
        return list(defaults)
    kept = []
    for index, url in enumerate(value):
        try:
            kept.append(_normalize_global_topic_url_list([url])[0])
        except Exception as error:
            logger.warning("%s[%d] 非法，已跳过该 URL（不影响同组其它 URL）：%s",
                           source, index, error)
    if not kept:
        logger.warning("%s 没有一条合法 URL 模板，已回落代码默认值", source)
        return list(defaults)
    return kept


def _safe_default_rpc_config():
    """默认配置的"干净"版本：默认值里的边缘统计写错时先把它中和掉，
    免得一个坏默认值把逐键回落的校验全部带崩。"""
    config = _default_rpc_config()
    config["global_topic_url_list"] = _default_global_topic_url_list()
    return config


def _validate_rpc_config(config):
    """严格校验并归一化配置；任何非法项直接抛异常（只给显式输入用）。

    显式输入包括 rpc_config(...) 热更新、命令行参数、构造参数：用户当场传错
    必须立刻报错。文件内容与运行时合并走容错的 _normalize_rpc_config。
    """
    if not isinstance(config, dict):
        raise TypeError("RPC 配置必须是 JSON object")
    result = _default_rpc_config()
    known_keys = set(result)
    result.update({key: value for key, value in config.items() if key in known_keys})

    broker_base = _normalize_broker_list(
        config.get("_broker_list_base", result["broker_list"]), "broker_list"
    )
    brokers_add = _normalize_broker_list(
        result["broker_list_add"], "broker_list_add", allow_empty=True
    )
    brokers_del = _normalize_broker_list(
        result["broker_list_del"], "broker_list_del", allow_empty=True
    )
    deleted = {_broker_identity(broker) for broker in brokers_del}
    effective_brokers = [
        broker for broker in broker_base
        if _broker_identity(broker) not in deleted
    ]
    for broker in brokers_add:
        identity = _broker_identity(broker)
        for index, existing in enumerate(effective_brokers):
            if _broker_identity(existing) == identity:
                effective_brokers[index] = broker
                break
        else:
            effective_brokers.append(broker)
    result["_broker_list_base"] = broker_base
    result["broker_list"] = effective_brokers
    result["broker_list_add"] = brokers_add
    result["broker_list_del"] = brokers_del

    topic = result["request_topic"]
    if not isinstance(topic, str) or not topic.strip():
        raise ValueError("request_topic 必须是非空字符串")
    result["request_topic"] = topic.strip()

    pub_key = result["pub_key"]
    if isinstance(pub_key, bytes):
        try:
            pub_key = pub_key.decode("utf-8")
        except UnicodeDecodeError as error:
            raise ValueError("pub_key 必须是 UTF-8 公钥字符串") from error
    if pub_key is not None and not isinstance(pub_key, str):
        raise ValueError("pub_key 必须是字符串或 null")
    if pub_key:
        try:
            normalized_key = get_standard_public_pem_bytes(pub_key)
            VerifyingKey.from_pem(normalized_key)
        except Exception as error:
            raise ValueError(f"pub_key 无法解析: {error}") from error
    result["pub_key"] = pub_key or None
    # 远端上报是边缘统计：只过滤非法 URL，绝不因它抛异常（显式 [] 才关闭上报）
    result["global_topic_url_list"] = _safe_global_topic_url_list(
        result["global_topic_url_list"])

    # [global_topic_list 监听 topic 报告] 参数校验（qos 钉死规则在 MQTTServer.__init__）
    report_topic = result["global_topic_list"]
    if not isinstance(report_topic, str) or not report_topic.strip():
        raise ValueError("global_topic_list 必须是非空字符串")
    result["global_topic_list"] = report_topic.strip()
    result["global_topic_list_interval"] = _normalize_positive_seconds(
        result["global_topic_list_interval"], "global_topic_list_interval")
    result["global_topic_list_idle_threshold"] = _normalize_positive_seconds(
        result["global_topic_list_idle_threshold"], "global_topic_list_idle_threshold")
    result["global_topic_list_check_interval"] = _normalize_positive_seconds(
        result["global_topic_list_check_interval"], "global_topic_list_check_interval")
    publish_kwargs = result["global_topic_list_publish_kwargs"]
    if publish_kwargs is None:
        publish_kwargs = dict(DEFAULT_GLOBAL_TOPIC_LIST_PUBLISH_KWARGS)
    elif not isinstance(publish_kwargs, dict):
        raise ValueError("global_topic_list_publish_kwargs 必须是 JSON object 或 null")
    result["global_topic_list_publish_kwargs"] = dict(publish_kwargs)

    # [连接质量统计周期] 0 表示停用对应周期任务，因此用非负校验
    result["stats_print_interval"] = _normalize_non_negative_seconds(
        result["stats_print_interval"], "stats_print_interval")
    result["stats_ping_interval"] = _normalize_non_negative_seconds(
        result["stats_ping_interval"], "stats_ping_interval")

    # [客户端专属] null/空字符串表示「未设置」，由客户端继续回退；
    # 设置后必须是 >=0 的秒数（0=客户端关闭周期报告）。
    client_interval = result["client_stats_print_interval"]
    if client_interval is None or (
            isinstance(client_interval, str) and not client_interval.strip()):
        result["client_stats_print_interval"] = None
    else:
        result["client_stats_print_interval"] = _normalize_non_negative_seconds(
            client_interval, "client_stats_print_interval")
    return result


def _normalize_rpc_config(config):
    """合并配置：非法项逐项回落代码默认值并告警，绝不阻塞主流程。

    每个配置键都有代码默认值；global_topic_* / stats_* 还只是边缘统计，
    所以脏 rpc.json、旧键、甚至代码默认值写错，都只能丢掉那一项，
    其余项（尤其 MQTT/HTTP 主流程所需项）照常生效。
    显式输入（rpc_config(...)、命令行参数）仍由 _validate_rpc_config 严格报错。
    """
    if not isinstance(config, dict):
        logger.warning("RPC 配置不是 JSON object，全部回落代码默认值")
        return _safe_default_rpc_config()
    try:
        return _validate_rpc_config(config)
    except Exception as error:
        logger.warning("RPC 配置存在非法项，按项回落代码默认值：%s", error)
    # 逐键回落：坏掉的项不牵连同级其它项
    sanitized = _sanitize_rpc_document(config)
    try:
        return _validate_rpc_config(sanitized)
    except Exception as error:
        # 兜底：连默认值都有问题（代码/打包层面的 bug）时也放行启动
        logger.warning("RPC 配置净化后仍不合法，按净化结果启动：%s", error)
        sanitized["_broker_list_base"] = [
            tuple(entry) if isinstance(entry, (list, tuple)) else entry
            for entry in sanitized["broker_list"]
        ]
        return sanitized


def _json_rpc_config(config):
    return {
        "broker_list": _json_brokers(config["broker_list"]),
        "broker_list_add": _json_brokers(config["broker_list_add"]),
        "broker_list_del": _json_brokers(config["broker_list_del"]),
        "request_topic": config["request_topic"],
        "pub_key": config["pub_key"],
        "global_topic_url_list": list(config["global_topic_url_list"]),
        "global_topic_list": config["global_topic_list"],
        "global_topic_list_interval": config["global_topic_list_interval"],
        "global_topic_list_idle_threshold": config["global_topic_list_idle_threshold"],
        "global_topic_list_check_interval": config["global_topic_list_check_interval"],
        "global_topic_list_publish_kwargs": config["global_topic_list_publish_kwargs"],
        "stats_print_interval": config["stats_print_interval"],
        "stats_ping_interval": config["stats_ping_interval"],
        "client_stats_print_interval": config["client_stats_print_interval"],
    }


def _json_brokers(brokers):
    return [
        [entry[0], entry[1], list(entry[2]) if isinstance(entry[2], tuple) else entry[2]]
        if len(entry) == 3 else [entry[0], entry[1]]
        for entry in brokers
    ]


def _atomic_write_text(path, text):
    """同目录临时文件 + fsync + ``os.replace`` 原子替换文本，保留原文件权限。

    mkstemp 在 POSIX 上固定创建 0600，直接 replace 会把 rpc.json 原本的
    0644 等权限改掉，换成另一个用户运行的服务就可能读不到，所以写临时文件后
    先恢复原文件权限，再原子替换。
    """
    config_path = Path(path)
    config_path.parent.mkdir(parents=True, exist_ok=True)
    try:
        existing_mode = stat.S_IMODE(os.stat(config_path).st_mode)
    except OSError:
        existing_mode = None
    fd, temp_path = tempfile.mkstemp(
        prefix=config_path.name + '.', suffix='.tmp', dir=str(config_path.parent))
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as config_file:
            config_file.write(text)
            config_file.flush()
            os.fsync(config_file.fileno())
        if existing_mode is not None:
            os.chmod(temp_path, existing_mode)
        os.replace(temp_path, config_path)
    except BaseException:
        try:
            os.unlink(temp_path)
        except OSError:
            pass
        raise


def write_json_document(path, document):
    """原子写 JSON 文档（整文件标准化序列化）。

    只用于新建或无法保留原结构的兜底；按键增量改写用
    :func:`update_json_document`，保留用户手写的缩进与内容。
    """
    _atomic_write_text(
        path, json.dumps(document, ensure_ascii=False, indent=2) + "\n")


def _json_value_end(text, i):
    """i 处 JSON 值的结束位置（不含值后的分隔符），非法抛 ValueError。"""
    n = len(text)
    ch = text[i]
    if ch == '"':
        i += 1
        while i < n:
            if text[i] == "\\":
                i += 2
                continue
            if text[i] == '"':
                return i + 1
            i += 1
        raise ValueError("JSON 字符串未闭合")
    if ch in "[{":
        depth = 0
        while i < n:
            c = text[i]
            if c == '"':
                i = _json_value_end(text, i)
                continue
            if c in "[{":
                depth += 1
            elif c in "]}":
                depth -= 1
                if depth == 0:
                    return i + 1
            i += 1
        raise ValueError("JSON 容器未闭合")
    end = i
    while end < n and text[end] not in ",]} \t\r\n":
        end += 1
    if end == i:
        raise ValueError("JSON 值为空")
    return end


def _top_level_entries(text):
    """扫描顶层 object 条目：[(key, key_start, value_start, value_end, comma_end)]。

    comma_end 指向条目后逗号的下一位置，无逗号时等于 value_end；
    另返回右花括号位置。只用于改写定位，完整合法性由 json.loads 保证。
    """
    i = 0
    n = len(text)
    while i < n and text[i] in " \t\r\n":
        i += 1
    if i >= n or text[i] != "{":
        raise ValueError("顶层不是 JSON object")
    i += 1
    entries = []
    while True:
        while i < n and text[i] in " \t\r\n":
            i += 1
        if i >= n:
            raise ValueError("JSON 截断")
        if text[i] == "}":
            return entries, i
        if text[i] != '"':
            raise ValueError("顶层键不是字符串")
        key_start = i
        key_end = _json_value_end(text, i)
        key = json.loads(text[key_start:key_end])
        i = key_end
        while i < n and text[i] in " \t\r\n":
            i += 1
        if i >= n or text[i] != ":":
            raise ValueError("顶层键后缺冒号")
        i += 1
        while i < n and text[i] in " \t\r\n":
            i += 1
        if i >= n:
            raise ValueError("JSON 截断")
        value_start = i
        value_end = _json_value_end(text, i)
        i = value_end
        comma_end = value_end
        while i < n and text[i] in " \t\r\n":
            i += 1
        if i < n and text[i] == ",":
            comma_end = i + 1
            i += 1
        entries.append((key, key_start, value_start, value_end, comma_end))


def _entry_by_key(entries, key):
    return next((entry for entry in entries if entry[0] == key), None)


def _insert_entry(text, entries, key, value_text):
    """新增顶层键：多行文件独占新增一行，缩进沿用末条目行首空白。

    单行文件（全文无换行）行内追加，避免凭空造出无缩进的行。
    """
    item = json.dumps(key, ensure_ascii=False) + ": " + value_text
    if not entries:
        open_index = text.find("{")
        return text[:open_index + 1] + item + text[open_index + 1:]
    last = entries[-1]
    if "\n" not in text:
        head = ", "
    else:
        line_start = text.rfind("\n", 0, last[1]) + 1
        line_prefix = text[line_start:last[1]]
        indent = line_prefix[:len(line_prefix) - len(line_prefix.lstrip())]
        head = ",\n" + indent
    return text[:last[4]] + head + item + text[last[4]:]


def _remove_entry(text, entries, key):
    """删除顶层键所在条目并处理相邻逗号与整行换行，其余字节不动。"""
    index = next((i for i, entry in enumerate(entries)
                  if entry[0] == key), None)
    if index is None:
        return text
    key_start, _value_start, value_end, comma_end = entries[index][1:]
    start = key_start
    while start > 0 and text[start - 1] in " \t":
        start -= 1
    if comma_end > value_end:            # 条目后带逗号：连同逗号一起删
        end = comma_end
    else:
        end = value_end
        if index > 0:                    # 前一条目带逗号：把那个逗号一并删掉
            previous = entries[index - 1]
            if previous[4] > previous[3]:
                start = previous[4] - 1
    if (start > 0 and text[start - 1] == "\n"
            and end < len(text) and text[end] == "\n"):
        end += 1                         # 独占一行：吞行尾换行，正好删一行
    return text[:start] + text[end:]


def update_json_document(path, updates=None, deletes=(), force_write=False):
    """按键文本级改写 JSON 文件：只动被改键对应文本，其余字节原样保留。

    - 已有键：原位把值文本替换为单行紧凑 JSON；
    - 新增键：按最后一个顶层键的缩进追加一行（单行文件行内追加）；
    - 删除键：连同相邻逗号一并移除；
    - 文件缺失 / 空文件：无可保留结构，直接整文件写入（只含 updates 键），
      不需要 force_write；
    - 非空错误文件（非法 JSON / 顶层非 object）：默认抛 ValueError 拒绝
      覆盖，保护用户手写内容；只有 ``force_write=True`` 才退化为
      :func:`write_json_document` 整文件标准化写入（只含 updates 键）。

    不加锁，调用方持 CONFIG_LOCK（与 write_json_document 同约定）。
    """
    config_path = Path(path)
    updates = dict(updates or {})
    deletes = [key for key in deletes if key]
    if not updates and not deletes:
        return
    text = None
    error = None
    try:
        raw = config_path.read_text(encoding="utf-8")
    except FileNotFoundError:
        raw = None                       # 文件缺失：新建，直接写
    except (OSError, ValueError, TypeError) as read_error:
        raw = None
        error = read_error               # 读不出内容，无法证明为空，按错误文件处理
    if raw is not None and raw.strip():
        try:
            document = json.loads(raw)
            if not isinstance(document, dict):
                raise ValueError("顶层不是 object")
            _top_level_entries(raw)
            text = raw
        except (ValueError, TypeError) as parse_error:
            error = parse_error
    if text is None and error is not None and not force_write:
        raise ValueError(
            f"rpc.json 非空且无法解析，拒绝覆盖（如需强制覆盖请传 force_write=True）: "
            f"{config_path}: {error}") from error
    if text is None:
        if error is not None:
            logger.warning("rpc.json 无法保留原结构，force_write 整文件标准化写入: %s: %s",
                           config_path, error)
        write_json_document(config_path, dict(updates))
        return
    for key, value in updates.items():
        entries, _close_index = _top_level_entries(text)
        value_text = json.dumps(value, ensure_ascii=False)
        entry = _entry_by_key(entries, key)
        if entry is not None:
            text = text[:entry[2]] + value_text + text[entry[3]:]
        else:
            text = _insert_entry(text, entries, key, value_text)
    for key in deletes:
        entries, _close_index = _top_level_entries(text)
        text = _remove_entry(text, entries, key)
    _atomic_write_text(config_path, text)


def _read_rpc_config_document(path=None):
    """严格读取 rpc.json 文档，返回 dict。

    文件不存在返回 {}；JSON 损坏 / 根节点不是 object 时抛 ValueError
    （调用方自行决定回落还是拒绝写入）；其它 OSError 原样上抛。
    """
    config_path = Path(path) if path is not None else DEFAULT_RPC_CONFIG_PATH
    with CONFIG_LOCK:
        try:
            with config_path.open("r", encoding="utf-8") as config_file:
                raw = config_file.read()
        except FileNotFoundError:
            return {}
        if not raw.strip():
            return {}                      # 空文件视为未配置，直接写不需要 force_write
        try:
            stored = json.loads(raw)
        except json.JSONDecodeError as error:
            raise ValueError(f"RPC 配置文件 JSON 格式错误: {config_path}: {error}") from error
        if not isinstance(stored, dict):
            raise ValueError(f"RPC 配置文件必须包含 JSON object: {config_path}")
        return stored


def load_rpc_config(path=None):
    """读取 rpc.json；任何非法项都回落代码默认值，绝不阻断启动。

    rpc.json 是用户手改的文件：旧键、类型写错、甚至整份 JSON 坏掉都很常见，
    而每个配置在代码里都有默认值兜底，所以这里只记警告、逐键回落默认值。
    严格校验仍保留在 rpc_config(...) 热更新与命令行参数上（显式输入必须报错）。
    """
    try:
        stored = _read_rpc_config_document(path)
    except (ValueError, TypeError, OSError) as error:
        logger.warning("rpc.json 无法解析，全部回落代码默认值：%s", error)
        stored = {}
    return _normalize_rpc_config(stored)


def load_broker_list(path=None, module=None, broker_add=None):
    """broker 直读通道：rpc.json 三键合成后的生效 broker 列表。

    与 server 端 load_rpc_config 同一份合成实现（单一来源），server_mqtt 与
    client 启动读到同样的 broker。module 给定时套用 broker_list_no 的按模块
    屏蔽（见 filter_broker_list）。每次调用现读文件、不缓存，任何解析问题
    只告警回落，绝不阻断启动。

    broker_add 给定时（CLI ``--broker_add host:port`` 显式条目，字符串或
    (host, port) 元组）强制加入最终列表，优先级最高：绕过 broker_list_no
    屏蔽与 broker_list_del 删除；同身份已在列表则不动，合成表（屏蔽前）
    有同身份条目则补回原条目（保留认证），否则追加纯 (host, port)。
    """
    composed = load_rpc_config(path)["broker_list"]
    result = filter_broker_list(composed, module, path=path)
    if not broker_add:
        return result
    return _force_broker_list(result, composed, broker_add)


def parse_broker_add(spec):
    """解析 ``--broker_add`` 的 "host:port" 条目 -> (host, port)。

    无端口时默认 1883；host 允许含冒号的 IPv6（rpartition(":") 拆最后一段，
    与 broker_list_no 的键解析同口径）。非法输入告警返回 None，绝不抛异常。
    """
    if not isinstance(spec, str) or not spec.strip():
        logger.warning("broker_add 条目 %r 非法，已跳过", spec)
        return None
    host, sep, port_text = spec.strip().rpartition(":")
    host = host.strip()
    port_text = port_text.strip()
    if not sep:
        host, port_text = spec.strip(), ""
    if not host:
        logger.warning("broker_add 条目 %r 非法，已跳过", spec)
        return None
    if not port_text:
        return (host, 1883)
    if not port_text.isdecimal():
        logger.warning("broker_add 条目 %r 端口非法，已跳过", spec)
        return None
    port = int(port_text)
    if not 1 <= port <= 65535:
        logger.warning("broker_add 条目 %r 端口超出范围，已跳过", spec)
        return None
    return (host, port)


def _canonical_rpc_key(name):
    """别名 -> 规范键；不是已知字段返回 None。"""
    for canonical, aliases in RPC_CONFIG_ALIASES.items():
        if name == canonical or name in aliases:
            return canonical
    return None


def _parse_explicit_broker_spec(spec):
    """显式输入的 broker 字符串：非法直接报错（不被 parse_broker_add 静默跳过）。"""
    entry = parse_broker_add(spec)
    if entry is None:
        raise ValueError(f"broker 条目非法: {spec!r}（应为 host:port）")
    return entry


def _normalize_broker_alias_value(value):
    """broker_list_add/del 的简写形式归一化。

    接受 ``"host:port"``、``["host:port", ...]``、单条 ``(host, port[, auth])``；
    其它形态（条目列表、None、空列表）原样返回，交给严格校验判定。
    """
    if isinstance(value, str):
        return [_parse_explicit_broker_spec(value)]
    if isinstance(value, tuple):
        value = list(value)
    if isinstance(value, list) and value and all(isinstance(item, str) for item in value):
        return [_parse_explicit_broker_spec(item) for item in value]
    if (isinstance(value, list) and 2 <= len(value) <= 3
            and isinstance(value[0], str)
            and not isinstance(value[1], (list, tuple, dict))):
        return [value]                   # 单条 (host, port[, auth]) 包成列表
    return value


def normalize_rpc_updates(updates):
    """把 rpc_config(...) 的字段别名归一化为规范键。

    只在显式输入入口调用（模块级 / 实例级 rpc_config），rpc.json 文件内容
    不走这里。同一字段同时给出多个取值不同的别名视为冲突，直接报错，
    避免静默取其中之一。
    """
    if not isinstance(updates, dict):
        return updates
    normalized = {}
    sources = {}
    for key, value in updates.items():
        canonical = _canonical_rpc_key(key)
        if canonical is None:
            normalized[key] = value      # 未知键保留，由严格校验统一告警
            continue
        if canonical in sources and normalized[canonical] != value:
            raise ValueError(
                f"rpc_config 收到同一字段的冲突别名: {sources[canonical]} 与 {key}")
        if canonical in ("broker_list_add", "broker_list_del"):
            value = _normalize_broker_alias_value(value)
        normalized[canonical] = value
        sources[canonical] = key
    return normalized


def _force_broker_list(result, composed, broker_add):
    """把 broker_add 显式条目强制并入最终列表（优先级最高，绕过屏蔽/删除）。"""
    forced = list(result)
    have = {_broker_identity(b) for b in forced}
    by_identity = {_broker_identity(b): b for b in composed}
    for spec in broker_add:
        entry = parse_broker_add(spec) if isinstance(spec, str) else spec
        if entry is None:
            continue
        try:
            entry = _normalize_broker_list([entry], "broker_add")[0]
        except ValueError as error:
            logger.warning("broker_add 条目 %r 非法，已跳过：%s", spec, error)
            continue
        identity = _broker_identity(entry)
        if identity in have:
            continue                       # 已在列表（可能带认证）：不动
        forced.append(by_identity.get(identity, entry))
        have.add(identity)
    return forced


def filter_broker_list(brokers, module, path=None):
    """按 rpc.json 的 broker_list_no 对 broker 列表做按模块屏蔽。

    broker_list_no 形如 ``{"192.168.1.20:1883": ["pty"]}``：该 broker 对列出的
    模块不生效（模块名由调用方自报：server/pty/socks5/tools）。屏蔽在三键合成
    之后按身份（host.casefold()+port）匹配，对基线与 add 替换/追加条目统一
    生效；module=None 不屏蔽，未知模块名/非法条目宽松跳过并告警。
    """
    if module is None:
        return list(brokers)
    mask = _read_broker_list_no(path)
    if not mask:
        return list(brokers)
    return [
        broker for broker in brokers
        if module not in mask.get(_broker_identity(broker), ())
    ]


def _read_broker_list_no(path=None):
    """解析 broker_list_no：``{"host:port": [模块名, ...]}`` -> {身份: 模块名集合}。

    可选顶层键（不进 _default_rpc_config，rpc_config() 视图不含它），任何非法
    形态只告警跳过；键按 rsplit(":", 1) 拆 host:port（IPv6 host 也可拆对）。
    """
    try:
        stored = _read_rpc_config_document(path)
    except (ValueError, TypeError, OSError) as error:
        logger.warning("rpc.json 无法解析，broker_list_no 按空处理：%s", error)
        return {}
    raw = stored.get("broker_list_no")
    if raw is None:
        return {}
    if not isinstance(raw, dict):
        logger.warning("broker_list_no 必须是 JSON object，已按空处理")
        return {}
    mask = {}
    for key, modules in raw.items():
        host, sep, port_text = str(key).strip().rpartition(":")
        host = host.strip()
        port_text = port_text.strip()
        if not sep or not host or not port_text.isdecimal():
            logger.warning("broker_list_no 键 %r 不是 host:port，已跳过", key)
            continue
        port = int(port_text)
        if not 1 <= port <= 65535 or not isinstance(modules, list):
            logger.warning("broker_list_no 条目 %r 非法，已跳过", key)
            continue
        names = {
            name.strip() for name in modules
            if isinstance(name, str) and name.strip()
        }
        if names:
            mask.setdefault((host.casefold(), port), set()).update(names)
    return mask


def _sanitize_rpc_document(document, source="rpc.json"):
    """逐键校验配置文档；非法项丢弃并回落代码默认值，不抛异常。

    起点用 _safe_default_rpc_config()：默认值本身写错时先中和掉，
    否则一个坏默认值会让每一条试校验都失败，把所有键一起带崩。
    """
    if not isinstance(document, dict):
        logger.warning("%s 不是 JSON object，全部回落代码默认值", source)
        return _safe_default_rpc_config()
    config = _safe_default_rpc_config()
    known_keys = set(config)
    for key, value in document.items():
        if key not in known_keys:
            continue
        trial = dict(config)
        trial[key] = value
        try:
            _validate_rpc_config(trial)
        except Exception as error:
            logger.warning("%s 的 %s 非法，已回落代码默认值：%s", source, key, error)
            continue
        config[key] = value
    return config


def _valid_stored_request_topic(raw_document, sanitized_document):
    """只把文件里合法生效的 request_topic/request_topic_map 交给 _resolve_request_topic。

    request_topic 缺键或值非法（已被 sanitize 回落默认值）时不返回该键，让
    _resolve_request_topic 继续走机器 ID 兜底，保持原有"首启生成设备 topic
    并持久化"的行为。request_topic_map 原样携带（解析校验在
    _read_request_topic_map），让 map 在服务端解析链同样生效。
    """
    result = {}
    if "request_topic" in raw_document:
        value = raw_document["request_topic"]
        if value == sanitized_document["request_topic"]:
            result["request_topic"] = value
    if "request_topic_map" in raw_document:
        result["request_topic_map"] = raw_document["request_topic_map"]
    return result


def _validate_explicit_updates(updates):
    """只严格校验用户显式传入的键；文件/默认值的脏数据不在此报错。"""
    defaults = _safe_default_rpc_config()
    for key, value in updates.items():
        if key not in defaults:
            logger.warning("rpc_config 收到未知配置键，已忽略：%s", key)
            continue
        trial = dict(defaults)
        trial[key] = value
        _validate_rpc_config(trial)


def _write_rpc_config_updates(updates, path=None, force_write=False):
    """只持久化用户显式修改的项，避免把内置 broker 凭据复制进配置文件。

    force_write 语义与 :func:`update_json_document` 一致：正确或空 rpc.json
    直接写；非空错误文件默认抛 ValueError 拒绝覆盖，force_write=True 才重写。
    """
    config_path = Path(path) if path is not None else DEFAULT_RPC_CONFIG_PATH
    config_path.parent.mkdir(parents=True, exist_ok=True)
    with CONFIG_LOCK:
        try:
            with config_path.open("r", encoding="utf-8") as config_file:
                raw = config_file.read()
            stored = json.loads(raw) if raw.strip() else {}
        except FileNotFoundError:
            stored = {}
        except json.JSONDecodeError as error:
            # 非空错误文件：是否覆盖由 update_json_document 的 force_write 把关，
            # 这里只按空配置算出写回内容，绝不静默重写用户文件。
            logger.warning("rpc.json JSON 格式错误: %s: %s", config_path, error)
            stored = {}
        if not isinstance(stored, dict):
            logger.warning("rpc.json 不是 JSON object，按空配置处理: %s", config_path)
            stored = {}
        # 校验用净化后的配置（脏键回落默认值），写回仍以用户文件的原始键为准
        source = _sanitize_rpc_document(stored, config_path)
        source.update(updates)
        normalized = _normalize_rpc_config(source)
        serialized = _json_rpc_config(normalized)
        patch = {}
        for key in updates:
            if key == "broker_list":
                patch[key] = _json_brokers(normalized["_broker_list_base"])
            elif key == "pub_key" and updates[key] == "":
                patch[key] = ""
            elif key in serialized:
                patch[key] = serialized[key]
        # 文本级按键改写：只动被改键的一行，保留用户手写缩进与其余内容
        update_json_document(config_path, patch, force_write=force_write)


def _read_request_topic_map(stored):
    """解析 request_topic_map：{"设备id": "别名/topic"} -> {设备id: topic}。

    可选顶层键（不进 _default_rpc_config），供多机共用一份 rpc.json 时按
    U.get_machine_id() 输出的设备 id 区别各机器的 topic；key/value 都须是
    非空字符串，非法条目告警跳过，永不抛异常。
    """
    raw = stored.get("request_topic_map") if isinstance(stored, dict) else None
    if raw is None:
        return {}
    if not isinstance(raw, dict):
        logger.warning("request_topic_map 必须是 JSON object，已按空处理")
        return {}
    topic_map = {}
    for machine_id, topic in raw.items():
        if (isinstance(machine_id, str) and machine_id.strip()
                and isinstance(topic, str) and topic.strip()):
            topic_map[machine_id.strip()] = topic.strip()
        else:
            logger.warning("request_topic_map 条目 %r 非法，已跳过",
                           (machine_id, topic))
    return topic_map


def _machine_id_or_none():
    """本机设备 id（request_topic_map 的键）；拿不到返回 None，绝不抛异常。"""
    try:
        import U
        machine_id = U.get_machine_id()
        if not isinstance(machine_id, str) or not machine_id.strip():
            return None
        return machine_id.strip()
    except Exception as error:
        logger.warning("获取设备 id 失败（request_topic_map 按未命中处理）：%s", error)
        return None


def _mapped_request_topic(stored):
    """request_topic_map[设备id]：命中返回 topic，否则 None（不写回文件）。"""
    topic_map = _read_request_topic_map(stored)
    if not topic_map:
        return None                      # 空表不调 U.get_machine_id()，零回归
    machine_id = _machine_id_or_none()
    return topic_map.get(machine_id) if machine_id else None


def _resolve_request_topic(request_topic=None, stored=None, path=None):
    if request_topic is not None:
        return request_topic

    if stored is None:
        try:
            stored = _read_rpc_config_document(path)
        except (ValueError, TypeError, OSError) as error:
            logger.warning("rpc.json 无法解析，按未配置 request_topic 处理：%s", error)
            stored = {}

    # request_topic_map 优先级高于 request_topic（多机共用 rpc.json 区别机器），
    # 命中只读不写回，避免共用配置被本机 topic 污染。
    mapped = _mapped_request_topic(stored)
    if mapped is not None:
        return mapped

    if "request_topic" in stored:
        return stored["request_topic"]

    try:
        import U
        machine_topic = U.get_machine_id()
        if not isinstance(machine_topic, str) or not machine_topic.strip():
            raise ValueError("get_machine_id() 未返回有效 topic")
        machine_topic = machine_topic.strip()
    except Exception as error:
        logger.warning(
            "获取设备 request_topic 失败，回退到内置默认值 %s: %s",
            REQUEST_TOPIC,
            error,
        )
        return REQUEST_TOPIC

    try:
        _write_rpc_config_updates({"request_topic": machine_topic}, path)
    except (ValueError, OSError) as error:
        # 唯一自动写入点：非空错误文件按 force_write 规则拒绝覆盖（保护用户
        # 手写内容），topic 照常在内存生效，只是不持久化，绝不阻断启动。
        logger.warning("request_topic 未持久化（rpc.json 拒绝覆盖）: %s", error)
    return machine_topic


def resolve_client_request_topic(request_topic=None, path=None):
    """客户端 request_topic 默认值解析：显式 > map[设备id] > request_topic > 内置默认。

    与服务端 _resolve_request_topic 的区别：客户端不生成机器 ID、不写回文件，
    未配置时直接用内置默认值 REQUEST_TOPIC，保持兼容。
    """
    if request_topic is not None:
        return request_topic
    try:
        stored = _read_rpc_config_document(path)
    except (ValueError, TypeError, OSError) as error:
        logger.warning("rpc.json 无法解析，按未配置 request_topic 处理：%s", error)
        stored = {}
    mapped = _mapped_request_topic(stored)
    if mapped is not None:
        return mapped
    if "request_topic" in stored:
        return stored["request_topic"]
    return REQUEST_TOPIC


def _merge_rpc_updates(current, updates):
    """以某个实例当前生效配置为基线叠加显式更新，返回严格校验后的候选配置。

    broker_list 走 _broker_list_base 语义：基线里 broker_list 是"生效结果"，
    叠加时要先还原成 base，再按 broker_list_add / broker_list_del 重新合成。
    """
    merged = dict(current)
    merged["broker_list"] = merged["_broker_list_base"]
    if "broker_list" in updates:
        merged["_broker_list_base"] = updates["broker_list"]
    return _normalize_rpc_config({**merged, **updates})
