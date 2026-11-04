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


def write_json_document(path, document):
    """原子写 JSON 文档（server_mqtt 配置写回 / server_http 别名持久化共用）。

    同目录临时文件 + fsync + ``os.replace`` 并发请求不会读到半截 JSON；
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
            json.dump(document, config_file, ensure_ascii=False, indent=2)
            config_file.write("\n")
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


def _read_rpc_config_document(path=None):
    """严格读取 rpc.json 文档，返回 dict。

    文件不存在返回 {}；JSON 损坏 / 根节点不是 object 时抛 ValueError
    （调用方自行决定回落还是拒绝写入）；其它 OSError 原样上抛。
    """
    config_path = Path(path) if path is not None else DEFAULT_RPC_CONFIG_PATH
    with CONFIG_LOCK:
        try:
            with config_path.open("r", encoding="utf-8") as config_file:
                stored = json.load(config_file)
        except FileNotFoundError:
            stored = {}
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


def load_broker_list(path=None, module=None):
    """broker 直读通道：rpc.json 三键合成后的生效 broker 列表。

    与 server 端 load_rpc_config 同一份合成实现（单一来源），server_mqtt 与
    client 启动读到同样的 broker。module 给定时套用 broker_list_no 的按模块
    屏蔽（见 filter_broker_list）。每次调用现读文件、不缓存，任何解析问题
    只告警回落，绝不阻断启动。
    """
    return filter_broker_list(load_rpc_config(path)["broker_list"], module, path=path)


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
    """只把文件里合法生效的 request_topic 交给 _resolve_request_topic。

    缺键或值非法（已被 sanitize 回落默认值）时返回空 dict，让 _resolve_request_topic
    继续走机器 ID 兜底，保持原有"首启生成设备 topic 并持久化"的行为。
    """
    if "request_topic" in raw_document:
        value = raw_document["request_topic"]
        if value == sanitized_document["request_topic"]:
            return {"request_topic": value}
    return {}


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


def _write_rpc_config_updates(updates, path=None):
    """只持久化用户显式修改的项，避免把内置 broker 凭据复制进配置文件。"""
    config_path = Path(path) if path is not None else DEFAULT_RPC_CONFIG_PATH
    config_path.parent.mkdir(parents=True, exist_ok=True)
    with CONFIG_LOCK:
        try:
            with config_path.open("r", encoding="utf-8") as config_file:
                stored = json.load(config_file)
        except FileNotFoundError:
            stored = {}
        except json.JSONDecodeError as error:
            # 文件坏了也要能写回（如首启生成本机 topic）：按空配置重写，
            # 否则脏 rpc.json 会让启动路径抛异常。
            logger.warning("rpc.json JSON 格式错误，按空配置重写: %s: %s", config_path, error)
            stored = {}
        if not isinstance(stored, dict):
            logger.warning("rpc.json 不是 JSON object，按空配置重写: %s", config_path)
            stored = {}
        # 校验用净化后的配置（脏键回落默认值），写回仍以用户文件的原始键为准
        source = _sanitize_rpc_document(stored, config_path)
        source.update(updates)
        normalized = _normalize_rpc_config(source)
        serialized = _json_rpc_config(normalized)
        for key in updates:
            if key == "broker_list":
                stored[key] = _json_brokers(normalized["_broker_list_base"])
            elif key == "pub_key" and updates[key] == "":
                stored[key] = ""
            elif key in serialized:
                stored[key] = serialized[key]
        write_json_document(config_path, stored)


def _resolve_request_topic(request_topic=None, stored=None, path=None):
    if request_topic is not None:
        return request_topic

    if stored is None:
        try:
            stored = _read_rpc_config_document(path)
        except (ValueError, TypeError, OSError) as error:
            logger.warning("rpc.json 无法解析，按未配置 request_topic 处理：%s", error)
            stored = {}
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

    _write_rpc_config_updates({"request_topic": machine_topic}, path)
    return machine_topic


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
