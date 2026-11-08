#!/usr/bin/env python3
import copy
import json
import logging
import os
import tempfile
import threading
import time
import sys
import types
import weakref
from pathlib import Path
from urllib.parse import urlsplit

from ecdsa import VerifyingKey
from multi_mqtt import (
    BROKER_LIST,
    ConnectionQualityStats,
    MultiMQTTManager,
    _describe_public_key,
    get_standard_public_pem_bytes,
)
from rpc_executor import utc_ms,stime
from rpc_executor import PythonExecutor#, format_result
import rpc_config as _rpc_config  # 本模块下方 rpc_config=set_rpc_config 别名赋值会覆盖同名模块名，引用模块一律用 _rpc_config
# rpc.json 配置层（默认值 / 校验 / 容错读 / 原子写 / 统一锁）已抽离到 rpc_config.py；
# re-export 全部原名字，保持模块接口与远端 RPC 命名空间（PythonExecutor(globals=globals)）不变。
from rpc_config import (
    DEFAULT_GLOBAL_TOPIC_LIST,
    DEFAULT_GLOBAL_TOPIC_LIST_CHECK_INTERVAL,
    DEFAULT_GLOBAL_TOPIC_LIST_IDLE_THRESHOLD,
    DEFAULT_GLOBAL_TOPIC_LIST_INTERVAL,
    DEFAULT_GLOBAL_TOPIC_LIST_PUBLISH_KWARGS,
    DEFAULT_GLOBAL_TOPIC_URL_LIST,
    DEFAULT_PUB_KEY,
    DEFAULT_STATS_PING_INTERVAL,
    DEFAULT_STATS_PRINT_INTERVAL,
    REPORT_BODY_TOKEN,
    REQUEST_TOPIC,
    _broker_identity,
    _default_global_topic_url_list,
    _default_rpc_config,
    _json_brokers,
    _json_rpc_config,
    _merge_rpc_updates,
    _normalize_broker_list,
    _normalize_global_topic_url_list,
    _normalize_non_negative_seconds,
    _normalize_positive_seconds,
    _normalize_rpc_config,
    _safe_default_rpc_config,
    _safe_global_topic_url_list,
    _sanitize_rpc_document,
    _valid_stored_request_topic,
    _validate_explicit_updates,
    _validate_rpc_config,
    filter_broker_list,
    load_broker_list,
)

# --- 配置常量写穿 ---
# 顶层 re-export 的配置常量（DEFAULT_* / REQUEST_TOPIC 等）真实现住在 rpc_config，
# 但 mock.patch.object(server_mqtt, "DEFAULT_...") / 手工赋值这类"改常量"操作发生
# 在本模块命名空间上；换成 ModuleType 子类拦截 __setattr__，把这些常量的修改实时
# 同步进 rpc_config，保证默认值 / 校验链看到的永远是当前生效值（接口行为不变）。
_SYNCED_CONFIG_CONSTS = frozenset({
    'REQUEST_TOPIC', 'DEFAULT_PUB_KEY',
    'DEFAULT_GLOBAL_TOPIC_LIST', 'DEFAULT_GLOBAL_TOPIC_LIST_INTERVAL',
    'DEFAULT_GLOBAL_TOPIC_LIST_IDLE_THRESHOLD', 'DEFAULT_GLOBAL_TOPIC_LIST_CHECK_INTERVAL',
    'DEFAULT_GLOBAL_TOPIC_LIST_PUBLISH_KWARGS', 'REPORT_BODY_TOKEN',
    'DEFAULT_GLOBAL_TOPIC_URL_LIST',
    'DEFAULT_STATS_PRINT_INTERVAL', 'DEFAULT_STATS_PING_INTERVAL',
})


class _ServerMqttModule(types.ModuleType):
    def __setattr__(self, name, value):
        super().__setattr__(name, value)
        if name in _SYNCED_CONFIG_CONSTS:
            setattr(_rpc_config, name, value)


sys.modules[__name__].__class__ = _ServerMqttModule

logger = logging.getLogger("Server")
DEFAULT_REPLY_TOPIC = "sys/device/response"

RPC_CONFIG_PATH = Path(__file__).with_name("rpc.json")
# 读-改-写 rpc.json 的统一锁：与 server_http 别名持久化共用同一把（rpc_config.CONFIG_LOCK），
# 跨模块写同一文件的读-改-写串行，消除两把锁时的丢失更新；reload 安全由 setdefault 保证。
_RPC_CONFIG_LOCK = _rpc_config.CONFIG_LOCK
gms = globals().setdefault('gms', None)  # 当前活着的服务端实例（构造/启动时自注册 gms = self），reload 重执行把名字重绑回 None——rpc_config()/apply 热配置路径（324-352 行）静默失效直到下次实例化；setdefault 让 reload 后仍指向活着的实例
# 全部活着的服务端实例（弱引用，停止/回收后自动消失）：模块级 rpc_config()/
# rpc_config_get() 对每个实例都生效，不再只认最后构造的 gms。
_RPC_SERVERS = globals().setdefault('_RPC_SERVERS', weakref.WeakSet())

# [global_topic_list 监听 topic 定期报告 / 连接质量统计周期] 的默认配置常量与
# rpc.json 默认值模板、校验、读写逻辑一起抽离到 rpc_config.py，顶部 re-export 原名。


# --- rpc.json 配置层已抽离到 rpc_config.py（默认值 / 校验 / 容错读 / 原子写 /
# 统一锁）。以下薄封装保留原函数名与签名，并在调用时把模块级 RPC_CONFIG_PATH
# （测试 / 部署打补丁注入、rpc_config(path=...) 重绑）晚绑定解析成显式路径，
# 接口与行为与抽离前完全一致。---

def _read_rpc_config_document(path=None):
    return _rpc_config._read_rpc_config_document(
        RPC_CONFIG_PATH if path is None else path)


def load_rpc_config(path=None):
    return _rpc_config.load_rpc_config(
        RPC_CONFIG_PATH if path is None else path)


def _write_rpc_config_updates(updates, path=None, force_write=False):
    return _rpc_config._write_rpc_config_updates(
        updates, RPC_CONFIG_PATH if path is None else path,
        force_write=force_write)


def _resolve_request_topic(request_topic=None, stored=None):
    return _rpc_config._resolve_request_topic(
        request_topic, stored, path=RPC_CONFIG_PATH)


def _live_servers():
    """当前活着的 MQTTServer 实例（弱引用，回收后自动消失）。

    模块级 rpc_config()/rpc_config_get() 对每个实例都生效，而不是只认最后
    构造的 gms；gms 仅作兼容兜底（老代码/测试可能手动赋值）。
    """
    servers = [server for server in list(_RPC_SERVERS) if server is not None]
    if not servers and gms is not None:
        servers = [gms]
    return servers


def _primary_server(servers=None):
    """取一个代表实例：优先 gms，其次最近注册的实例；没有实例返回 None。"""
    servers = _live_servers() if servers is None else servers
    if gms is not None and gms in servers:
        return gms
    return servers[-1] if servers else None


def set_rpc_config(*, path=None, force_write=False, **updates):
    """持久化并立即应用到**所有**存活的服务端实例。

    ``force_write``：写入语义见 ``rpc_config.update_json_document``——正确 JSON
    或空文件/缺失文件直接写（文本级按键改写保留原格式）；非空且非法的错误文件
    默认抛 ``ValueError`` 拒绝覆盖（保护用户手写内容），只有 ``force_write=True``
    才退化为整文件标准化写入。

    三种写法：
    1. ``rpc_config(request_topic="new")``：热应用到所有实例并写入默认文件
       ``RPC_CONFIG_PATH``；
    2. ``rpc_config(path=..., request_topic="new")``：热应用并写入该文件，
       本次不改变默认文件位置；
    3. ``rpc_config(path=...)``（只有 path）：把该文件读入内存并热应用到
       所有实例，同时把模块默认位置与各实例的 ``rpc_config_path`` 更新为它。
       broker 按 host:port 逐条比对：完全相同的条目原样保留（连接不断开），
       只有不同/新增/缺失的条目才重建或关闭。

    动态只更新一条 broker（其它 broker 连接完全不受影响）：
        rpc_config(broker_list_add=[("public.runmqtt.com", 1883,
                                     ["ajbkvbp/device", "新密码"])])
    broker 身份按 host:port（host 大小写不敏感）：同 host 换密码/用户名就是
    原位替换那一条，不会新增重复条目，也不会动其它 broker 的连接。
    """
    global gms, RPC_CONFIG_PATH
    if not updates:
        if path is None:
            return rpc_config_get()
        # 只有 path：读入该文件到内存并热应用，不写文件
        config_path = Path(path)
        if not config_path.exists():   # 显式给的路径，写错就当场报错
            raise FileNotFoundError(f"rpc 配置文件不存在: {config_path}")
        candidate = load_rpc_config(config_path)
        servers = _live_servers()
        if servers:
            previous = [server._rpc_config_snapshot() for server in servers]
            applied = []
            try:
                for server in servers:
                    server.apply_rpc_config(candidate)
                    applied.append(server)
            except Exception:
                for server, prev in zip(applied, previous):
                    try:
                        server.apply_rpc_config(prev)
                    except Exception:
                        logger.exception("rpc_config 回滚失败，实例配置可能已部分生效")
                raise
            RPC_CONFIG_PATH = config_path
            for server in servers:
                server.rpc_config_path = config_path
            logger.info("配置文件位置已更新到内存并已热应用: %s（实例 %d 个）",
                        config_path, len(servers))
            return _primary_server(servers).rpc_config_get()
        RPC_CONFIG_PATH = config_path
        logger.info("配置文件位置已更新到内存: %s（暂无存活实例）", config_path)
        return _json_rpc_config(candidate)
    # 显式输入先做字段别名归一化（add_broker/topic/pub ... -> 规范键），
    # 让模块级 rpc_config 与实例级 gms.rpc_config 支持同一套别名。
    updates = _rpc_config.normalize_rpc_updates(updates)
    config_path = Path(path) if path is not None else RPC_CONFIG_PATH
    with _RPC_CONFIG_LOCK:
        _validate_explicit_updates(updates)   # 显式传错立刻报错，不静默回落
        servers = _live_servers()
        if servers:
            previous = [server._rpc_config_snapshot() for server in servers]
            candidates = [_merge_rpc_updates(prev, updates) for prev in previous]
        else:
            # 服务未启动时按 rpc.json（脏值已逐键回落默认值）算出当前配置
            previous = []
            candidates = [_merge_rpc_updates(load_rpc_config(config_path), updates)]
        applied = []
        try:
            for server, candidate in zip(servers, candidates):
                server.apply_rpc_config(candidate)
                applied.append(server)
            _write_rpc_config_updates(updates, config_path, force_write=force_write)
        except Exception:
            for server, prev in zip(applied, previous):
                try:
                    server.apply_rpc_config(prev)
                except Exception:
                    logger.exception("rpc_config 回滚失败，实例配置可能已部分生效")
            raise
        primary = _primary_server(servers)
        return primary.rpc_config_get() if primary else _json_rpc_config(candidates[-1])
rpc_config=set_config=set_rpc=rpc_set_config=rpc_config_set=set_rpc_config

def get_rpc_config():
    """返回实际生效的设置，只读、无副作用。

    取存活实例内存中的最新配置；没有实例时回退 rpc.json + 代码默认值。
    """
    with _RPC_CONFIG_LOCK:
        primary = _primary_server()
        if primary is not None:
            return primary.rpc_config_get()
        return _json_rpc_config(load_rpc_config())
get_config=get_rpc=rpc_get_config=rpc_config_get=get_rpc_config



def _rpc_config_with_cli_overrides(
    request_topic=None, pub=None, brokers=None, global_topic_url_list=None
):
    """按命令行参数覆盖配置文件，并校验最终 MQTT 配置。

    文件里的脏值先逐键回落默认值（同 load_rpc_config），只有命令行显式传入
    的非法值才会报错退出。
    """
    try:
        raw_stored = _read_rpc_config_document()
    except (ValueError, TypeError, OSError) as error:
        logger.warning("rpc.json 无法解析，按代码默认值继续：%s", error)
        raw_stored = {}
    stored = _sanitize_rpc_document(raw_stored, RPC_CONFIG_PATH)
    config = _normalize_rpc_config(stored)
    config["broker_list"] = config["_broker_list_base"]
    config["request_topic"] = _resolve_request_topic(
        request_topic, _valid_stored_request_topic(raw_stored, stored)
    )
    if pub is not None:
        config["pub_key"] = " ".join(pub) if isinstance(pub, (list, tuple)) else pub
    if brokers is not None:
        config["broker_list"] = []
        config["broker_list_add"] = []
        config["broker_list_del"] = []
        for broker in brokers:
            entry = _rpc_config.parse_broker_add(broker)
            if entry is None:
                raise ValueError(
                    f"broker 格式必须是 [user:pass@]host[:port]: {broker!r}")
            config["broker_list"].append(entry)
        config["_broker_list_base"] = config["broker_list"]
    if global_topic_url_list is not None:
        config["global_topic_url_list"] = global_topic_url_list
    # 命令行显式传参仍严格：传错直接报错退出
    return _validate_rpc_config(config)


def _is_verify_enabled(mqtt_net):
    """
    判断当前 MQTT 网络层是否真的处于"验签"状态。

    MultiMQTTManager 里的关键属性：
      - server_public_key_bytes: 已解析成 PEM bytes（None 表示未配置）
      - server_vk:              ecdsa.VerifyingKey 对象（None 表示未启用）

    公钥配置但解析失败的情况在 MultiMQTTManager.__init__ 里就会让
    ``VerifyingKey.from_pem`` 直接抛异常（构造即失败、fail-closed），
    因此不存在"invalid 标志"这条路径。

    返回 (enabled: bool, reason: str)
    """
    if getattr(mqtt_net, "server_vk", None) is not None:
        return True, "启用"
    if getattr(mqtt_net, "server_public_key_bytes", None):
        # 有 PEM bytes 但没有 vk，说明构造时被异常吞掉过，需要显式暴露
        return False, "公钥已配置但 VerifyingKey 未初始化！"
    return False, "关闭（接收所有消息）"


class MQTTServer:
    def __init__(
        self,
        globals=None,
        server_public_key_bytes=None,
        brokers=None,
        request_topic=None,
        reply_topic=DEFAULT_REPLY_TOPIC,
        keepalive=None,
        max_reconnect_delay=None,
        global_topic_list=None,
        global_topic_list_interval=None,
        global_topic_list_idle_threshold=None,
        global_topic_list_check_interval=None,
        global_topic_list_publish_kwargs=None,
        global_topic_list_payload_fn=None,
        global_topic_url_list=None,
        stats_print_interval=None,
        stats_ping_interval=None,
        rpc_config_source=None,
        rpc_config_path=None,
    ):
        # 本实例默认读写的配置文件（对象级 rpc_config(path=...) 可临时改用别的）
        self.rpc_config_path = (
            Path(rpc_config_path) if rpc_config_path is not None else RPC_CONFIG_PATH
        )
        if brokers is None:
            # 直读通道：与启动配置同源合成（rpc.json 三键），并套用 broker_list_no
            # 对 server 模块的屏蔽；显式传 brokers=（含 []）原样使用。
            brokers = load_broker_list(path=self.rpc_config_path, module="server")
        # 注册进弱引用集合：模块级 rpc_config()/rpc_config_get() 对每个实例生效
        _RPC_SERVERS.add(self)
        # 实例化网络层管理器（enable_crypto 默认为 False）
        if request_topic is None:
            if rpc_config_source is not None and "request_topic" in rpc_config_source:
                request_topic = rpc_config_source["request_topic"]
            else:
                request_topic = _resolve_request_topic()
        self.request_topic = request_topic
        self.reply_topic = reply_topic
        self._net_options = {}

        # 公钥解析统一由 MultiMQTTManager 内部完成（含 OpenSSH -> PEM 转换）。
        # 这里只把原始输入透传进去，不再重复解析，避免结果不一致。
        net_kwargs = {}
        if keepalive is not None:
            net_kwargs["keepalive"] = keepalive
        if max_reconnect_delay is not None:
            net_kwargs["max_reconnect_delay"] = max_reconnect_delay
        self._net_options = dict(net_kwargs)
        self._rpc_config = _normalize_rpc_config(
            rpc_config_source
            if rpc_config_source is not None
            else {
                "broker_list": brokers,
                "request_topic": request_topic,
                "pub_key": server_public_key_bytes,
            }
        )
        self._rpc_config_lock = threading.RLock()
        # 生效优先级：显式构造参数 > rpc.json（rpc_config_source）> 模块默认值
        self.stats_print_interval = (
            self._rpc_config["stats_print_interval"]
            if stats_print_interval is None
            else _normalize_non_negative_seconds(
                stats_print_interval, "stats_print_interval")
        )
        self.stats_ping_interval = (
            self._rpc_config["stats_ping_interval"]
            if stats_ping_interval is None
            else _normalize_non_negative_seconds(
                stats_ping_interval, "stats_ping_interval")
        )
        self.mqtt_net = MultiMQTTManager(
            brokers=brokers,
            log_messages=False,  # 每个 broker 不打印原始消息
            server_public_key_bytes=server_public_key_bytes,
            enable_stats=True,
            stats_print_interval=self.stats_print_interval,
            stats_ping_interval=self.stats_ping_interval,
            **net_kwargs,
        )
        
        self.mqtt_net.set_on_message(self.handle_message)
        self.executor = PythonExecutor(globals=globals)

        # --- [global_topic_list 定期报告] 状态（tick 由 _dispatch_loop 周期驱动，不另开线程） ---
        # 生效优先级：显式构造参数 > rpc.json（rpc_config_source）> 模块默认值；
        # 该报告默认常开，不再有 enable 开关；远端上报由 global_topic_url_list=[] 关闭
        self.global_topic_list = (
            self._rpc_config["global_topic_list"]
            if global_topic_list is None
            else str(global_topic_list)
        )
        self.global_topic_list_interval = (
            self._rpc_config["global_topic_list_interval"]
            if global_topic_list_interval is None
            else float(global_topic_list_interval)
        )
        # 距上次下发指令超过该秒数视为空闲（默认 24 小时，可配置），恢复每小时发布
        self.global_topic_list_idle_threshold = (
            self._rpc_config["global_topic_list_idle_threshold"]
            if global_topic_list_idle_threshold is None
            else float(global_topic_list_idle_threshold)
        )
        self.global_topic_list_check_interval = (
            self._rpc_config["global_topic_list_check_interval"]
            if global_topic_list_check_interval is None
            else float(global_topic_list_check_interval)
        )
        # 发布参数缺省 QoS1+Retain，经 **ka 一路透传到 paho-mqtt
        self.global_topic_list_publish_kwargs = (
            dict(self._rpc_config["global_topic_list_publish_kwargs"])
            if global_topic_list_publish_kwargs is None
            else dict(global_topic_list_publish_kwargs)
        )
        # 报告依赖 QoS1+Retain 的 broker 侧保留语义（新订阅者上线即收最新
        # 列表）；自定义发布参数无论传什么，qos 恒为 1，不允许静默跌回 QoS0。
        self.global_topic_list_publish_kwargs["qos"] = 1
        # 自定义报告函数: fn(server) -> dict；None 则用默认的监听 topic 列表
        self.global_topic_list_payload_fn = global_topic_list_payload_fn
        self.global_topic_url_list = (
            list(self._rpc_config["global_topic_url_list"])
            if global_topic_url_list is None
            else _safe_global_topic_url_list(global_topic_url_list)
        )
        self._rpc_config.update({
            "global_topic_list": self.global_topic_list,
            "global_topic_url_list": list(self.global_topic_url_list),
            "global_topic_list_interval": self.global_topic_list_interval,
            "global_topic_list_idle_threshold": self.global_topic_list_idle_threshold,
            "global_topic_list_check_interval": self.global_topic_list_check_interval,
            "global_topic_list_publish_kwargs": dict(self.global_topic_list_publish_kwargs),
            "stats_print_interval": self.stats_print_interval,
            "stats_ping_interval": self.stats_ping_interval,
        })
        self._last_gtl_publish_ts = 0.0           # 上次发布报告的时间，0=尚未发布过
        self.last_command_ts = 0.0                # 最近一次收到控制指令的时间，0=从未收到

        verify_enabled, reason = _is_verify_enabled(self.mqtt_net)
        if verify_enabled:
            logger.debug(
                "MQTTServer 初始化完成: 验签已启用, mqtt_pub_key=%s",
                _describe_public_key(self.mqtt_net.server_public_key_bytes),
            )
        else:
            logger.debug("MQTTServer 初始化完成: 验签未启用, 原因=%s", reason)

    def _rpc_config_snapshot(self):
        """内部用：含 _broker_list_base 的原始配置深拷贝（供合并/回滚）。"""
        with self._rpc_config_lock:
            return copy.deepcopy(self._rpc_config)

    def get_rpc_config(self):
        """唯一 getter：读取本实例内存中的最新生效配置，只读、无副作用。

        get_config / get_rpc / get_rpc_config / rpc_get_config 都是它的别名，
        不在这里做任何文件读取或配置合并。
        """
        return _json_rpc_config(self._rpc_config_snapshot())

    def set_rpc_config(self, *, path=None, force_write=False, **updates):
        """对象级 set：写入/加载配置。

        ``force_write`` 语义同模块级 ``set_rpc_config``：正确 JSON 或空文件/
        缺失文件直接写；非空且非法的错误文件默认抛 ``ValueError`` 拒绝覆盖，
        仅 ``force_write=True`` 时整文件标准化写入。

        1. ``rpc_config(request_topic="new")``：热应用并写入本实例默认文件
           ``self.rpc_config_path``；
        2. ``rpc_config(path=..., request_topic="new")``：热应用并把这一项
           写入指定文件，本次不改变实例默认文件位置；
        3. ``rpc_config(path=...)``（只有 path）：把该文件读入内存并热应用，
           同时把默认文件位置更新为该文件。已存在的 broker 按 host:port
           逐条比对：完全相同的条目原样保留（连接不断开），只有不同/新增/
           缺失的条目才重建或关闭。
        """
        if not updates:
            if path is None:
                return self.rpc_config_get()
            config_path = Path(path)
            if not config_path.exists():   # 显式给的路径，写错就当场报错
                raise FileNotFoundError(f"rpc 配置文件不存在: {config_path}")
            with _RPC_CONFIG_LOCK:
                previous = self._rpc_config_snapshot()
                candidate = load_rpc_config(config_path)
                try:
                    self.apply_rpc_config(candidate)
                except Exception:
                    self.apply_rpc_config(previous)
                    raise
                self.rpc_config_path = config_path
            logger.info("配置文件位置已更新到内存并已热应用: %s", config_path)
            return self.rpc_config_get()
        # 显式输入先做字段别名归一化，与模块级 rpc_config 行为一致
        updates = _rpc_config.normalize_rpc_updates(updates)
        config_path = Path(path) if path is not None else self.rpc_config_path
        with _RPC_CONFIG_LOCK:
            _validate_explicit_updates(updates)   # 显式传错立刻报错
            previous = self._rpc_config_snapshot()
            candidate = _merge_rpc_updates(previous, updates)
            self.apply_rpc_config(candidate)
            try:
                _write_rpc_config_updates(updates, config_path, force_write=force_write)
            except Exception:
                self.apply_rpc_config(previous)
                raise
            return self.rpc_config_get()

    # get 只保留一个实现，其余名字统一指向它（set 同理）
    get_config=get_rpc=rpc_get_config=rpc_config_get=get_rpc_config
    rpc_config=set_config=set_rpc=rpc_set_config=rpc_config_set=set_rpc_config

    def _apply_network_public_key(self, pub_key):
        """热更新验签公钥，不碰任何 MQTT 连接。

        公钥只在接收消息验签时用到（network.server_vk），与 broker/端口/
        口令完全无关，所以换公钥绝不能重连：优先让网络层自己原子替换，
        老网络层则直接改属性。
        """
        updater = getattr(self.mqtt_net, "update_server_public_key", None)
        if callable(updater):
            updater(pub_key)
            return
        pem = get_standard_public_pem_bytes(pub_key)
        self.mqtt_net.server_vk = VerifyingKey.from_pem(pem) if pem else None
        self.mqtt_net.server_public_key_bytes = pem

    def apply_rpc_config(self, config):
        """更新服务设置，尽量不动已有连接。

        broker 条目变化且网络层支持 update_brokers 时走原位热更新：同 host
        换密码/端口只重建那一条 client，其它连接、分发线程、stats、周期
        任务完全不受影响。公钥只在收包验签时使用、不参与建连，因此换公钥
        只热替换 network.server_vk，一条连接都不动、更不重建 manager。

        global_topic_list 报告参数（间隔/阈值/tick 粒度）随之热生效：
        check_interval 变更时在当前 manager 上重注册周期任务。
        """
        global gms
        candidate = _normalize_rpc_config(config)
        with self._rpc_config_lock:
            previous = self._rpc_config
            broker_changed = candidate["broker_list"] != previous["broker_list"]
            key_changed = candidate["pub_key"] != previous["pub_key"]
            topic_changed = candidate["request_topic"] != previous["request_topic"]
            gtl_old_check_interval = self.global_topic_list_check_interval
            hot_broker_update = (
                broker_changed
                # 只对已启动（有 client、分发线程在跑）的网络层做增量更新；
                # 未启动时仍走整表替换 + start，避免连上却没人分发消息
                and bool(getattr(self.mqtt_net, "clients", None))
                and callable(getattr(self.mqtt_net, "update_brokers", None))
            )
            if broker_changed and not hot_broker_update:
                replacement = MultiMQTTManager(
                    brokers=candidate["broker_list"],
                    log_messages=False,
                    server_public_key_bytes=candidate["pub_key"],
                    enable_stats=True,
                    stats_print_interval=candidate["stats_print_interval"],
                    stats_ping_interval=candidate["stats_ping_interval"],
                    **self._net_options,
                )
                replacement.set_on_message(self.handle_message)
                try:
                    replacement.start()
                    if not replacement.clients:
                        raise RuntimeError("新 MQTT 网络管理器未能创建任何 broker 连接")
                    replacement.subscribe(candidate["request_topic"])
                except Exception:
                    replacement.stop()
                    raise
                previous_net = self.mqtt_net
                self.mqtt_net = replacement
                self.request_topic = candidate["request_topic"]
                self._apply_global_topic_list_settings(candidate)
                self._rpc_config = candidate
                replacement.set_periodic_task(
                    self.global_topic_list_tick,
                    interval=self.global_topic_list_check_interval,
                )
                previous_net.stop()
            else:
                if hot_broker_update:
                    # 只改 broker：单条重建，其它 broker 的连接保持原样
                    self.mqtt_net.update_brokers(candidate["broker_list"])
                if key_changed:
                    # 公钥只用于收包验签，热替换即可，不重连、不重建 manager
                    self._apply_network_public_key(candidate["pub_key"])
                if topic_changed:
                    self.mqtt_net.subscribe(candidate["request_topic"])
                    self.mqtt_net.unsubscribe(previous["request_topic"])
                self.request_topic = candidate["request_topic"]
                self._apply_global_topic_list_settings(candidate)
                self._rpc_config = candidate
                if self.global_topic_list_check_interval != gtl_old_check_interval:
                    self._register_global_topic_list_tick()   # set_periodic_task 覆盖式注册
            self._apply_stats_settings(candidate)
            gms = self

    def _apply_stats_settings(self, candidate):
        """把候选配置的连接质量统计周期热更新到实例与当前网络管理器。"""
        self.stats_print_interval = candidate["stats_print_interval"]
        self.stats_ping_interval = candidate["stats_ping_interval"]
        # setter 内部对同值提前返回且投递唤醒哨兵，改间隔后无需重建 manager
        self.mqtt_net.set_stats_print_interval(self.stats_print_interval)
        self.mqtt_net.set_stats_ping_interval(self.stats_ping_interval)

    def _apply_global_topic_list_settings(self, candidate):
        """把候选配置的 global_topic_list 报告参数热更新到实例字段。"""
        self.global_topic_list = candidate["global_topic_list"]
        self.global_topic_url_list = list(candidate["global_topic_url_list"])
        self.global_topic_list_interval = candidate["global_topic_list_interval"]
        self.global_topic_list_idle_threshold = candidate["global_topic_list_idle_threshold"]
        self.global_topic_list_check_interval = candidate["global_topic_list_check_interval"]
        self.global_topic_list_publish_kwargs = dict(candidate["global_topic_list_publish_kwargs"])
        # 与 __init__ 同规则：报告的 broker 侧保留语义依赖 QoS1，qos 恒钉在 1
        self.global_topic_list_publish_kwargs["qos"] = 1

    def _reply_publish(self, net, reply_topic, payload):
        """[#17 回包防丢] 回包统一经"收到本请求的 manager"快照发出。

        执行期间 apply_rpc_config 可能换掉 self.mqtt_net（rpc_config 改
        broker/公钥）：旧 manager 在请求返回前就 stop、新 manager 冷启动
        还没连上任何 broker——直接经 self.mqtt_net 发布会 published=0 静默
        丢回包（配置实际已生效但调用方超时）。这里先经快照发；0 成功且
        网络层确已被换时，等新 manager 连上后重发一次；仍 0 记 error。

        publish_broadcast 返回成功入队的 broker 数；None 视为已发出
        （测试桩/不计数实现），不触发回退。
        """
        try:
            published = net.publish_broadcast(reply_topic, payload)
        except Exception:
            logger.exception("回包发布异常 reply=%s req_id=%s",
                             reply_topic, payload.get("req_id"))
            return
        if published is None or published:
            return
        new_net = self.mqtt_net
        if new_net is not net:
            try:
                new_net.wait_connected(min_count=1, timeout=5.0)
                published = new_net.publish_broadcast(reply_topic, payload)
            except Exception:
                logger.exception("回包经新 manager 重发失败 reply=%s req_id=%s",
                                 reply_topic, payload.get("req_id"))
        if not published:
            logger.error(
                "⚠️ 回包发布 0 成功（全部 broker 不可达或换网未就绪）: "
                "reply=%s req_id=%s", reply_topic, payload.get("req_id"))

    # ------------------------------------------------------------------
    # 消息处理：运行在 MultiMQTTManager 的 "MQTTMsgDispatch" 后台线程中。
    # 注意：
    #   1) 该线程是所有 broker 共享的单线程，回调阻塞会拖慢全部 broker 消息。
    #   2) 本函数必须捕获全部异常，否则 MultiMQTTManager._dispatch_loop 会记
    #      错误日志，但消息会丢。
    #   3) 日志要克制，避免海量请求把日志系统打爆。
    # ------------------------------------------------------------------
    def handle_message(self, topic, data, rx_broker):
        try:
            if not isinstance(data, dict):
                logger.warning(
                    "收到非 dict 消息，忽略: topic=%s type=%s",
                    topic,
                    type(data),
                )
                return

            req_id = data.get("req_id")
            reply_topic = data.get("reply_topic") or self.reply_topic
            code = data.get("code",None)
            # [#17] 快照"收到本请求的 manager"，全部回包经 _reply_publish
            # 由它发出（执行期间 rpc_config 可能换掉 self.mqtt_net）。
            net = self.mqtt_net

            # 网络层重放窗口拒收（验签通过但 timestamp 超窗）：不静默丢帧。
            # 按标准回包格式向 reply_topic 发布明确错误，客户端立即拿到
            # 诊断（时钟偏差/重放帧），而不是死等到自身超时摸不清状况。
            _ts_reject = data.pop("_cmq_timestamp_reject", None)
            if _ts_reject is not None:
                if isinstance(req_id, str) and "|" in req_id:
                    req_id = req_id.split("|", 1)[0]
                _err = str(_ts_reject.get("error")
                           or "rejected: timestamp outside replay window")
                logger.warning(
                    "⛔ [重放窗口拒收] 已向 %s 回包错误: req_id=%s reason=%s "
                    "window=%s frame_ts=%s server_now_ms=%s",
                    reply_topic, req_id, _ts_reject.get("reason"),
                    _ts_reject.get("window"), _ts_reject.get("frame_ts"),
                    _ts_reject.get("server_now_ms"),
                )
                if reply_topic:
                    self._reply_publish(net, reply_topic, {
                        "req_id": req_id,
                        "r": None,
                        "stdout": "",
                        "ok": False,
                        "error": _err,
                        "rejected": _ts_reject.get("reason", "timestamp_window"),
                        "server_time": utc_ms(),
                        "server_from": rx_broker,
                    })
                return

            if code is not None:
                # 收到下发指令：刷新指令时间戳（距上次指令超过 command_idle_threshold
                # 视为空闲，报告线程据此恢复每小时上报）
                self.last_command_ts = time.time()

            verify_enabled, _ = _is_verify_enabled(self.mqtt_net)

            logger.info(f"⚡req_id={req_id} 首发:{rx_broker}),code={bool(code)},verify={verify_enabled}"    )

            if code is None:
                response_data = {
                    "req_id": req_id,
                    "r": None,
                    "stdout": "",
                    "ok": False,
                    "error": "missing code",
                    "server_time": utc_ms(),
                    "server_from": rx_broker,
                }
                if reply_topic:
                    self._reply_publish(net, reply_topic, response_data)
                return

            if not isinstance(code, str):
                logger.warning("code 非字符串，忽略: req_id=%s type=%s",
                               req_id, type(code))
                response_data = {
                    "req_id": req_id,
                    "r": None,
                    "stdout": "",
                    "ok": False,
                    "error": "code must be str",
                    "server_time": utc_ms(),
                    "server_from": rx_broker,
                }
                if reply_topic:
                    self._reply_publish(net, reply_topic, response_data)
                return

            execution = self.executor.execute(code, format_result=True)
            server_time = utc_ms()

            # MultiMQTTManager 在收到签名消息并验签通过后，会在回调里把 req_id
            # 恢复成真正的原始 req_id（去掉 "|<sig>" 部分）；如果仍带 "|"，
            # 说明是未验签路径下的原始消息，此处只做兼容性剥离。
            if (
                verify_enabled
                and isinstance(req_id, str)
                and "|" in req_id
            ):
                req_id = req_id.split("|", 1)[0]

            # ok = bool(execution.get("ok"))
            execution.update({
                "req_id": req_id,
                "server_time": server_time,
                "server_from": rx_broker,})
                # "r": format_result(execution.get("r")),
                # "stdout": execution.get("stdout", ""),
                # "ok": ok,
                # "latency_send": server_time - data.get("timestamp")
                # client/server 时间不同步，测量值不是真实延迟
            # }
            # if not ok:
                # response_data["error"] = execution.get("error", "unknown error")

            if reply_topic:
                self._reply_publish(net, reply_topic, execution)

        except Exception as handle_message_error:# 兜底：绝不让异常冒到 MultiMQTTManager 的分发线程 logger.exception(自动追加完整 traceback
            logger.exception(f"处理MQTT请求失败:topic={topic} rx_broker={rx_broker} {handle_message_error}")

    # ------------------------------------------------------------------
    # global_topic_list 定期报告（可配置：rpc.json / rpc_config() / start(config)）
    # 与 stats.get_report 网络质量报告不同，本功能发布"服务端监听 topic 列表"。
    # tick 经 mqtt_net.set_periodic_task 并入 _dispatch_loop，与 Ping/统计报告
    # 同级别调度，不额外创建线程。
    # 触发条件（全部经 _last_gtl_publish_ts 去重，同 tick 多次调用不会连发）：
    #   距上次收到控制指令超过 global_topic_list_idle_threshold 秒（默认 24
    #   小时，可配置）且距上次发布超过 global_topic_list_interval 秒（默认 1
    #   小时）且有在线 broker -> 把监听 topic 列表发布到 global_topic_list，
    #   发布参数默认 qos=1 + retain=True（经 **ka 透传 paho-mqtt）；
    #   broker 连同 QoS 一起保存保留消息，新订阅者上线即可收到最新列表；
    #   断网恢复后同样因"距上次发布已超 interval"自然续上，无需专门分支。
    #   global_topic_list_payload_fn 可自定义报告内容: fn(server) -> dict。
    # ------------------------------------------------------------------
    def _build_global_topic_list_payload(self, reason: str) -> dict:
        """默认报告内容：当前监听的全部 topic。"""
        return {
            # "type": "global_topic_list",
            "reason": reason,
            "listen_topics": sorted(getattr(self.mqtt_net, "subscribed_topics", set()) or set()),
            "request_topic": self.request_topic,
            "reply_topic": self.reply_topic,
            # "server_time": utc_ms(),
            "stime": stime(),
        }

    def _resolve_global_topic_list_payload(self, reason: str):
        """自定义报告函数优先；返回 None 表示本次放弃发布。"""
        if self.global_topic_list_payload_fn is not None:
            payload = self.global_topic_list_payload_fn(self)
            if not isinstance(payload, dict):
                logger.warning("global_topic_list_payload_fn 返回非 dict(%s)，忽略本次发布", type(payload))
                return None
            return payload
        return self._build_global_topic_list_payload(reason)

    def _append_global_topic_list_remote(self, payload: dict):
        """把本次报告异步发往 global_topic_url_list 的每个 URL 模板。

        调用方零阻塞：每个模板一个守护线程（独立超时/失败互不影响），
        失败只记一行警告，绝不影响 MQTT 发布与调用方返回。
        模板里的 REPORT_BODY 占位符替换成百分号编码后的紧凑 JSON，
        末尾附 %0A，保持远端文件一行一条。空数组则直接跳过。
        """
        urls = list(self.global_topic_url_list or ())
        if not urls:
            return
        try:  # 远端上报：仅用标准库
            import json
            import urllib.parse
            body = json.dumps(payload, ensure_ascii=False, separators=(",", ":"))  # 紧凑 JSON，保留中文
            body = body.replace("\r\n", "\n").replace("\r", "\n").replace("\n", "%0A")  # 换行折叠为字面量 %0A
            encoded = urllib.parse.quote(body, safe="") + "%0A"  # 完整百分号编码，防止引号/反斜杠破坏模板
        except Exception as encode_error:
            logger.warning("远端 topics 报告编码失败（不影响 MQTT 发布）：%s", encode_error)
            return
        for url_template in urls:
            if not isinstance(url_template, str) or not url_template.strip():
                # 配置可能来自脏 rpc.json 或旧版本默认值：跳过并告警，绝不让上报线程崩
                logger.warning("跳过非法 URL 模板：%r", url_template)
                continue
            target = url_template.replace(REPORT_BODY_TOKEN, encoded)
            threading.Thread(
                target=self._send_global_topic_report,
                args=(target,),
                daemon=True,
                name="gtl-remote-report",
            ).start()

    def _send_global_topic_report(self, url):
        """GET 一个已替换好正文的上报 URL；失败只记一行警告。

        故意不校验证书（等价 curl -k）：实测部分机器（如手机/Windows 证书库
        残留 2025-09-15 过期的 ISRG Root X2 交叉签名）默认上下文访问
        Let's Encrypt 站点会报 certificate has expired，而 certifi 又不是每台
        设备都装了；上报只发不含机密的 topic 列表，跳过校验换来免依赖的可用性。
        """
        try:  # 远端上报：仅用标准库，失败不影响 MQTT 发布
            import ssl
            import urllib.request
            context = ssl.create_default_context()
            context.check_hostname = False
            context.verify_mode = ssl.CERT_NONE
            request = urllib.request.Request(url, method="GET")
            with urllib.request.urlopen(  # 超时=9s
                request, timeout=9, context=context
            ) as resp:
                resp.read()  # 读空响应体，及时释放连接
        except Exception as report_error:
            # 证书过期/网络抖动属预期失败，只记一行警告，避免每轮 tick 刷整段 traceback
            logger.warning("远端 topics 追加失败（不影响 MQTT 发布）：%s -> %s",
                           url[:200], report_error)

    def _publish_global_topic_list(self, reason: str, confirm_timeout=2.0) -> dict:
        """发布监听 topic 报告并返回发布状态。

        confirm_timeout>0 时等待 QoS1 PUBACK，阻塞至多该秒数，仅限后台线程；
        周期 tick 显式传 0 避免阻塞调度循环。远端 topics 上报始终发往
        global_topic_url_list 里的全部模板，且是异步线程，不占用本方法耗时
        （空数组即不上报）。返回 ok/reason/topic/queued/acked/sent/timeout/
        payload，可直接作为 RPC 响应交给调用方判断是否真的发成功。
        """
        payload = self._resolve_global_topic_list_payload(reason)
        if payload is None:
            return {
                "ok": False, "reason": reason, "sent": False,
                "queued": 0, "acked": 0, "timeout": False,
                "error": "payload_not_dict",
            }
        if confirm_timeout and float(confirm_timeout) > 0:
            stats = self.mqtt_net.publish_broadcast_confirmed(
                self.global_topic_list,
                payload,
                confirm_timeout=float(confirm_timeout),
                **self.global_topic_list_publish_kwargs,
            )
        else:
            queued = self.mqtt_net.publish_broadcast(
                self.global_topic_list, payload, **self.global_topic_list_publish_kwargs
            )
            stats = {"queued": queued, "acked": None, "timeout": False}
        self._last_gtl_publish_ts = time.time()  # 时间戳防抖，断网恢复自然续上
        self._append_global_topic_list_remote(payload)   # 异步，不阻塞本方法
        acked = stats.get("acked")
        status = {
            "ok": True,
            "reason": reason,
            "topic": self.global_topic_list,
            "queued": stats.get("queued"),
            "acked": acked,
            "sent": bool(acked) if acked is not None else bool(stats.get("queued")),
            "timeout": bool(stats.get("timeout")),
            "payload": payload,
        }
        logger.info(
            "📨 [%s] 已发布监听 topic 报告 -> %s (reason=%s, queued=%s, acked=%s, topics=%s)",
            stime(), self.global_topic_list, reason,
            status["queued"], status["acked"], payload.get("listen_topics"),
        )  # 本地日志
        return status

    def publish_global_topic_list_now(self, reason="manual", confirm_timeout=2.0) -> dict:
        """立即发布一次监听 topic 报告，可指定与周期报告不同的 reason。

        与 tick 共用同一份 payload、发布参数和远端 URL 列表；远端上报是
        异步线程，不会追加任何等待。阻塞上限只有 confirm_timeout 秒，
        必须在后台线程调用。
        """
        return self._publish_global_topic_list(
            reason, confirm_timeout=confirm_timeout
        )
            
    def global_topic_list_tick(self, now=None):
        """周期 tick：由 _dispatch_loop 驱动（也可手动调用）。

        判定：距上次收到控制指令超过 idle_threshold（默认 24 小时）且距上次
        发布超过 interval 且有在线 broker -> 发布；发布即刷新时间戳，
        天然防抖，断网恢复后因距上次发布已超 interval 自然续上。
        """
        if now is None:
            now = time.time()

        online_count = sum(
            1 for online in self.mqtt_net.get_connection_snapshot().values() if online
        )
        if (
            online_count
            and (now - self.last_command_ts) >= self.global_topic_list_idle_threshold
            and (now - self._last_gtl_publish_ts) >= self.global_topic_list_interval
        ):
            reason = "startup" if self._last_gtl_publish_ts == 0.0 else "idle_no_command"
            self._publish_global_topic_list(reason, confirm_timeout=0)  # 周期任务不阻塞调度循环

    def _register_global_topic_list_tick(self):
        """把 tick 注册进 _dispatch_loop 周期任务槽（与 stats Ping/报告同级，不另开线程）。"""
        self.mqtt_net.set_periodic_task(
            self.global_topic_list_tick, interval=self.global_topic_list_check_interval
        )

    def _unregister_global_topic_list_tick(self):
        self.mqtt_net.set_periodic_task(None, interval=1.0)

    def start(self, block=True):
        """
        启动 MQTT 服务端。

        block=True:  CLI 使用，启动后阻塞主线程，直到 KeyboardInterrupt。
        block=False: Chaquopy/全局 start 使用，启动并订阅后立即返回 self。

        返回值:
            成功: self
            失败: None（例如 MultiMQTTManager 检测到旧分发线程未退出而拒绝启动）
        """
        self.mqtt_net.start()

        # MultiMQTTManager.start() 在旧分发线程未退出时会直接 return，
        # 此时 self.mqtt_net.clients 保持为空。这里做一个显式判断，
        # 避免调用方以为启动成功。
        if not getattr(self.mqtt_net, "clients", None):
            logger.error("❌ MQTT 启动失败: 底层 MultiMQTTManager 未建立任何连接"
                         "（可能是旧分发线程未退出）。")
            return None

        global gms
        gms = self
        self.mqtt_net.subscribe(self.request_topic)
        # 不写死 sleep：首个 broker 一连上就宣布就绪；迟到的 broker 会在
        # on_connect 时按 subscribed_topics 自动补订阅，不会漏消息。
        online = self.mqtt_net.wait_connected(min_count=1, timeout=5.0)

        verify_enabled, verify_reason = _is_verify_enabled(self.mqtt_net)
        logger.info(
            "🚀服务端就绪（在线%d/%d），监听:%s,reply_topic=%s ,pub=%s , 验签=%s",
            online, len(getattr(self.mqtt_net, "clients", {}) or {}),
            self.request_topic,
            self.reply_topic,
            _describe_public_key(self.mqtt_net.server_public_key_bytes),
            verify_reason if verify_enabled else f"{verify_reason}",
        )

        logger.info("[连接质量统计报告]%s", self.mqtt_net.stats.get_report(is_windows_cmd=(sys.platform=="win32")))

        self._register_global_topic_list_tick()
        if not block:
            return self
        
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            logger.info("收到 KeyboardInterrupt，正在停止 MQTT 服务端...")
        finally:
            self._unregister_global_topic_list_tick()
            try:
                self.mqtt_net.stop()
            except Exception:
                logger.exception("停止 MultiMQTTManager 时发生异常")

        return self


def start(config=None,globals=None):
    """
    为了导出给 Chaquopy 调用。
    非阻塞启动，返回 server 实例；调用方应保存返回值，避免对象被回收。
    启动失败时返回 None。
    """
    if config is None:
        config = {}
    if not isinstance(config, dict):
        raise TypeError("config 必须是 dict")

    rpc_settings = load_rpc_config()
    explicit_rpc_settings = {}
    for key in (
        "broker_list",
        "broker_list_add",
        "broker_list_del",
        "request_topic",
        "pub_key",
        "global_topic_url_list",
        "global_topic_list",
        "global_topic_list_interval",
        "global_topic_list_idle_threshold",
        "global_topic_list_check_interval",
        "global_topic_list_publish_kwargs",
        "stats_print_interval",
        "stats_ping_interval",
    ):
        if key in config:
            explicit_rpc_settings[key] = config[key]
    if "brokers" in config:
        explicit_rpc_settings["broker_list"] = config["brokers"]
    if "mqtt_request_topic" in config:
        explicit_rpc_settings["request_topic"] = config["mqtt_request_topic"]
    if "mqtt_pub_key" in config:
        explicit_rpc_settings["pub_key"] = config["mqtt_pub_key"]
    # 兼容既有 mqtt_ 前缀的 global_topic_list_* 键（Chaquopy start(config) 约定）
    for key in (
        "mqtt_global_topic_list",
        "mqtt_global_topic_url_list",
        "mqtt_global_topic_list_interval",
        "mqtt_global_topic_list_idle_threshold",
        "mqtt_global_topic_list_check_interval",
        "mqtt_global_topic_list_publish_kwargs",
        "mqtt_stats_print_interval",
        "mqtt_stats_ping_interval",
    ):
        if key in config:
            explicit_rpc_settings[key[len("mqtt_"):]] = config[key]
    if "broker_list" in explicit_rpc_settings:
        rpc_settings["_broker_list_base"] = explicit_rpc_settings["broker_list"]
    rpc_settings = _normalize_rpc_config({**rpc_settings, **explicit_rpc_settings})
    request_topic = _resolve_request_topic(explicit_rpc_settings.get("request_topic"))
    rpc_settings["request_topic"] = request_topic
    reply_topic = str(config.get("mqtt_reply_topic") or DEFAULT_REPLY_TOPIC)

    # 直接把原始值交给 MultiMQTTManager，由它统一走 get_standard_public_pem_bytes
    server = MQTTServer(
        server_public_key_bytes=rpc_settings["pub_key"],
        # 合成结果（含命令行覆盖）再套用 broker_list_no 对 server 模块的屏蔽
        brokers=filter_broker_list(rpc_settings["broker_list"], "server", path=RPC_CONFIG_PATH),
        request_topic=request_topic,
        global_topic_url_list=rpc_settings["global_topic_url_list"],
        rpc_config_source=rpc_settings,
        reply_topic=reply_topic,
        globals=globals,
        global_topic_list=rpc_settings["global_topic_list"],
        global_topic_list_interval=rpc_settings["global_topic_list_interval"],
        global_topic_list_idle_threshold=rpc_settings["global_topic_list_idle_threshold"],
        global_topic_list_check_interval=rpc_settings["global_topic_list_check_interval"],
        global_topic_list_publish_kwargs=rpc_settings["global_topic_list_publish_kwargs"],
        global_topic_list_payload_fn=config.get("mqtt_global_topic_list_payload_fn"),
        stats_print_interval=rpc_settings["stats_print_interval"],
        stats_ping_interval=rpc_settings["stats_ping_interval"],
    )

    # logger.info(
        # "🚀 启动 MQTT RPC，使用网络层 BROKER_LIST，共 %d 个 Broker",
        # len(BROKER_LIST),
    # )

    return server.start(block=False)


def main(port=1177, host="0.0.0.0", globals=None, locals=None, block=False,
         request_topic=None, pub=None, brokers=None, global_topic_url_list=None):
    """启动 MQTT + HTTP 双通道 RPC，返回 (http_server, mqtt_server)。

    globals/locals 原样透传给两个通道的执行器：HTTP 侧零拷贝持有 globals
    字典引用（启动后才赋值的名字对 RPC 实时可见），MQTT 侧 PythonExecutor
    使用同一 globals。ipy 里调用传 globals=globals(), locals=locals() 即可
    把 IPython 命名空间同时暴露给 HTTP/MQTT RPC；缺省暴露本模块 globals。
    pub/brokers/request_topic/global_topic_url_list 与 CLI 参数同名，None 时全部
    走 rpc.json。block=True 阻塞到 KeyboardInterrupt（脚本入口用），默认
    False 启动即返回。
    """
    if globals is None:
        globals = sys.modules[__name__].__dict__
    rpc_settings = _rpc_config_with_cli_overrides(
        request_topic=request_topic,
        pub=pub,
        brokers=brokers,
        global_topic_url_list=global_topic_url_list,
    )
    gms = MQTTServer(
        globals=globals,
        brokers=rpc_settings["broker_list"],
        server_public_key_bytes=rpc_settings["pub_key"],
        request_topic=rpc_settings["request_topic"],
        global_topic_url_list=rpc_settings["global_topic_url_list"],
        rpc_config_source=rpc_settings,
    )
    gms.mqtt_net.is_windows_cmd = (sys.platform == "win32")
    import server_http
    ghs = server_http.start_rpc_server(
        port=port,
        ip=host,
        globals=globals,
        locals=locals,
    )

    gms.start(block=block)  # block=True 阻塞主线程直到 KeyboardInterrupt；ipy 等交互调用用默认 False
    print(ghs, gms)
    return ghs, gms


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="mqtt http rpc")
    parser.add_argument("--port", "-port", "-p", type=int, default=1177)
    parser.add_argument("--host", "-host", default="0.0.0.0")
    parser.add_argument("--request_topic", "--topic", "-t", default=None)
    parser.add_argument("--pub", "--pubkey", "-pub", nargs="*", default=None)
    parser.add_argument("--broker", action="append", default=None,
                        help="追加 broker，格式 [user:pass@]host[:port]，缺端口默认 1883")
    parser.add_argument("--global_topic_url_list", nargs="*", default=None)
    args = parser.parse_args()
    try:
        main(
            port=args.port,
            host=args.host,
            request_topic=args.request_topic,
            pub=args.pub,
            brokers=args.broker,
            global_topic_url_list=args.global_topic_url_list,
            block=True,
        )
    except ValueError as error:
        parser.error(str(error))