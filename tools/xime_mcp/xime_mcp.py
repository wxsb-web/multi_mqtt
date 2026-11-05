#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""xime_mcp —— 把远端手机的 XimeAccessibilityService 挂成标准 MCP 工具。

本进程是 **客户端**，运行在本地电脑；远端手机只需要跑着 ``server_mqtt``
和内置的 ``feature_touch`` 模块，手机侧不需要安装任何 MCP 代码。每次工具
调用都把一段 Python 代码字符串经 MQTT RPC **动态下发**到手机进程里执行，
再回收 ``feature_touch`` 返回的 JSON。

连接信息（topic / 私钥 / 超时等）只从同一份 ``rpc.json`` 的 ``xime_mcp``
段读取，本文件不包含任何设备地址、topic 或密钥。

配置示例（rpc.json，与 broker 配置同文件）::

    "xime_mcp": {
        "request_topic": "<手机 server_mqtt 监听的 topic>",
        "private_key": null,              # 可选；支持 int / "k=<表达式>" / PEM / 文件路径
        "timeout": 20,
        "min_broker_count": 1,
        "allow_no_server_pubkey_response": true
    }

启动方式（默认就是与 pty 相同的同口模式，MCP 与 RPC 复用 5555 端口）::

    python tools/xime_mcp/xime_mcp.py

需要走 stdin/stdout 的本地 MCP 客户端时显式指定::

    python tools/xime_mcp/xime_mcp.py --transport stdio

同口模式下可以像 pty 一样直接 RPC 查看本进程内部任意状态::

    curl "http://127.0.0.1:5555/r=get_log()"
    curl "http://127.0.0.1:5555/r=list(globals().keys())"
    curl "http://127.0.0.1:5555/r=get_config()"
    curl "http://127.0.0.1:5555/r=xime_status()"

元素视图（浏览器打开根路径自动跳转；不是截屏，只用 SVG 画节点形状，
点击画面会把坐标换算成设备坐标并回传 tap）::

    http://127.0.0.1:5555/ui_html(p)

自检（不启动 MCP，直接调一次手机 status）::

    python tools/xime_mcp/xime_mcp.py --selftest
"""

import argparse
import ast
import asyncio
import functools
import json
import logging
import os
import sys
import threading
import time
from collections import deque

_HERE = os.path.dirname(os.path.abspath(__file__))
_ROOT = os.path.dirname(os.path.dirname(_HERE))
_CLIENT_DIR = os.path.join(_ROOT, "client")
for _path in (_ROOT, _CLIENT_DIR, _HERE):
    if _path not in sys.path:
        sys.path.insert(0, _path)

import client_mqtt  # noqa: E402  复用 client/rpc 的 MQTT 客户端与配置读取
import xime_ui  # noqa: E402  元素视图的页面 / WebSocket / MQTT 数据面
from multi_mqtt import get_standard_pem_bytes  # noqa: E402

logger = logging.getLogger("xime_mcp")

CONFIG_PATH = os.environ.get("XIME_MCP_CONFIG") or os.path.join(_ROOT, "rpc.json")

# 进程内事件环形缓冲：HTTP 同口 RPC 可查（/r=get_log()），stdio 模式不写 stdout。
_EVENT_LOG = deque(maxlen=300)


def _event(kind, **fields):
    """记录一条进程内事件（只进内存，不打印）。"""
    item = {"ts": time.strftime("%Y-%m-%d %H:%M:%S"), "kind": kind}
    item.update(fields)
    _EVENT_LOG.append(item)
    return item


def get_log(n=200):
    """返回最近 n 条进程内事件（HTTP RPC：``/r=get_log()`` / ``/r=get_log(50)``）。"""
    try:
        count = max(1, int(n))
    except (TypeError, ValueError):
        count = 200
    rows = list(_EVENT_LOG)[-count:]
    return "\n".join(
        "%s [%s] %s" % (
            row["ts"], row["kind"],
            " ".join("%s=%r" % (k, v) for k, v in row.items() if k not in ("ts", "kind")),
        ) for row in rows
    )


def clear_log():
    """清空进程内事件缓冲（HTTP RPC：``/r=clear_log()``）。"""
    _EVENT_LOG.clear()
    return "cleared"

# 设备端代码模板：每次调用现拼字符串下发。末尾显式 ``r = ...``，保证 RPC
# 执行器回退到 locals["r"] 时拿到的就是本次结果（不是上一次残留值）。
_DEVICE_CODE = """import json
import feature_touch
try:
    r = feature_touch.{fn}({args})
except Exception as exc:
    r = json.dumps({{"ok": False, "op": {op}, "error": str(exc)}}, ensure_ascii=False)
"""

_CONFIG_DEFAULTS = {
    "timeout": 20.0,
    "min_broker_count": 1,
    "allow_no_server_pubkey_response": True,
    "reply_topic": None,
    "private_key": None,
}

# 命令行覆盖（与 client_mqtt 的 alias 同名参数）：非 None 的键优先于 rpc.json。
_CLI_OVERRIDES = {}


class XimeConfigError(RuntimeError):
    """rpc.json 的 xime_mcp 配置段缺失或非法。"""


def _as_positive_float(value, name, default):
    if value is None:
        return float(default)
    if isinstance(value, bool):
        raise XimeConfigError("xime_mcp.%s 必须是正数" % name)
    try:
        number = float(value)
    except (TypeError, ValueError):
        raise XimeConfigError("xime_mcp.%s 必须是正数" % name)
    if number <= 0:
        raise XimeConfigError("xime_mcp.%s 必须大于 0" % name)
    return number


def _as_non_negative_int(value, name, default):
    if value is None:
        return int(default)
    if isinstance(value, bool):
        raise XimeConfigError("xime_mcp.%s 必须是非负整数" % name)
    try:
        number = int(value)
    except (TypeError, ValueError):
        raise XimeConfigError("xime_mcp.%s 必须是非负整数" % name)
    if number < 0:
        raise XimeConfigError("xime_mcp.%s 不能小于 0" % name)
    return number


def _normalize_key(value):
    """私钥归一化：支持 int / "k=..." / PEM / 文件路径；空值返回 None。"""
    if value is None:
        return None
    if isinstance(value, (bytes, bytearray)):
        return bytes(value)
    text = str(value).strip()
    if not text:
        return None
    if text.startswith("k="):
        text = text[2:].strip()
    return get_standard_pem_bytes(text)


def get_config(path=None):
    """返回 rpc.json 里 xime_mcp 段的归一化配置；缺失或非法抛 XimeConfigError。"""

    path = path or CONFIG_PATH
    try:
        document = client_mqtt.load_client_rpc_config(path)
    except Exception as exc:  # 配置读取失败也要给出可操作的信息
        raise XimeConfigError("读取 %s 失败: %s" % (path, exc))
    section = document.get("xime_mcp") if isinstance(document, dict) else None
    if not isinstance(section, dict):
        raise XimeConfigError(
            "rpc.json 缺少 xime_mcp 配置段（在 %s 里加："
            '"xime_mcp": {"request_topic": "<手机 topic>", "timeout": 20})' % path
        )
    topic = str(section.get("request_topic") or section.get("topic") or "").strip()
    if not topic:
        raise XimeConfigError("xime_mcp.request_topic 不能为空（填手机 server_mqtt 监听的 topic）")
    reply_topic = str(section.get("reply_topic") or "").strip() or None
    config = {
        "request_topic": topic,
        "reply_topic": reply_topic,
        "timeout": _as_positive_float(section.get("timeout"), "timeout", _CONFIG_DEFAULTS["timeout"]),
        "min_broker_count": _as_non_negative_int(
            section.get("min_broker_count"), "min_broker_count", _CONFIG_DEFAULTS["min_broker_count"]),
        "allow_no_server_pubkey_response": bool(
            section.get("allow_no_server_pubkey_response",
                        _CONFIG_DEFAULTS["allow_no_server_pubkey_response"])),
        "private_key": _normalize_key(section.get("private_key", section.get("k"))),
    }
    for key, value in _CLI_OVERRIDES.items():
        if value is not None:
            config[key] = value
    return config


def _parse_device_payload(value):
    """把设备端 ``r`` 解析成 dict/list。

    兼容两种回包形态：新版 client 直接给对象；部分机器（如 pty100 上的
    client_mqtt）把 ``r`` 以 Python repr 字符串返回，因此再退一步
    ``ast.literal_eval``。
    """
    if isinstance(value, (dict, list)):
        return value
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return {"ok": False, "error": "设备返回空字符串"}
        try:
            return json.loads(text)
        except (TypeError, ValueError):
            pass
        try:
            return ast.literal_eval(text)
        except (SyntaxError, ValueError):
            pass
        return {"ok": False, "error": "无法解析设备返回值", "raw": text}
    return {"ok": False, "error": "设备返回类型不支持: %s" % type(value).__name__}


def device_call(fn, args=(), op=None, timeout=None):
    """动态下发一次 ``feature_touch.<fn>(...)`` 调用并解析结果。

    参数说明：
    - ``fn``：feature_touch 里的函数名，白名单由 MCP 工具层保证；
    - ``args``：已序列化好的参数片段列表（字符串），原样拼进代码；
    - ``op``：返回结果里标注的操作名；
    - ``timeout``：本次 RPC 超时；默认取 rpc.json 的 xime_mcp.timeout。
    """
    config = get_config()
    code = _DEVICE_CODE.format(
        fn=fn,
        args=", ".join(args),
        op=json.dumps(op or fn, ensure_ascii=False),
    )
    return _send_device_code(code, op or fn, timeout, config)


def _send_device_code(code, op, timeout, config):
    """把一段设备端 Python 代码经 MQTT RPC 下发，并归一化回包。"""
    rpc_timeout = float(timeout or config["timeout"])
    kwargs = {
        "request_topic": config["request_topic"],
        "timeout": rpc_timeout,
        "min_broker_count": config["min_broker_count"],
        "allow_no_server_pubkey_response": config["allow_no_server_pubkey_response"],
        # stdio MCP 必须保持 stdout 干净：关闭连接质量周期报告（内部走 print）
        "stats_print_interval": 0,
    }
    if config["private_key"]:
        kwargs["client_private_key_bytes"] = config["private_key"]
    if config["reply_topic"]:
        kwargs["reply_topic"] = config["reply_topic"]

    started = time.monotonic()
    raw = client_mqtt.rpc(code, **kwargs)
    elapsed_ms = int((time.monotonic() - started) * 1000)
    if not raw:
        return {
            "ok": False,
            "op": op,
            "error": "RPC 无回包（手机可能离线 / topic 不对 / broker 未就绪）",
            "request_topic": config["request_topic"],
            "timeout": rpc_timeout,
            "elapsed_ms": elapsed_ms,
        }
    if not isinstance(raw, dict):
        return {"ok": False, "op": op, "error": "RPC 回包不是 dict", "raw": raw}

    result = _parse_device_payload(raw.get("r"))
    if isinstance(result, dict):
        result.setdefault("op", op)
        result["elapsed_ms"] = elapsed_ms
        if raw.get("latency_ms") is not None:
            result["latency_ms"] = raw["latency_ms"]
    if raw.get("ok") is False and isinstance(result, dict) and not result.get("error"):
        result["error"] = raw.get("error") or "RPC 执行失败"
        result["ok"] = False
    return result


# 元素视图的数据面复用同一套控制面 RPC 与 rpc.json 配置，避免循环导入。
xime_ui.configure(send_code=_send_device_code, get_config_fn=get_config,
                  log_event=_event)


def _q(value):
    """把参数序列化成设备端可安全拼接的 Python 字面量（True/False/None 走 repr）。"""
    if isinstance(value, (str, bool, int, float)) or value is None:
        return repr(value)
    return json.dumps(value, ensure_ascii=False)


def _call(fn, *args, timeout=None, op=None):
    return device_call(fn, args=[_q(arg) for arg in args], op=op, timeout=timeout)


def xime_status(deep=False):
    """返回服务连接状态、系统手势上限和最近一次失败原因。

    deep=True 时改走设备端 diagnose：额外检查服务类是否存在、无障碍开关、
    已启用服务列表与提示，用于连接异常时排查。
    """
    if deep:
        return device_call("diagnose", op="status")
    return device_call("status", op="status")


def xime_dump_ui(max_nodes=400, include_empty=False):
    """读取当前界面节点树（可跨应用）。节点字段：t 文本 / d 描述 / id 资源 ID /
    c 类名 / b [left,top,right,bottom] / k 可点击 / e 可编辑 / f 聚焦 / s 可滚动。"""
    return _call("dump_ui", int(max_nodes), bool(include_empty), op="dump_ui")


def xime_find_text(text, max_nodes=400, exact=False, include_empty=False):
    """按文本或内容描述查找节点，返回命中列表（含点击用中心点 x/y）。"""
    return _call("find_text", str(text), int(max_nodes), bool(exact),
                 bool(include_empty), op="find_text")


def xime_tap_text(text, index=0, max_nodes=400, exact=False, duration_ms=80):
    """找到文本命中的节点并点击其中心点；index 用于同名节点排序。"""
    return _call("tap_text", str(text), int(index), int(max_nodes),
                 bool(exact), int(duration_ms), op="tap_text")


def xime_wait_text(text, timeout=10.0, interval=0.3, max_nodes=400, exact=False):
    """轮询等待文本出现；命中立即返回节点，超时返回失败。"""
    wait = max(0.1, float(timeout))
    return _call("wait_text", str(text), float(timeout), float(interval),
                 int(max_nodes), bool(exact), op="wait_text",
                 timeout=wait + 5.0)


def xime_tap(x, y, duration_ms=80):
    """点击/短按指定坐标，duration_ms 建议 50~150。"""
    return _call("tap", float(x), float(y), int(duration_ms), op="tap")


def xime_long_press(x, y, duration_ms=600):
    """长按指定坐标，duration_ms 建议 500~1500。"""
    return _call("long_press", float(x), float(y), int(duration_ms), op="long_press")


def xime_swipe(x1, y1, x2, y2, duration_ms=300):
    """直线滑动；duration_ms 越小越快。"""
    return _call("swipe", float(x1), float(y1), float(x2), float(y2),
                 int(duration_ms), op="swipe")


def xime_drag(x1, y1, x2, y2, hold_ms=600, duration_ms=300):
    """拖拽：起点长按 hold_ms 后移动，全程不松手。"""
    return _call("drag", float(x1), float(y1), float(x2), float(y2),
                 int(hold_ms), int(duration_ms), op="drag")


def xime_pinch(center_x, center_y, start_radius, end_radius, duration_ms=400):
    """双指缩放：start_radius < end_radius 放大，反之缩小。"""
    return _call("pinch", float(center_x), float(center_y), float(start_radius),
                 float(end_radius), int(duration_ms), op="pinch")


def xime_gesture(strokes):
    """通用多指/多段手势。strokes 是 JSON 字符串：
    {"strokes":[{"points":[[x,y],...],"start":0,"duration":300,
                 "willContinue":false,"continuePrevious":false}]}"""
    if isinstance(strokes, (dict, list)):
        strokes = json.dumps(strokes, ensure_ascii=False)
    return _call("gesture", str(strokes), op="gesture")


def xime_global_action(action):
    """全局动作：1=返回 2=Home 3=最近任务 4=通知栏 5=快捷设置 6=电源菜单 8=锁屏 9=截图。"""
    return _call("global_action", int(action), op="global_action")


def xime_start_activity(action):
    """用 Intent action 拉起界面（如 android.settings.SETTINGS）；后台启动可能被系统拦截。"""
    return _call("start_activity", str(action), op="start_activity")


_APP_LIST_CODE = """import json
from java import jclass
try:
    ActivityThread = jclass("android.app.ActivityThread")
    app = ActivityThread.currentApplication()
    pm = app.getPackageManager()
    apps = pm.getInstalledApplications(0)
    Intent = jclass("android.content.Intent")
    launch_intent = Intent(Intent.ACTION_MAIN)
    launch_intent.addCategory(Intent.CATEGORY_LAUNCHER)
    resolved = pm.queryIntentActivities(launch_intent, 0)
    launchable = set()
    for i in range(resolved.size()):
        launchable.add(str(resolved.get(i).activityInfo.packageName))
    only_launchable = %r
    query = (%r or "").strip().lower()
    items = []
    for i in range(apps.size()):
        info = apps.get(i)
        try:
            label = str(info.loadLabel(pm))
        except Exception:
            label = ""
        pkg = str(info.packageName)
        if only_launchable and pkg not in launchable:
            continue
        if query and query not in pkg.lower() and query not in label.lower():
            continue
        items.append({"pkg": pkg, "label": label})
    items.sort(key=lambda x: x["pkg"])
    r = {"ok": True, "count": len(items), "apps": items}
except Exception as exc:
    r = {"ok": False, "op": "list_apps", "error": str(exc)}
"""

_APP_LAUNCH_CODE = """import json
from java import jclass
pkg = %r
try:
    ActivityThread = jclass("android.app.ActivityThread")
    app = ActivityThread.currentApplication()
    pm = app.getPackageManager()
    Intent = jclass("android.content.Intent")
    intent = pm.getLaunchIntentForPackage(pkg)
    if intent is None:
        r = {"ok": False, "op": "launch_app", "pkg": pkg,
             "error": "该应用没有可启动入口（包名不存在或未安装）"}
    else:
        intent.addFlags(Intent.FLAG_ACTIVITY_NEW_TASK)
        app.startActivity(intent)
        r = {"ok": True, "op": "launch_app", "pkg": pkg, "started": True}
except Exception as exc:
    r = {"ok": False, "op": "launch_app", "pkg": pkg, "error": str(exc)}
"""


def xime_list_apps(query="", only_launchable=False):
    """列出已安装应用，返回 pkg/label。query 按包名或应用名模糊过滤；
    only_launchable=True 只返回桌面可启动的应用。"""
    config = get_config()
    code = _APP_LIST_CODE % (bool(only_launchable), str(query or ""))
    return _send_device_code(code, "list_apps", None, config)


def xime_launch_app(package):
    """按包名启动应用（通过 PackageManager 的启动 Intent）。"""
    package = str(package or "").strip()
    if not package:
        return {"ok": False, "op": "launch_app", "error": "package 不能为空"}
    config = get_config()
    code = _APP_LAUNCH_CODE % package
    return _send_device_code(code, "launch_app", None, config)


def xime_event_state():
    """读取无障碍事件版本号与最近一次事件（静止时版本号不变，可用于判断是否需要刷新）。"""
    return _call("event_state", op="event_state")


def xime_event_capture(enabled=True, ttl_ms=30000):
    """开启/关闭无障碍事件监听，ttl_ms 到期自动关闭；仅在自动化期间开启。"""
    return _call("event_capture", bool(enabled), int(ttl_ms), op="event_capture")


def xime_windows():
    """列出当前窗口（id/type/layer/title/active/focused/bounds/package）。"""
    return _call("windows", op="windows")


def xime_perform_action(node_ref, action, text=""):
    """对节点执行通用无障碍动作。node_ref 形如 w0:p1.2；
    action 支持 click/long_click/scroll_forward/scroll_backward/set_text/paste/select/focus/clear_focus。"""
    return _call("perform_action", str(node_ref), str(action), str(text),
                 op="perform_action")


def xime_set_text(node_ref, text):
    """向节点写入文本（ACTION_SET_TEXT，失败时设备端会回读校验）。"""
    return _call("set_text", str(node_ref), str(text), op="set_text")


def xime_screenshot(window_id=-1):
    """按需截图（-1 为整屏）；返回本地文件路径与尺寸，低版本或受限 ROM 会明确报错。"""
    return _call("screenshot", int(window_id), op="screenshot")



# 无障碍 API 不能点亮屏幕：改用 PowerManager 的 SCREEN_BRIGHT_WAKE_LOCK(10) +
# ACQUIRE_CAUSES_WAKEUP(0x10000000) = 0x1000000A。只亮屏，不清除密码锁屏。
_WAKE_CODE = """import json
try:
    from java import jclass
    _app = jclass("com.chaquo.python.Python").getPlatform().getApplication()
    _pm = _app.getSystemService("power")
    _before = bool(_pm.isInteractive())
    if not _before:
        _pm.newWakeLock(0x1000000A, "xime:rpc_wake").acquire(%d)
    r = json.dumps({"ok": True, "op": "wake", "interactive_before": _before,
                    "interactive": bool(_pm.isInteractive())}, ensure_ascii=False)
except Exception as exc:
    r = json.dumps({"ok": False, "op": "wake", "error": str(exc)},
                   ensure_ascii=False)
"""


def xime_wake(seconds=10):
    """点亮屏幕（seconds 秒后自动释放）；有密码的锁屏仍需人工解锁。"""
    config = get_config()
    wake_ms = max(1000, int(float(seconds) * 1000))
    return _send_device_code(_WAKE_CODE % wake_ms, "wake", None, config)


_MCP_INSTRUCTIONS = (
    "远端手机的无障碍服务（XimeAccessibilityService）。所有工具经 MQTT RPC "
    "把控制代码动态下发到手机进程执行，手机端只需运行 server_mqtt 与内置 "
    "feature_touch。读界面用 dump_ui / find_text，点击优先用 tap_text（自动取"
    "节点中心点），等待界面用 wait_text；坐标手势用 tap / long_press / swipe / "
    "drag / pinch / gesture；系统操作用 global_action；启动界面用 "
    "start_activity；列应用用 list_apps，按包名启动应用用 launch_app；"
    "事件版本用 event_state，按需开关事件监听用 event_capture，列窗口用 "
    "windows，按节点路径执行通用动作用 perform_action，写文本用 set_text，"
    "按需截图用 screenshot；"
    "失败时先 status 看连接；需要完整体检时用 status(deep=True)。"
)


_TOOL_PREFIX = "xime_"


def _tool_name(fn):
    """对外暴露的 MCP 工具名：去掉内部函数名的 xime_ 前缀。"""
    name = getattr(fn, "__name__", "")
    return name[len(_TOOL_PREFIX):] if name.startswith(_TOOL_PREFIX) else name


def _log_tool(fn):
    """给每个工具加 stderr 日志和进程内事件（stdio 下 stdout 必须只走协议）。"""

    tool_name = _tool_name(fn)

    @functools.wraps(fn)
    def wrapper(*args, **kwargs):
        started = time.monotonic()
        params = ", ".join(
            [repr(a) for a in args] + ["%s=%r" % (k, v) for k, v in kwargs.items()])
        params = params[:160]
        try:
            result = fn(*args, **kwargs)
        except Exception as exc:
            elapsed = time.monotonic() - started
            logger.warning("%s 失败 %.2fs: %s: %s", tool_name,
                           elapsed, type(exc).__name__, exc)
            _event("tool_error", tool=tool_name, elapsed_ms=round(elapsed * 1000),
                   params=params, error="%s: %s" % (type(exc).__name__, exc))
            raise
        elapsed = time.monotonic() - started
        ok = result.get("ok") if isinstance(result, dict) else None
        logger.info("%s -> ok=%s %.2fs", tool_name, ok, elapsed)
        extra = {}
        if isinstance(result, dict):
            if result.get("error"):
                extra["error"] = str(result["error"])[:200]
            if result.get("count") is not None:
                extra["count"] = result["count"]
        _event("tool_call", tool=tool_name, ok=ok,
               elapsed_ms=round(elapsed * 1000), params=params, **extra)
        return result

    return wrapper


_TOOLS = (
    xime_status,
    xime_dump_ui,
    xime_find_text,
    xime_tap_text,
    xime_wait_text,
    xime_tap,
    xime_long_press,
    xime_swipe,
    xime_drag,
    xime_pinch,
    xime_gesture,
    xime_global_action,
    xime_start_activity,
    xime_list_apps,
    xime_launch_app,
    xime_event_state,
    xime_event_capture,
    xime_windows,
    xime_perform_action,
    xime_set_text,
    xime_screenshot,
    xime_wake,
)


def register_tools(server):
    """把上面的工具注册成 MCP 工具（对外名称去掉 xime_ 前缀）。"""
    for fn in _TOOLS:
        server.tool(name=_tool_name(fn))(_log_tool(fn))


# ----------------------------------------------------------------------------
# 元素视图：浏览器里用 SVG 画远端界面（不是截屏，只按 dump_ui 的 bounds 绘制）
# ----------------------------------------------------------------------------

def ui_html(p):
    """元素视图页面（HTTP RPC：``/ui_html(p)``，根路径自动跳转）。"""
    return xime_ui.ui_html(p)



# ----------------------------------------------------------------------------
# 同口 HTTP：server_http 负责端口与 /r= RPC，MCP ASGI 以 /mcp 挂在同一端口
#
# 结构与 pty 一致，但不 import pty：RPC 完全复用 server_http；MCP 部分把
# pty 097 里已验证过的「前置路径分流 + 专用事件循环 + 手动 lifespan」桥接
# 逻辑在本文件内实现。这样 /r= 的语义（别名、unquote、response、Basic Auth、
# 多行代码）与 server_http 原生行为一致，不再维护第二套执行器。
# ----------------------------------------------------------------------------

_HTTP_SERVER = None
_MCP_SERVER = None
_MCP_ASGI_APP = None
_MCP_HTTP_PATHS = frozenset()
_MCP_LOOP = None
_MCP_LOOP_LOCK = threading.Lock()


def _mcp_loop_main(loop):
    global _MCP_LOOP
    asyncio.set_event_loop(loop)
    _MCP_LOOP = loop
    loop.run_forever()


def _mcp_ensure_loop():
    """懒启动 MCP ASGI 专用事件循环线程。"""
    with _MCP_LOOP_LOCK:
        if _MCP_LOOP is None or _MCP_LOOP.is_closed():
            loop = asyncio.new_event_loop()
            threading.Thread(target=_mcp_loop_main, args=(loop,),
                             name="xime-mcp-loop", daemon=True).start()
            deadline = time.monotonic() + 5
            while _MCP_LOOP is None and time.monotonic() < deadline:
                time.sleep(0.01)
    return _MCP_LOOP


def _mcp_drive_lifespan(app, loop):
    """手动驱动 ASGI lifespan（等价 uvicorn 启动阶段），起 session manager 等。"""
    q = asyncio.Queue()

    async def receive():
        return await q.get()

    async def send(message):
        if message.get("type") == "lifespan.startup.failed":
            logger.warning("MCP lifespan 启动失败: %s", message.get("message", "?"))

    async def _drive():
        await q.put({"type": "lifespan.startup"})
        await app({"type": "lifespan", "asgi": {"version": "3.0",
                                                "spec_version": "2.0"},
                   "state": {}}, receive, send)

    asyncio.run_coroutine_threadsafe(_drive(), loop)


class _MCPHttpBridge:
    """把 server_http 请求线程里的 /mcp 请求桥到 MCP ASGI 应用。"""

    def __init__(self, handler):
        self.h = handler

    def run(self, app):
        h = self.h
        path, _, query = h.path.partition("?")
        try:
            length = int(h.headers.get("Content-Length") or 0)
        except ValueError:
            length = 0
        body = h.rfile.read(length) if length > 0 else b""
        body_state = {"sent": False}
        state = {"started": False, "streaming": False}

        async def receive():
            if not body_state["sent"]:
                body_state["sent"] = True
                return {"type": "http.request", "body": body,
                        "more_body": False}
            await asyncio.Event().wait()

        async def send(message):
            if message["type"] == "http.response.start":
                state["started"] = True
                h.log_request(message["status"])
                h.send_response_only(message["status"])
                has_len = False
                for k, v in message.get("headers", []):
                    name = bytes(k).decode("latin-1")
                    value = bytes(v).decode("latin-1")
                    low = name.lower()
                    if low in ("connection", "transfer-encoding"):
                        continue
                    if low == "content-length":
                        has_len = True
                    h.send_header(name, value)
                state["streaming"] = not has_len
                if state["streaming"]:
                    h.send_header("Connection", "close")
                h.end_headers()
            elif message["type"] == "http.response.body":
                chunk = message.get("body", b"") or b""
                if chunk:
                    h.wfile.write(chunk)
                    h.wfile.flush()
                if state["streaming"] and not message.get("more_body"):
                    h.close_connection = True

        scope = {
            "type": "http",
            "asgi": {"version": "3.0", "spec_version": "2.3"},
            "http_version": h.request_version.rsplit("/", 1)[-1] or "1.1",
            "method": h.command,
            "scheme": "http",
            "path": path,
            "query_string": query.encode("latin-1"),
            "root_path": "",
            "server": (h.server.server_address[0], h.server.server_address[1]),
            "client": (h.client_address[0], h.client_address[1]),
            "headers": [(k.lower().encode("latin-1"), v.encode("latin-1"))
                        for k, v in h.headers.items()],
        }
        try:
            fut = asyncio.run_coroutine_threadsafe(app(scope, receive, send),
                                                   _mcp_ensure_loop())
            fut.result()
        except (ConnectionError, BrokenPipeError, OSError):
            h.close_connection = True
        except Exception as exc:
            logger.warning("MCP 桥接异常: %r", exc)
            h.close_connection = True
            if not state["started"]:
                try:
                    h.send_error(502, "MCP bridge error")
                except Exception:
                    pass


def _mcp_hook_handler(Handler):
    """给 server_http.RPCRequestHandler 打前置分发钩子（幂等）。"""
    if not hasattr(Handler, "do_DELETE"):
        def _do_delete(self):
            self.send_error(501, "Unsupported method (%r)" % self.command)
        Handler.do_DELETE = _do_delete

    def _wrap(fn):
        def wrapper(self, *args, **kwargs):
            if (_MCP_ASGI_APP is not None
                    and self.path.split("?", 1)[0] in _MCP_HTTP_PATHS):
                _MCPHttpBridge(self).run(_MCP_ASGI_APP)
                return None
            return fn(self, *args, **kwargs)
        wrapper.__name__ = getattr(fn, "__name__", "handler")
        wrapper.__doc__ = getattr(fn, "__doc__", None)
        wrapper._xime_mcp_wrapped = True
        return wrapper

    for name in ("do_GET", "do_POST", "do_DELETE"):
        base = getattr(Handler, name, None)
        if base is not None and not getattr(base, "_xime_mcp_wrapped", False):
            setattr(Handler, name, _wrap(base))


def _mcp_access_log_off(handler):
    """server_http 日志过滤器：MCP 端点访问行静音。"""
    if not _MCP_HTTP_PATHS:
        return False
    try:
        path = getattr(handler, "path", "") or ""
    except Exception:
        return False
    return path.split("?", 1)[0] in _MCP_HTTP_PATHS


def _mcp_build_asgi(server, host):
    """取 MCP 的 ASGI 应用与它占用的 HTTP 路径（优先 streamable-http）。"""
    if hasattr(server, "streamable_http_app"):
        try:
            try:
                app = server.streamable_http_app(host=str(host), stateless_http=True)
            except TypeError:
                try:
                    app = server.streamable_http_app(host=str(host))
                except TypeError:
                    app = server.streamable_http_app()
            base = str(getattr(getattr(server, "settings", None),
                               "streamable_http_path", None) or "/mcp").rstrip("/")
            return app, {base, base + "/"}
        except Exception as exc:
            logger.warning("streamable-http 应用创建失败，改试 sse: %r", exc)
    if hasattr(server, "sse_app"):
        try:
            try:
                app = server.sse_app(host=str(host))
            except TypeError:
                app = server.sse_app()
            st = getattr(server, "settings", None)
            sse = str(getattr(st, "sse_path", None) or "/sse").rstrip("/")
            msg = str(getattr(st, "message_path", None) or "/messages").rstrip("/")
            return app, {sse, sse + "/", msg, msg + "/"}
        except Exception as exc:
            logger.warning("sse 应用创建失败: %r", exc)
    return None, set()


def _mount_mcp(server, host):
    """把 MCP 挂到已启动的 server_http 口上，返回实际路径列表。"""
    global _MCP_SERVER, _MCP_ASGI_APP, _MCP_HTTP_PATHS
    import server_http

    app, paths = _mcp_build_asgi(server, host)
    if app is None or not paths:
        raise RuntimeError("当前 mcp SDK 未提供可挂载的 HTTP 应用")
    _mcp_hook_handler(server_http.RPCRequestHandler)
    _mcp_drive_lifespan(app, _mcp_ensure_loop())
    _MCP_SERVER = server
    _MCP_HTTP_PATHS = frozenset(paths)
    _MCP_ASGI_APP = app
    try:
        server_http.add_log_filter("xime-mcp", _mcp_access_log_off)
    except Exception as exc:
        logger.warning("MCP 访问日志静音注册失败: %r", exc)
    return sorted(paths)


def _run_http(server, args):
    """默认 HTTP 模式：server_http 起 RPC 口，MCP 旁挂同口。"""
    global _HTTP_SERVER
    import server_http

    http_server = server_http.start_rpc_server(
        port=args.port,
        ip=args.host,
        globals=globals(),
        log_sink=lambda line: logger.info("%s", line.rstrip()),
        persist_aliases=False,
        alias_url=server_http.ALIAS_URL_DASH,
        redirect_root="/ui_html(p)",
        # 元素视图数据面：浏览器 <-> 本进程 /uiws；本进程 <-> 手机两个 MQTT topic
        websocket_handlers={xime_ui.WS_PATH: xime_ui.ws_handler},
    )
    _HTTP_SERVER = http_server
    paths = _mount_mcp(server, args.host)
    logger.info("同口服务：MCP http://%s:%d%s、RPC http://%s:%d/<code>、"
                "元素视图 http://%s:%d/ui_html(p)",
                args.host, args.port, paths[0],
                args.host, args.port, args.host, args.port)
    http_server.thread.join()


def _new_server():
    """构造 MCP server（mcp 1.x/2.x 双兼容）；未安装 mcp 时抛 RuntimeError。"""
    try:
        from mcp.server.mcpserver import MCPServer as _ServerClass  # mcp 2.x
        kind = "2"
    except ImportError:
        try:
            from mcp.server.fastmcp import FastMCP as _ServerClass  # mcp 1.x
            kind = "1"
        except ImportError as exc:
            raise RuntimeError('未安装 mcp 包，先执行 pip install "mcp[cli]"') from exc
    try:
        return _ServerClass("xime", instructions=_MCP_INSTRUCTIONS), kind
    except TypeError:  # 老版本构造器不认 instructions
        return _ServerClass("xime"), kind


def main(argv=None):
    global CONFIG_PATH
    parser = argparse.ArgumentParser(description="xime 无障碍服务 MCP 客户端")
    parser.add_argument("--config", default=CONFIG_PATH, help="rpc.json 路径（默认仓库根目录）")
    parser.add_argument("--transport", choices=("stdio", "http"),
                        default="http", help="http=server_http 同口 MCP+RPC（默认）；stdio=只走 stdin/stdout")
    parser.add_argument("--host", default="0.0.0.0", help="HTTP 模式监听地址（远程访问用 0.0.0.0）")
    parser.add_argument("--port", type=int, default=5555, help="HTTP 模式监听端口")
    parser.add_argument("--selftest", action="store_true", help="只调一次手机 status 并打印结果")
    # 连接参数直接复用 client_mqtt 的别名表：-t/--topic/--request_topic、-k/--key/--private_key、
    # -mb/--min、-to/--timeout 等与 client 命令行完全一致，非空时覆盖 rpc.json。
    parser.add_argument(
        *client_mqtt._cli_opts(*client_mqtt.alias_request_topic),
        dest="request_topic", default=None,
        help="覆盖 xime_mcp.request_topic（手机 server_mqtt 监听的 topic）")
    parser.add_argument(
        *client_mqtt._cli_opts(*client_mqtt.alias_reply_topic),
        dest="reply_topic", default=None,
        help="覆盖 xime_mcp.reply_topic")
    parser.add_argument(
        *client_mqtt._cli_opts(*client_mqtt.alias_private_key),
        dest="private_key", default=None,
        help="覆盖 xime_mcp.private_key（支持 2**222 / k=表达式 / PEM / 文件路径）")
    parser.add_argument(
        *client_mqtt._cli_opts(*client_mqtt.alias_timeout),
        dest="rpc_timeout", type=float, default=None,
        help="覆盖 xime_mcp.timeout（秒）")
    parser.add_argument(
        *client_mqtt._cli_opts(*client_mqtt.alias_min_broker_count),
        dest="min_broker_count", type=int, default=None,
        help="覆盖 xime_mcp.min_broker_count")
    parser.add_argument(
        *client_mqtt._cli_opts(*client_mqtt.alias_allow_no_pub),
        dest="allow_no_pub", action="store_true", default=None,
        help="覆盖 xime_mcp.allow_no_server_pubkey_response=True")
    args = parser.parse_args(argv)

    logging.basicConfig(stream=sys.stderr, level=logging.INFO,
                        format="[%(asctime)s] %(levelname)s %(name)s: %(message)s")
    CONFIG_PATH = args.config
    _CLI_OVERRIDES.update({
        "request_topic": (args.request_topic or "").strip() or None,
        "reply_topic": (args.reply_topic or "").strip() or None,
        "timeout": args.rpc_timeout,
        "min_broker_count": args.min_broker_count,
        "allow_no_server_pubkey_response": True if args.allow_no_pub else None,
        "private_key": _normalize_key(args.private_key) if args.private_key else None,
    })

    if args.selftest:
        print(json.dumps(xime_status(), ensure_ascii=False, indent=2))
        return 0

    try:
        server, kind = _new_server()
    except RuntimeError as exc:
        logger.error("%s", exc)
        return 2
    register_tools(server)
    _event("start", transport=args.transport, config=CONFIG_PATH,
           host=args.host if args.transport == "http" else None,
           port=args.port if args.transport == "http" else None)
    logger.info("xime_mcp 启动：transport=%s，配置=%s", args.transport, CONFIG_PATH)
    try:
        if args.transport == "stdio":
            logger.info("stdio 模式：MCP 只走 stdin/stdout，没有 HTTP/RPC 端口；"
                        "需要 /r= 自省请用 --transport http --port 5555")
            try:
                server.run(transport="stdio")
            except TypeError:
                server.run()
        else:
            _run_http(server, args)
    except KeyboardInterrupt:
        pass
    return 0


if __name__ == "__main__":
    sys.exit(main())
