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
for _path in (_ROOT, _CLIENT_DIR):
    if _path not in sys.path:
        sys.path.insert(0, _path)

import client_mqtt  # noqa: E402  复用 client/rpc 的 MQTT 客户端与配置读取
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


_MCP_INSTRUCTIONS = (
    "远端手机的无障碍服务（XimeAccessibilityService）。所有工具经 MQTT RPC "
    "把控制代码动态下发到手机进程执行，手机端只需运行 server_mqtt 与内置 "
    "feature_touch。读界面用 dump_ui / find_text，点击优先用 tap_text（自动取"
    "节点中心点），等待界面用 wait_text；坐标手势用 tap / long_press / swipe / "
    "drag / pinch / gesture；系统操作用 global_action；启动界面用 "
    "start_activity；列应用用 list_apps，按包名启动应用用 launch_app；"
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
)


def register_tools(server):
    """把上面 15 个函数注册成 MCP 工具（对外名称去掉 xime_ 前缀）。"""
    for fn in _TOOLS:
        server.tool(name=_tool_name(fn))(_log_tool(fn))


# ----------------------------------------------------------------------------
# 元素视图：浏览器里用 SVG 画远端界面（不是截屏，只按 dump_ui 的 bounds 绘制）
# ----------------------------------------------------------------------------

_UI_MAX_NODES = 600


def _ui_payload(max_nodes=_UI_MAX_NODES, include_empty=True):
    """取一次界面节点树供 ui_html 渲染；任何异常都折叠成 ok=False 的 dict。"""
    try:
        result = xime_dump_ui(int(max_nodes), bool(include_empty))
    except Exception as exc:
        return {"ok": False, "op": "dump_ui", "nodes": [], "count": 0,
                "error": "%s: %s" % (type(exc).__name__, exc)}
    if not isinstance(result, dict):
        return {"ok": False, "op": "dump_ui", "nodes": [], "count": 0,
                "error": "设备返回类型不支持: %s" % type(result).__name__}
    result.setdefault("nodes", [])
    return result


def ui_html(p):
    """元素视图页面（HTTP RPC：``/ui_html(p)``，根路径自动跳转）。

    不是截图：只把 dump_ui 返回的节点 bounds 画成 SVG 矩形，按层级用不同
    颜色区分，可点击/可编辑/可滚动各有标记。鼠标点击页面任意位置时，把
    屏幕坐标换算成设备坐标，经 ``/r=xime_tap(x,y)`` 回传到手机。
    """
    p.set_header("Content-Type", "text/html; charset=utf-8")
    p.set_header("Cache-Control", "no-store")
    payload = _ui_payload()
    data = json.dumps(payload, ensure_ascii=False)
    data = data.replace("</", "<\\/")
    p.set_data(_UI_PAGE.replace("__DATA__", data))


_UI_PAGE = r"""<!doctype html>
<html lang="zh-CN">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>xime 元素视图</title>
<style>
* { box-sizing: border-box; }
html, body { height: 100%; margin: 0; overflow: hidden; background: #0b0f14;
  color: #d7e0ea; font: 13px/1.4 system-ui, -apple-system, "Segoe UI", sans-serif; }
#side { position: absolute; top: 0; right: 0; bottom: 0; width: 240px;
  display: flex; flex-direction: column; gap: 12px; padding: 14px 12px;
  border-left: 1px solid #1e2a36; background: #0e141b; }
#side .name { font-weight: 600; color: #7ee787; white-space: nowrap; }
#meta { font-size: 12px; line-height: 1.6; color: #8ea3b7; word-break: break-all;
  max-height: 40vh; overflow: auto; }
button { background: #1b2733; color: #d7e0ea; border: 1px solid #2b3a4a;
  border-radius: 4px; padding: 6px 12px; cursor: pointer; font: inherit; width: 100%; }
button:hover { background: #243444; }
label { display: flex; align-items: center; gap: 6px; color: #8ea3b7;
  font-size: 12px; user-select: none; cursor: pointer; white-space: nowrap; }
#stage { position: absolute; inset: 0 240px 0 0; display: flex; align-items: center;
  justify-content: center; padding: 8px; }
#ui { height: 100%; width: auto; max-width: 100%; background: #0f1620;
  border: 1px solid #22303e; border-radius: 2px; cursor: crosshair;
  touch-action: manipulation; }
#ui rect:hover { stroke: #ffffff !important; stroke-width: 3px !important; }
#msg { position: absolute; left: 12px; bottom: 12px; max-width: calc(100% - 264px);
  padding: 6px 12px; border-radius: 4px; background: rgba(14,20,27,.94);
  border: 1px solid #2b3a4a; color: #d7e0ea; font-size: 12px; white-space: nowrap;
  overflow: hidden; text-overflow: ellipsis; pointer-events: none; }
#msg.err { border-color: #a3342f; color: #ff9a94; }
#legend { margin-top: auto; display: flex; flex-direction: column; gap: 6px;
  padding-top: 10px; border-top: 1px solid #1e2a36; font-size: 11px;
  color: #8ea3b7; }
#legend span { display: flex; align-items: center; }
#legend i { display: inline-block; width: 11px; height: 11px; border-radius: 2px;
  margin-right: 4px; vertical-align: -1px; flex: none; }
</style>
</head>
<body>
<div id="stage"><svg id="ui" xmlns="http://www.w3.org/2000/svg"
  preserveAspectRatio="xMidYMid meet"></svg></div>
<aside id="side">
  <span class="name">xime 元素视图</span>
  <span id="meta">加载中…</span>
  <label><input id="auto" type="checkbox"> 自动刷新</label>
  <button id="refresh">刷新</button>
  <div id="legend">
    <span><i style="background:linear-gradient(90deg,#4ea1ff,#39d0c4,#7ee787,#e3b341,#ff8a65,#c792ea)"></i>不同颜色=不同层级</span>
    <span><i style="background:transparent;border:2px solid #d7e0ea"></i>可点击(粗边)</span>
    <span><i style="background:#ff8a65"></i>可编辑</span>
    <span><i style="background:transparent;border:1px dashed #d7e0ea"></i>可滚动</span>
  </div>
</aside>
<div id="msg"></div>
<script>
const DATA = __DATA__;
const NS = 'http://www.w3.org/2000/svg';
const PALETTE = ['#4ea1ff', '#39d0c4', '#7ee787', '#e3b341',
                 '#ff8a65', '#c792ea', '#f78fb3', '#9fb0c0'];
const svg = document.getElementById('ui');
const metaEl = document.getElementById('meta');
const msgEl = document.getElementById('msg');
let W = 0, H = 0, tapping = false;

function el(tag, attrs) {
  const node = document.createElementNS(NS, tag);
  for (const key in attrs) node.setAttribute(key, attrs[key]);
  return node;
}

function shortClass(name) {
  const text = String(name || '');
  const parts = text.split('.');
  return parts[parts.length - 1] || text;
}

function labelOf(n) {
  return n.t || n.d || (n.id ? String(n.id).split('/').pop() : '')
         || shortClass(n.c) || '?';
}

function summary(data) {
  if (!data || !data.ok) {
    return '读取失败: ' + ((data && data.error) || '未知错误');
  }
  const parts = ['pkg=' + (data.pkg || '(未知)'),
                 '节点=' + (data.count !== undefined ? data.count : (data.nodes || []).length)];
  if (data.elapsed_ms !== undefined) parts.push('RPC ' + data.elapsed_ms + 'ms');
  parts.push('点击画面回传 tap');
  return parts.join(' · ');
}

function setMsg(text, isError) {
  msgEl.textContent = text;
  msgEl.className = isError ? 'err' : '';
}

function describe(n) {
  const flags = [];
  if (n.k) flags.push('可点击');
  if (n.e) flags.push('可编辑');
  if (n.s) flags.push('可滚动');
  if (n.f) flags.push('聚焦');
  const b = n.b || [0, 0, 0, 0];
  return shortClass(n.c) + '  [' + b.join(', ') + ']  ' + labelOf(n).slice(0, 40)
         + (flags.length ? '  ' + flags.join('/') : '');
}

function computeTree(nodes) {
  const stack = [];
  const depths = [];
  const hasChild = [];
  nodes.forEach(function (n, index) {
    const b = n.b;
    hasChild[index] = false;
    if (!b || b.length < 4) { depths.push(0); return; }
    const w = b[2] - b[0], h = b[3] - b[1];
    // 零面积节点不参与层级推导，否则会把真正的叶子误判成容器
    if (w <= 0 || h <= 0) { depths.push(0); return; }
    while (stack.length) {
      const t = nodes[stack[stack.length - 1]].b;
      const inside = b[0] >= t[0] - 1 && b[1] >= t[1] - 1
                  && b[2] <= t[2] + 1 && b[3] <= t[3] + 1;
      if (inside) break;
      stack.pop();
    }
    depths.push(stack.length);
    if (stack.length) hasChild[stack[stack.length - 1]] = true;
    stack.push(index);
  });
  return {depths: depths, hasChild: hasChild};
}

function render() {
  const nodes = (DATA && Array.isArray(DATA.nodes)) ? DATA.nodes : [];
  for (const n of nodes) {
    const b = n && n.b;
    if (!b || b.length < 4) continue;
    W = Math.max(W, b[2]);
    H = Math.max(H, b[3]);
  }
  W = W || 1080;
  H = H || 1920;
  svg.setAttribute('viewBox', '0 0 ' + W + ' ' + H);
  svg.setAttribute('width', W);
  svg.setAttribute('height', H);
  svg.appendChild(el('rect', {x: 0, y: 0, width: W, height: H, fill: '#0f1620'}));

  if (DATA && DATA.pkg) document.title = 'xime 元素视图 · ' + DATA.pkg;
  metaEl.textContent = summary(DATA);
  if (!DATA || !DATA.ok) {
    setMsg(summary(DATA), true);
    return;
  }
  if (!nodes.length) {
    const tip = el('text', {x: W / 2, y: H / 2, 'text-anchor': 'middle',
      'font-size': Math.max(18, Math.round(H / 30)), fill: '#8ea3b7',
      'font-family': 'system-ui, sans-serif'});
    tip.textContent = '未取到节点（界面可能不可访问或已锁屏）';
    svg.appendChild(tip);
    return;
  }

  const tree = computeTree(nodes);

  // 第一遍只画矩形：标签必须最后统一画，否则会被后绘制的子节点矩形盖住
  nodes.forEach(function (n, index) {
    const b = n.b;
    if (!b || b.length < 4) return;
    const x = b[0], y = b[1], w = b[2] - b[0], h = b[3] - b[1];
    if (w <= 0 || h <= 0) return;
    const color = PALETTE[Math.min(tree.depths[index], PALETTE.length - 1)];
    const clickable = !!n.k, editable = !!n.e, scrollable = !!n.s;
    const attrs = {
      x: x, y: y, width: w, height: h, 'rx': 3,
      fill: editable ? '#ff8a65' : color,
      'fill-opacity': clickable ? '0.22' : (editable ? '0.20' : '0.07'),
      stroke: editable ? '#ffb59c' : color,
      'stroke-opacity': clickable ? '0.95' : '0.60',
      'stroke-width': clickable ? '2' : '1',
      'vector-effect': 'non-scaling-stroke'
    };
    if (scrollable && !clickable) attrs['stroke-dasharray'] = '4 3';
    const rect = el('rect', attrs);
    rect.addEventListener('mouseenter', function () {
      metaEl.textContent = describe(n);
    });
    svg.appendChild(rect);
  });

  // 完全相同的 bounds 只让最深层节点出标签，避免多层同名文字压在一起
  const boxOwner = new Map();
  nodes.forEach(function (n, index) {
    const b = n.b;
    if (!b || b.length < 4) return;
    if (b[2] - b[0] <= 0 || b[3] - b[1] <= 0) return;
    const key = b.join(',');
    const prev = boxOwner.get(key);
    if (prev === undefined || tree.depths[index] >= tree.depths[prev]) {
      boxOwner.set(key, index);
    }
  });

  const labeledBoxes = new Set();
  const placedLabels = [];
  let fallback = -1;

  function drawLabel(n, index) {
    const b = n.b;
    const x = b[0], y = b[1], w = b[2] - b[0], h = b[3] - b[1];
    const fontSize = Math.min(26, Math.max(10, h / 2.2));
    if (w < fontSize * 2 || h < fontSize + 4) return false;
    const boxKey = b.join(',');
    if (labeledBoxes.has(boxKey)) return false;
    const maxChars = Math.max(1, Math.floor(
      Math.min(w - 8, W * 0.5) / (fontSize * 0.85)));
    let text = labelOf(n);
    if (text.length > maxChars) text = text.slice(0, maxChars - 1) + '…';

    const label = el('text', {
      x: x + 3, y: 0, 'font-size': fontSize,
      fill: '#ffffff', 'fill-opacity': '0.96', 'pointer-events': 'none',
      'font-family': 'ui-monospace, Consolas, monospace'
    });
    label.textContent = text;
    svg.appendChild(label);

    function measure() {
      try { return label.getComputedTextLength(); } catch (e) { return 0; }
    }
    const maxWidth = Math.min(w - 8, W * 0.5);
    let textWidth = measure();
    // 估算截断后仍可能超宽（中文比等宽字符宽），按真实测量再二分截断到框内
    if (textWidth > maxWidth) {
      let lo = 1, hi = text.length, best = 1;
      while (lo <= hi) {
        const mid = Math.floor((lo + hi) / 2);
        label.textContent = text.slice(0, mid) + '…';
        textWidth = measure();
        if (textWidth <= maxWidth) { best = mid; lo = mid + 1; }
        else { hi = mid - 1; }
      }
      label.textContent = text.slice(0, best) + '…';
      textWidth = measure();
    }
    const chipW = Math.min(w - 2, textWidth + 6);
    const chipH = fontSize * 1.32;
    const lineHeight = fontSize * 1.42;

    // 与已经放好的标签逐行碰撞检测：往下挪，挪不出本节点就放弃这个标签
    let labelTop = y + 2;
    while (labelTop + chipH <= y + h) {
      let clash = false;
      for (const p of placedLabels) {
        if (x + 1 < p.x + p.w && x + 1 + chipW > p.x
            && labelTop < p.y + p.h && labelTop + chipH > p.y) {
          clash = true;
          break;
        }
      }
      if (!clash) break;
      labelTop += lineHeight;
    }
    if (labelTop + chipH > y + h) {
      label.remove();
      return false;
    }
    label.setAttribute('y', labelTop + fontSize);
    placedLabels.push({x: x + 1, y: labelTop, w: chipW, h: chipH});
    labeledBoxes.add(boxKey);

    const chip = el('rect', {
      x: x + 1, y: labelTop, width: chipW, height: chipH,
      'rx': 2, fill: '#0b0f14', 'fill-opacity': '0.78',
      'pointer-events': 'none'
    });
    svg.insertBefore(chip, label);
    return true;
  }

  nodes.forEach(function (n, index) {
    const b = n.b;
    if (!b || b.length < 4) return;
    if (b[2] - b[0] <= 0 || b[3] - b[1] <= 0) return;
    if (boxOwner.get(b.join(',')) !== index) return;
    if (fallback < 0 || tree.depths[index] > tree.depths[fallback]) {
      fallback = index;
    }
    const hasText = !!(n.t || n.d || n.id);
    // 纯容器（无文本但有子节点）不标类名，让真正的叶子节点出标签
    if (!hasText && tree.hasChild[index]) return;
    drawLabel(n, index);
  });

  // 整页都是纯容器、一个标签都没有时，至少标出最深层节点
  if (labeledBoxes.size === 0 && fallback >= 0) {
    drawLabel(nodes[fallback], fallback);
  }
}

function toDevice(ev) {
  const matrix = svg.getScreenCTM();
  if (!matrix) return null;
  const point = svg.createSVGPoint();
  point.x = ev.clientX;
  point.y = ev.clientY;
  const local = point.matrixTransform(matrix.inverse());
  return {x: Math.round(local.x), y: Math.round(local.y)};
}

function marker(x, y) {
  const radius = Math.max(9, Math.round(W / 70));
  const dot = el('circle', {
    cx: x, cy: y, r: radius, fill: '#ffd166', 'fill-opacity': '0.9',
    stroke: '#ffffff', 'stroke-width': '2',
    'vector-effect': 'non-scaling-stroke', 'pointer-events': 'none'
  });
  svg.appendChild(dot);
  setTimeout(function () { dot.remove(); }, 1400);
}

svg.addEventListener('mouseleave', function () {
  metaEl.textContent = summary(DATA);
});

svg.addEventListener('click', function (ev) {
  if (tapping) return;
  const point = toDevice(ev);
  if (!point) return;
  if (point.x < 0 || point.y < 0 || point.x > W || point.y > H) return;
  tapping = true;
  marker(point.x, point.y);
  setMsg('tap ' + point.x + ', ' + point.y + ' …');
  const code = 'xime_tap(' + point.x + ', ' + point.y + ')';
  fetch('/r=' + encodeURIComponent(code), {cache: 'no-store'})
    .then(function (resp) { return resp.text(); })
    .then(function (text) {
      const flat = text.replace(/\s+/g, ' ').slice(0, 300);
      const failed = /'ok':\s*False|"ok":\s*false|Traceback/.test(text);
      setMsg('tap ' + point.x + ',' + point.y + ' → ' + flat, failed);
      setTimeout(function () { location.reload(); }, 1500);
    })
    .catch(function (err) {
      setMsg('tap ' + point.x + ',' + point.y + ' 失败: ' + err, true);
      tapping = false;
    });
});

const autoEl = document.getElementById('auto');
try { autoEl.checked = localStorage.getItem('xime_auto') === '1'; } catch (e) {}
autoEl.addEventListener('change', function () {
  try { localStorage.setItem('xime_auto', autoEl.checked ? '1' : '0'); } catch (e) {}
  if (autoEl.checked) setTimeout(function () { location.reload(); }, 4000);
});
if (autoEl.checked) setTimeout(function () { location.reload(); }, 4000);

document.getElementById('refresh').addEventListener('click', function () {
  location.reload();
});

render();
</script>
</body>
</html>"""


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
    args = parser.parse_args(argv)

    logging.basicConfig(stream=sys.stderr, level=logging.INFO,
                        format="[%(asctime)s] %(levelname)s %(name)s: %(message)s")
    CONFIG_PATH = args.config

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
