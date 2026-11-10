#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""一键把 C:\\test\\github\\Trae-AutoCheckin 配成两端 Syncthing 双向同步。

本机  : 直接调用 http://127.0.0.1:8384 REST（X-API-Key 从 config.xml 自动读取）
远端  : 经 MQTT RPC 在远端进程内调用它的 127.0.0.1:8384 REST（key 同样自动读取）

用法:
    python mqtt_syncthing.py            # 配置 + 等待首次同步完成
    python mqtt_syncthing.py --check    # 只看状态，不改配置

RPC 参数（topic / timeout / k / a）来自 rpc.json 的 "mqtt_syncthing" 段，代码里只保留兜底默认值。

幂等：folder 已存在时只补齐对端设备，不重复创建。
"""
from __future__ import annotations

import argparse
import ast
import json
import os
import sys
import time
import urllib.error
import urllib.request
import xml.etree.ElementTree as ET

_HERE = os.path.dirname(os.path.abspath(__file__))
_ROOT = os.path.dirname(_HERE)
for _p in (_HERE, _ROOT):
    if _p not in sys.path:
        sys.path.insert(0, _p)

import rpc_config

# ---------------------------------------------------------------- 配置
FOLDER_ID = "trae-autocheckin"
FOLDER_PATH = r"C:\test\github\Trae-AutoCheckin"
FOLDER_LABEL = "Trae-AutoCheckin"

DEFAULT_RPC_CFG = {          # 仅兜底：rpc.json 缺失/坏掉时才用这些值
    "request_topic": "lenovo",
    "timeout": 40,
    "k": 2 ** 222,
    "a": 1,
}
# 与 server_mqtt / server_http / client 共用同一份 rpc.json（路径由 rpc_config
# 唯一持有）。旧实现写死 tools/rpc.json，该文件不存在，本段配置永远读不到。
RPC_CFG_PATH = str(rpc_config.get_config_path())


def load_rpc_cfg(path: str = RPC_CFG_PATH) -> dict:
    """读 rpc.json 的 mqtt_syncthing 段，逐键容错回落默认值。

    文件缺失、坏 JSON、类型写错都只 WARNING 并回落，不打断脚本（见 AGENTS.md 铁律）。
    """
    cfg = dict(DEFAULT_RPC_CFG)
    try:
        with open(path, "r", encoding="utf-8") as fh:
            raw = json.load(fh)
    except FileNotFoundError:
        print(f"WARNING: 未找到 {path}，mqtt_syncthing 使用默认值", file=sys.stderr)
        return cfg
    except Exception as e:
        print(f"WARNING: {path} 解析失败（{e}），mqtt_syncthing 使用默认值", file=sys.stderr)
        return cfg

    if not isinstance(raw, dict):
        print(f"WARNING: {path} 顶层不是对象，mqtt_syncthing 使用默认值", file=sys.stderr)
        return cfg

    section = raw.get("mqtt_syncthing")
    if not isinstance(section, dict):
        if section is not None:
            print("WARNING: rpc.json 的 mqtt_syncthing 非法，使用默认值", file=sys.stderr)
        return cfg

    topic = section.get("request_topic")
    if isinstance(topic, str) and topic.strip():
        cfg["request_topic"] = topic.strip()
    elif topic is not None:
        print("WARNING: mqtt_syncthing.request_topic 非法，使用默认值", file=sys.stderr)

    timeout = section.get("timeout")
    if isinstance(timeout, (int, float)) and not isinstance(timeout, bool) and timeout > 0:
        cfg["timeout"] = timeout
    elif timeout is not None:
        print("WARNING: mqtt_syncthing.timeout 非法，使用默认值", file=sys.stderr)

    k = section.get("k")
    if isinstance(k, bool):
        k = None
    if isinstance(k, str) and k.strip().isdigit():
        k = int(k.strip())
    if isinstance(k, int) and k > 0:
        cfg["k"] = k
    elif k is not None:
        print("WARNING: mqtt_syncthing.k 非法（要正整数，JSON 里别写成 6.7e66），使用默认值",
              file=sys.stderr)

    a = section.get("a")
    if isinstance(a, bool):
        a = None
    if isinstance(a, int):
        cfg["a"] = a
    elif a is not None:
        print("WARNING: mqtt_syncthing.a 非法，使用默认值", file=sys.stderr)

    return cfg


RPC_CFG = load_rpc_cfg()

LOCAL_CFG = os.path.join(os.environ.get("LOCALAPPDATA", ""), "Syncthing", "config.xml")
LOCAL_API = "http://127.0.0.1:8384"

# 端点更整齐：仓库里产生的缓存/日志不参与同步，避免两端握手冲突
STIGNORE = """(?d)__pycache__
(?d)*.pyc
(?d).trae_remote_fetch.log
"""

WAIT_TOTAL_S = 300


# ---------------------------------------------------------------- HTTP 工具
def _http(method: str, url: str, key: str, body: dict | None = None, timeout: int = 15):
    data = None if body is None else json.dumps(body).encode("utf-8")
    req = urllib.request.Request(url, data=data, method=method)
    req.add_header("X-API-Key", key)
    if data is not None:
        req.add_header("Content-Type", "application/json")
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            raw = r.read().decode("utf-8", "replace")
            try:
                return r.getcode(), json.loads(raw) if raw else None
            except ValueError:
                return r.getcode(), raw
    except urllib.error.HTTPError as e:
        try:
            detail = e.read().decode("utf-8", "replace")[:300]
        except Exception:
            detail = ""
        return e.code, detail
    except Exception as e:  # 连接被拒 / 超时
        return -1, repr(e)


def read_api_key(cfg_path: str) -> str:
    """从 Syncthing config.xml 读 GUI api key；v2 的 REST 头是 X-API-Key。"""
    tree = ET.parse(cfg_path)
    node = tree.getroot().find("gui")
    key = node.findtext("apikey") if node is not None else None
    if not key:
        raise RuntimeError(f"{cfg_path} 里没有 gui/apikey")
    return key.strip()


# ---------------------------------------------------------------- 远端 RPC 侧代码
REMOTE_TEMPLATE = r'''
import json, os, sys, urllib.request, urllib.error, xml.etree.ElementTree as ET

FOLDER_ID    = {folder_id!r}
FOLDER_PATH  = {folder_path!r}
FOLDER_LABEL = {folder_label!r}
DRY_RUN      = {dry_run!r}
STIGNORE     = {stignore!r}

def api_key():
    p = os.path.join(os.environ.get("LOCALAPPDATA", ""), "Syncthing", "config.xml")
    root = ET.parse(p).getroot()
    gui = root.find("gui")
    return gui.findtext("apikey").strip()

KEY = api_key()

def http(method, path, body=None, timeout=15):
    data = None if body is None else json.dumps(body).encode("utf-8")
    req = urllib.request.Request("http://127.0.0.1:8384" + path, data=data, method=method)
    req.add_header("X-API-Key", KEY)
    if data is not None:
        req.add_header("Content-Type", "application/json")
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            raw = r.read().decode("utf-8", "replace")
            try:
                return r.getcode(), (json.loads(raw) if raw else None)
            except ValueError:
                return r.getcode(), raw
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode("utf-8", "replace")[:300]
    except Exception as e:
        return -1, repr(e)

def folder_body(my_id, peer_id):
    dev = lambda d: {{"deviceID": d, "introducedBy": "", "encryptionPassword": ""}}
    return {{
        "id": FOLDER_ID, "label": FOLDER_LABEL, "path": FOLDER_PATH,
        "type": "sendreceive", "filesystemType": "basic",
        "devices": [dev(my_id), dev(peer_id)],
        "rescanIntervalS": 3600, "fsWatcherEnabled": True, "fsWatcherDelayS": 10,
        "ignorePerms": False, "autoNormalize": True, "paused": False,
        "minDiskFree": {{"value": 104857600, "unit": "B"}},
        "versioning": {{"type": "", "params": {{}}, "cleanupIntervalS": 3600,
                        "fsPath": "", "fsType": "basic"}},
        "maxConflicts": 10, "markerName": ".stfolder",
        "copyRangeMethod": "standard", "modTimeWindowS": 0,
    }}

code, st = http("GET", "/rest/system/status")
my_id = (st or {{}}).get("myID") if isinstance(st, dict) else None
code, folders = http("GET", "/rest/config/folders")
folders = folders if isinstance(folders, list) else []

existing = next((f for f in folders if f.get("id") == FOLDER_ID), None)
if existing is None:
    existing = next((f for f in folders
                     if os.path.normcase((f.get("path") or "").rstrip("\\")) ==
                     os.path.normcase(FOLDER_PATH.rstrip("\\"))), None)

# 对端（本机）设备 ID 由外部传入
PEER_ID = {peer_id!r}
action = "noop"
folder_after = existing

if PEER_ID:
    if existing is None:
        if DRY_RUN:
            action = "would-create"
        else:
            f = folder_body(my_id, PEER_ID)
            f["id"] = f["id"] or FOLDER_ID
            action = "create:%s" % http("POST", "/rest/config/folders", f)[0]
            folder_after = f
    else:
        ids = [d.get("deviceID") for d in (existing.get("devices") or [])]
        if my_id not in ids:
            existing.setdefault("devices", []).append(
                {{"deviceID": my_id, "introducedBy": "", "encryptionPassword": ""}})
        if PEER_ID not in ids:
            existing.setdefault("devices", []).append(
                {{"deviceID": PEER_ID, "introducedBy": "", "encryptionPassword": ""}})
            existing["type"] = existing.get("type") or "sendreceive"
            if DRY_RUN:
                action = "would-share"
            else:
                action = "share:%s" % http("PUT", "/rest/config/folders/" + existing["id"],
                                           existing)[0]
            folder_after = existing
        else:
            action = "already-shared"

# 落地 .stignore（仅当 folder 真存在时）
stignore_written = False
if not DRY_RUN and os.path.isdir(FOLDER_PATH):
    ig = os.path.join(FOLDER_PATH, ".stignore")
    if not os.path.exists(ig):
        with open(ig, "w", encoding="utf-8") as fh:
            fh.write(STIGNORE)
        stignore_written = True

code, db = http("GET", "/rest/db/status?folder=" + FOLDER_ID) if action not in ("noop",) else (-1, None)
db = db if isinstance(db, dict) else {{}}
code, comp = http("GET", "/rest/db/completion?folder=" + FOLDER_ID)
comp = comp if isinstance(comp, dict) else {{}}

r = dict(
    myID=my_id, peer_seen=PEER_ID, action=action,
    exists=existing is not None,
    folder_path=(folder_after or {{}}).get("path"),
    state=db.get("state"), needFiles=db.get("needFiles"), error=db.get("error"),
    completion_pct=None if not comp else round(comp.get("completion", 0), 1),
    haveFiles=db.get("localFiles"), globalFiles=db.get("globalFiles"),
    stignore_written=stignore_written,
    disk_free_mb=round(__import__("shutil").disk_usage("C:\\").free / 1048576),
)
'''


def remote_eval(code: str):
    from client import client_mqtt as mq

    d = mq.rpc(code, RPC_CFG["request_topic"], timeout=RPC_CFG["timeout"],
               k=RPC_CFG["k"], a=RPC_CFG["a"])
    raw = d.pop("r", None)
    if d.get("error"):
        raise RuntimeError(f"远端 RPC 执行失败: {d['error']}")
    try:
        return ast.literal_eval(raw) if isinstance(raw, str) else raw
    finally:
        try:
            mq.stop()
        except Exception:
            pass


# ---------------------------------------------------------------- 本机侧
def local_setup(remote_id: str, dry_run: bool, local_key: str) -> dict:
    code, st = _http("GET", f"{LOCAL_API}/rest/system/status", local_key)
    if not isinstance(st, dict):
        raise RuntimeError(f"本机 Syncthing 不可用: {st}")
    my_id = st["myID"]

    code, folders = _http("GET", f"{LOCAL_API}/rest/config/folders", local_key)
    folders = folders if isinstance(folders, list) else []
    existing = next((f for f in folders if f.get("id") == FOLDER_ID), None)
    if existing is None:
        existing = next((f for f in folders
                         if os.path.normcase((f.get("path") or "").rstrip("\\"))
                         == os.path.normcase(FOLDER_PATH.rstrip("\\"))), None)

    action = "noop"
    folder_after = existing

    if existing is None:
        body = {
            "id": FOLDER_ID, "label": FOLDER_LABEL, "path": FOLDER_PATH,
            "type": "sendreceive", "filesystemType": "basic",
            "devices": [{"deviceID": my_id, "introducedBy": "", "encryptionPassword": ""},
                        {"deviceID": remote_id, "introducedBy": "", "encryptionPassword": ""}],
            "rescanIntervalS": 3600, "fsWatcherEnabled": True, "fsWatcherDelayS": 10,
            "ignorePerms": False, "autoNormalize": True, "paused": False,
            "minDiskFree": {"value": 104857600, "unit": "B"},
            "versioning": {"type": "", "params": {}, "cleanupIntervalS": 3600,
                           "fsPath": "", "fsType": "basic"},
            "maxConflicts": 10, "markerName": ".stfolder",
            "copyRangeMethod": "standard", "modTimeWindowS": 0,
        }
        if dry_run:
            action = "would-create"
        else:
            c, _ = _http("POST", f"{LOCAL_API}/rest/config/folders", local_key, body)
            action = f"create:{c}"
            if c >= 400:
                raise RuntimeError(f"本机创建 folder 失败 HTTP {c}")
        folder_after = body
    else:
        ids = [d.get("deviceID") for d in (existing.get("devices") or [])]
        if remote_id not in ids:
            existing.setdefault("devices", []).append(
                {"deviceID": remote_id, "introducedBy": "", "encryptionPassword": ""})
            if dry_run:
                action = "would-share"
            else:
                c, _ = _http("PUT", f"{LOCAL_API}/rest/config/folders/{existing['id']}",
                             local_key, existing)
                action = f"share:{c}"
                if c >= 400:
                    raise RuntimeError(f"本机更新 folder 失败 HTTP {c}")
        else:
            action = "already-shared"
        folder_after = existing

    if not dry_run and os.path.isdir(FOLDER_PATH):
        ig = os.path.join(FOLDER_PATH, ".stignore")
        if not os.path.exists(ig):
            with open(ig, "w", encoding="utf-8") as fh:
                fh.write(STIGNORE)

    return {"myID": my_id, "action": action, "exists": existing is not None,
            "folder_path": (folder_after or {}).get("path")}


def local_db_state(local_key: str) -> dict:
    c, db = _http("GET", f"{LOCAL_API}/rest/db/status?folder={FOLDER_ID}", local_key)
    return db if isinstance(db, dict) else {"error": str(db)}


def show(tag: str, info: dict):
    print(f"[{tag}]")
    for k in ("myID", "exists", "folder_path", "action", "state",
              "needFiles", "haveFiles", "globalFiles", "completion_pct", "error"):
        if k in info and info[k] not in (None, ""):
            print(f"   {k:<16} {info[k]}")


def main() -> int:
    ap = argparse.ArgumentParser(description="配置 Trae-AutoCheckin 两端 Syncthing 双向同步")
    ap.add_argument("--check", action="store_true", help="只看状态，不改配置")
    ap.add_argument("--no-wait", action="store_true", help="配置后不等待首次同步完成")
    args = ap.parse_args()
    dry = args.check

    print("=" * 62)
    print(f"folder id   : {FOLDER_ID}")
    print(f"folder path : {FOLDER_PATH}")
    print(f"remote topic: {RPC_CFG['request_topic']}   (rpc.json: mqtt_syncthing)")
    print(f"rpc params  : timeout={RPC_CFG['timeout']} k={RPC_CFG['k']} a={RPC_CFG['a']}")
    print("=" * 62)

    local_key = read_api_key(LOCAL_CFG)

    # 1) 先拿远端设备 ID（用空 peer 只读）
    remote_pre = remote_eval(REMOTE_TEMPLATE.format(
        folder_id=FOLDER_ID, folder_path=FOLDER_PATH, folder_label=FOLDER_LABEL,
        dry_run=True, stignore=STIGNORE, peer_id=""))
    remote_id = remote_pre.get("myID")
    if not remote_id:
        print("无法从远端取到 myID，RPC 未连通")
        return 2

    # 2) 本机配置
    l = local_setup(remote_id, dry, local_key)
    show("本机", l)
    local_id = l["myID"]

    # 3) 远端配置（带上本机设备 ID 完成双向 share）
    r = remote_eval(REMOTE_TEMPLATE.format(
        folder_id=FOLDER_ID, folder_path=FOLDER_PATH, folder_label=FOLDER_LABEL,
        dry_run=dry, stignore=STIGNORE, peer_id=local_id))
    show("远端", r)

    if dry:
        print("\n--check 模式：未修改配置。")
        return 0

    if not os.path.isdir(FOLDER_PATH):
        print(f"\n本机目录不存在，已中止: {FOLDER_PATH}")
        return 3

    if args.no_wait:
        print("\n已配置，--no-wait 跳过等待。")
        return 0

    # 4) 等待首次同步收敛
    print("\n等待首次同步收敛（最多 %d 秒）..." % WAIT_TOTAL_S)
    t0 = time.time()
    last = None
    while time.time() - t0 < WAIT_TOTAL_S:
        db = local_db_state(local_key)
        cur = (db.get("state"), db.get("needFiles"), db.get("needBytes"),
               db.get("localFiles"), db.get("globalFiles"))
        if cur != last:
            print("   本机 state=%-8s needFiles=%-5s files=%s/%s"
                  % (cur[0], cur[1], cur[3], cur[4]))
            last = cur
        try:
            rr = remote_eval(REMOTE_TEMPLATE.format(
                folder_id=FOLDER_ID, folder_path=FOLDER_PATH, folder_label=FOLDER_LABEL,
                dry_run=False, stignore="", peer_id=local_id))
            if cur[0] == "idle" and (cur[1] or 0) == 0 and (rr.get("needFiles") or 0) == 0:
                print(f"\n同步完成，用时 {time.time() - t0:.1f}s")
                show("本机终态", dict(db, myID=local_id, exists=True,
                                    folder_path=db.get("path")))
                show("远端终态", rr)
                return 0
        except Exception as e:
            print(f"   远端查询失败（继续轮询）: {e}")
        time.sleep(5)

    print("\n超时未收敛，请到 Web UI 查看（本机 http://127.0.0.1:8384）")
    return 1


if __name__ == "__main__":
    sys.exit(main())
