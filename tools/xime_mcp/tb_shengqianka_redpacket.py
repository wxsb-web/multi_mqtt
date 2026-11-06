#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""淘宝「省钱卡 → 点击红包解锁 → 红包兑奖 5/5」全流程（走 xime MCP）。

链路：本机 --HTTP JSON-RPC--> xime MCP (``http://<ip>:5555/mcp``) --MQTT RPC--> 手机。

只用标准库，不依赖 mcp SDK；任何一步失败都只打日志退出，不影响手机端进程。

用法::

    python tb_shengqianka_redpacket.py                       # 默认 192.168.1.100
    python tb_shengqianka_redpacket.py --url http://192.168.1.109:5555/mcp
    python tb_shengqianka_redpacket.py --shot http://192.168.1.109:1122 \
        --shot-out shot.jpg                                  # 结束时抓一张真机截图

识别「红包商品」的规则（两个目标商品共有的结构特征，与图片 URL 无关）::

    商品图左下多了一个 207x252 左右的竖版 Image 节点（源图 -tps-282-344.png），
    它的父节点可点击；点它 = 点商品，进详情页后立刻返回，计数 +1。

注意：dump_ui 必须给足 max_nodes（默认 500 会把右列节点截断，导致漏掉红包角标）。
"""

import argparse
import json
import random
import re
import sys
import time
import urllib.request

DEFAULT_URL = "http://192.168.1.100:5555/mcp"
TAOBAO_PKG = "com.taobao.taobao"

# 红包角标：竖版小图，宽 180~245、高 215~285，高宽比 > 1.05
BADGE_W_MIN, BADGE_W_MAX = 180, 245
BADGE_H_MIN, BADGE_H_MAX = 215, 285
BADGE_RATIO_MIN = 1.05

COUNTER_RE = re.compile(r"^(\d+)/5$")
BADGE_URL_HINT = "tps-282-344"


class Mcp:
    """极简 MCP streamable-HTTP 客户端（initialize + tools/call）。"""

    def __init__(self, url, timeout=30):
        self.url = url
        self.timeout = timeout
        self.session_id = None
        self._rid = 0

    def _post(self, payload):
        data = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        req = urllib.request.Request(self.url, data=data, method="POST")
        req.add_header("Content-Type", "application/json")
        req.add_header("Accept", "application/json, text/event-stream")
        if self.session_id:
            req.add_header("Mcp-Session-Id", self.session_id)
            req.add_header("MCP-Protocol-Version", "2025-06-18")
        with urllib.request.urlopen(req, timeout=self.timeout) as resp:
            self.session_id = resp.headers.get("Mcp-Session-Id") or self.session_id
            return resp.read().decode("utf-8", "replace")

    @staticmethod
    def _payload(body):
        for line in body.splitlines():
            if line.startswith("data:"):
                return json.loads(line[5:].strip())
        return json.loads(body) if body.strip() else {}

    def initialize(self):
        self._rid += 1
        msg = self._payload(self._post({
            "jsonrpc": "2.0", "id": self._rid, "method": "initialize",
            "params": {"protocolVersion": "2025-06-18", "capabilities": {},
                       "clientInfo": {"name": "tb-redpacket", "version": "1.0"}},
        }))
        if "result" not in msg:
            raise RuntimeError("initialize 失败: %s" % msg)
        self._post({"jsonrpc": "2.0", "method": "notifications/initialized"})
        return msg["result"]

    def call(self, name, **arguments):
        self._rid += 1
        msg = self._payload(self._post({
            "jsonrpc": "2.0", "id": self._rid, "method": "tools/call",
            "params": {"name": name, "arguments": arguments},
        }))
        if "error" in msg:
            return {"ok": False, "error": str(msg["error"])}
        content = (msg.get("result") or {}).get("content") or []
        raw = content[0].get("text") if content else "{}"
        try:
            return json.loads(raw)
        except (TypeError, ValueError):
            return {"ok": False, "error": "非 JSON 返回: %r" % raw}


# ---------------------------------------------------------------------------
# 基础动作
# ---------------------------------------------------------------------------

def dump(dev):
    return dev.call("dump_ui", max_nodes=1500, include_empty=True)


def nodes_of(d):
    return d.get("nodes") or []


def counter(d):
    for n in nodes_of(d):
        m = COUNTER_RE.match(str(n.get("t") or ""))
        if m:
            return int(m.group(1))
    return None


def badges(d):
    """当前界面里所有红包角标节点（按结构特征识别，不看图片 ID）。"""
    out = []
    for n in nodes_of(d):
        b = n.get("b") or []
        if len(b) != 4 or not str(n.get("t") or "").startswith("O1CN"):
            continue
        w, h = b[2] - b[0], b[3] - b[1]
        if not (BADGE_W_MIN <= w <= BADGE_W_MAX and BADGE_H_MIN <= h <= BADGE_H_MAX):
            continue
        if h / float(w) <= BADGE_RATIO_MIN:
            continue
        out.append({"cx": (b[0] + b[2]) // 2, "cy": (b[1] + b[3]) // 2,
                    "top": b[1], "bounds": b, "url": str(n.get("t"))[-32:]})
    return out


def visible(bs):
    return [b for b in bs if 280 < b["top"] < 2050]


def swipe_up(dev):
    dev.call("swipe", x1=540, y1=1900, x2=540, y2=750, duration_ms=500)
    time.sleep(1.4)


def back(dev):
    dev.call("global_action", action=1)
    time.sleep(2.4)


def tap(dev, x, y):
    dev.call("tap", x=x, y=y, duration_ms=90)
    time.sleep(0.2)


# ---------------------------------------------------------------------------
# 页面导航
# ---------------------------------------------------------------------------

def open_taobao(dev):
    dev.call("launch_app", package=TAOBAO_PKG)
    time.sleep(2.5)


def open_shengqianka(dev):
    """在「我的淘宝」页点省钱卡入口。"""
    hit = dev.call("find_text", text="我的淘宝")
    for n in (hit.get("hits") or []):
        if n.get("clickable"):
            tap(dev, n["x"], n["y"])
            time.sleep(2.2)
            break
    hit = dev.call("find_text", text="省钱卡")
    hits = [n for n in (hit.get("hits") or []) if n.get("clickable")]
    if not hits:
        return False
    tap(dev, hits[0]["x"], hits[0]["y"])
    time.sleep(2.6)
    return True


def open_unlock(dev):
    """点「点击红包解锁」，进兑奖页。"""
    hit = dev.call("find_text", text="点击红包解锁")
    if not hit.get("hits"):
        return False
    h = hit["hits"][0]
    tap(dev, h["x"], h["y"])
    time.sleep(3.2)
    return True


def reenter(dev):
    """退回省钱卡再进一次，翻两下（用户经验：这样角标会重新出现）。"""
    back(dev)
    time.sleep(0.6)
    if not open_unlock(dev):
        open_shengqianka(dev)
        open_unlock(dev)
    swipe_up(dev)
    swipe_up(dev)


# ---------------------------------------------------------------------------
# 主循环
# ---------------------------------------------------------------------------

def run(dev, target=5, max_clicks=20, max_rounds=8, verbose=True):
    d = dump(dev)
    cur = counter(d) or 0
    log = ["起始计数: %d/%d" % (cur, target)]
    if verbose:
        print(log[-1])

    clicks = 0
    for rnd in range(1, max_rounds + 1):
        if cur >= target:
            break
        hits_this_round = 0
        for _ in range(6):                      # 单轮内边翻边点
            d = dump(dev)
            cur = counter(d) or cur
            bs = visible(badges(d))
            if not bs:
                swipe_up(dev)
                continue
            b = bs[0]
            tap(dev, b["cx"], b["cy"])
            time.sleep(2.6)                     # 进详情页
            back(dev)                           # 立刻返回
            d = dump(dev)
            new = counter(d) or cur
            clicks += 1
            line = "第%d轮 点击(%d,%d) 角标%s → %d/%d" % (
                rnd, b["cx"], b["cy"], b["url"], new, target)
            log.append(line)
            if verbose:
                print(line)
            if new > cur:
                hits_this_round += 1
                cur = new
            else:
                log.append("  该卡片未计数，跳过")
            if cur >= target:
                break
        if cur >= target:
            break
        if hits_this_round == 0 or clicks >= max_clicks:
            log.append("本轮无可点角标，退回省钱卡重新进入")
            if verbose:
                print(log[-1])
            reenter(dev)
            if hits_this_round == 0:
                time.sleep(random.uniform(1.5, 3.0))
    final = counter(dump(dev))
    log.append("结束计数: %s/%d，总点击 %d 次" % (final, target, clicks))
    return final, log


def save_shot(shot_base, out_path):
    """走 HTTP RPC 抓真机截图（大文件，不走 MQTT RPC）。"""
    import urllib.parse
    code = ("response.set_header('Content-Type','image/jpeg')\n"
            "response.set_data(open(feature_touch.screenshot()[42:102],'rb').read())")
    url = shot_base.rstrip("/") + "/r=" + urllib.parse.quote(code, safe="")
    with urllib.request.urlopen(url, timeout=40) as resp:
        blob = resp.read()
    with open(out_path, "wb") as fh:
        fh.write(blob)
    return len(blob)


def main(argv=None):
    ap = argparse.ArgumentParser(description="淘宝省钱卡红包兑奖自动化（xime MCP）")
    ap.add_argument("--url", default=DEFAULT_URL, help="xime MCP 地址")
    ap.add_argument("--target", type=int, default=5, help="目标计数（默认 5）")
    ap.add_argument("--max-clicks", type=int, default=20)
    ap.add_argument("--max-rounds", type=int, default=8)
    ap.add_argument("--skip-nav", action="store_true", help="已在兑奖页时跳过导航")
    ap.add_argument("--shot", default="", help="截图 HTTP RPC 基址，如 http://192.168.1.109:1122")
    ap.add_argument("--shot-out", default="tb_shot.jpg")
    args = ap.parse_args(argv)

    dev = Mcp(args.url)
    dev.initialize()
    status = dev.call("status")
    if not (status.get("status") or {}).get("connected"):
        print("手机未连上: %s" % status, file=sys.stderr)
        return 2
    print("已连接手机，开始流程")

    if not args.skip_nav:
        open_taobao(dev)
        open_shengqianka(dev)
        if not open_unlock(dev):
            print("没找到「点击红包解锁」入口", file=sys.stderr)
            return 3
        swipe_up(dev)
        swipe_up(dev)

    final, log = run(dev, target=args.target,
                     max_clicks=args.max_clicks, max_rounds=args.max_rounds)
    print("\n".join(log))
    if args.shot:
        print("截图已保存 %s（%d 字节）" % (args.shot_out, save_shot(args.shot, args.shot_out)))
    return 0 if (final or 0) >= args.target else 1


if __name__ == "__main__":
    sys.exit(main())
