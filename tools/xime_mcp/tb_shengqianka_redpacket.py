#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""淘宝「省钱卡 → 点击红包解锁 → 红包兑奖 5/5」全流程（走 xime MCP）。

链路：本机 --HTTP JSON-RPC--> xime MCP (``http://<ip>:5555/mcp``) --MQTT RPC--> 手机。

只用标准库，不依赖 mcp SDK；任何一步失败都只打日志退出，不影响手机端进程。

用法::

    python tb_shengqianka_redpacket.py                       # 默认 http://127.0.0.1:5555/mcp
    python tb_shengqianka_redpacket.py --url http://192.168.1.109:5555/mcp
    python tb_shengqianka_redpacket.py --shot http://192.168.1.109:1122 \
        --shot-out shot.jpg                                  # 结束时抓一张真机截图

识别「红包商品」的规则（两个目标商品共有的结构特征，与图片 URL 无关）::

    商品图左下多了一个 207x252 左右的竖版 Image 节点（源图 -tps-282-344.png），
    它的父节点可点击；点它 = 点商品，进详情页后立刻返回，计数 +1。

注意：dump_ui 必须给足 max_nodes（默认 500 会把右列节点截断，导致漏掉红包角标）；
页面异步渲染，进页/重进后要等角标出现，不能靠固定 sleep 或先下滑。
"""

import argparse
import datetime
import json
import random
import re
import sys
import time
import urllib.request

DEFAULT_URL = "http://127.0.0.1:5555/mcp"
TAOBAO_PKG = "com.taobao.taobao"

# 红包角标：竖版小图，宽 180~245、高 215~285，高宽比 > 1.05
BADGE_W_MIN, BADGE_W_MAX = 180, 245
BADGE_H_MIN, BADGE_H_MAX = 215, 285
BADGE_RATIO_MIN = 1.05

COUNTER_RE = re.compile(r"^(\d+)/5$")
BADGE_URL_HINT = "tps-282-344"
DUMP_NODES = 2000                # 一次 dump 的节点上限（不够会把角标截掉）

# 页面识别结果（按“从具体到笼统”的顺序判定，见 detect_page）
PAGE_REDEEM = "redeem"           # 兑奖页（有 x/5 计数或「下滑寻找」）
PAGE_UNLOCK = "unlock"           # 省钱卡页且露出「点击红包解锁」
PAGE_SQK = "sqk"                 # 省钱卡 WebView 页
PAGE_MINE = "mine"               # 我的淘宝（有可点的省钱卡入口）
PAGE_NATIVE = "native"           # 淘宝原生页（有底部「我的淘宝」tab）
PAGE_OTHER = "other"             # 淘宝其它页面 / 开屏广告 / 浮层
PAGE_NAME = {
    PAGE_REDEEM: "兑奖页", PAGE_UNLOCK: "省钱卡页(有解锁入口)",
    PAGE_SQK: "省钱卡页", PAGE_MINE: "我的淘宝",
    PAGE_NATIVE: "淘宝原生页", PAGE_OTHER: "未知页面",
}

_step = 0
_t0 = time.time()


def log(msg):
    """一个步骤一行（行首墙钟时间 + 累计耗时）；只打关键元素，绝不整棵 UI 树输出。"""
    global _step
    _step += 1
    now = datetime.datetime.now()
    print("%s [%02d %6.1fs] %s" % (
        now.strftime("%Y-%m-%d__%H.%M.%S__.") + "%03d" % (now.microsecond // 1000),
        _step, time.time() - _t0, msg), flush=True)


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
    return dev.call("dump_ui", max_nodes=DUMP_NODES, include_empty=True)


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


def summary(d):
    """dump 的浓缩信息：节点数 / 计数 / 角标数量，一行看完。"""
    bs = badges(d)
    c = counter(d)
    n = len(nodes_of(d))
    return "节点%d%s 计数%s 角标%d(可见%d)" % (
        n, "(截断)" if n >= DUMP_NODES else "",
        ("%d/5" % c) if c is not None else "无", len(bs), len(visible(bs)))


def wait_badges(dev, timeout=8.0, interval=1.2):
    """页面是异步渲染的：反复 dump 直到可见角标出现（首屏就有，别急着下滑）。"""
    t0 = time.time()
    d = dump(dev)
    bs = visible(badges(d))
    while not bs and time.time() - t0 < timeout:
        time.sleep(interval)
        d = dump(dev)
        bs = visible(badges(d))
    log("等角标 %.1fs → 可见%d %s" % (time.time() - t0, len(bs), summary(d)))
    return d, bs


def swipe_up(dev):
    log("上滑一屏")
    dev.call("swipe", x1=540, y1=1900, x2=540, y2=750, duration_ms=500)
    time.sleep(1.4)


def back(dev):
    log("全局返回(back)")
    dev.call("global_action", action=1)
    time.sleep(2.4)


def tap(dev, x, y):
    dev.call("tap", x=x, y=y, duration_ms=90)
    time.sleep(0.2)


# ---------------------------------------------------------------------------
# 页面导航
# ---------------------------------------------------------------------------

def wait_text(dev, text, timeout=12, max_nodes=1500):
    """轮询等文本出现（WebView 首屏渲染慢，不能用固定 sleep），结果压成一行。"""
    t0 = time.time()
    r = dev.call("wait_text", text=text, timeout=timeout, interval=0.6,
                 max_nodes=max_nodes)
    hits = r.get("hits") or []
    clickable = [n for n in hits if n.get("clickable")]
    dt = time.time() - t0
    if hits:
        h = (clickable or hits)[0]
        log("等「%s」%.1fs → 命中%d(可点%d) \"%s\"(%d,%d)" % (
            text, dt, len(hits), len(clickable),
            h.get("text") or h.get("desc") or "", h["x"], h["y"]))
    else:
        log("等「%s」%.1fs → 未命中（上限%ss）" % (text, dt, timeout))
    return r


def _pick_sqk(hits):
    """挑真正的省钱卡入口；排除“韩**A领了省钱卡红包，推荐您购买”这类浮层。"""
    for n in hits:
        s = (n.get("text") or "") + (n.get("desc") or "")
        if not n.get("clickable") or "领了" in s or "推荐" in s:
            continue
        return n
    return None


def hits_in(d, text):
    """在已有 dump 里按文本/描述找节点（不额外发 RPC），字段与 find_text 一致。"""
    out = []
    for n in nodes_of(d):
        t = str(n.get("t") or "")
        ds = str(n.get("d") or "")
        if text not in t and text not in ds:
            continue
        b = n.get("b") or []
        if len(b) != 4:
            continue
        out.append({"text": t, "desc": ds, "clickable": bool(n.get("k")),
                    "x": (b[0] + b[2]) // 2, "y": (b[1] + b[3]) // 2})
    return out


def click_node(dev, n, label=""):
    log("点「%s」(%d,%d)" % (label or n.get("text") or n.get("desc") or "节点",
                            n["x"], n["y"]))
    tap(dev, n["x"], n["y"])
    time.sleep(1.2)


def foreground_pkg(dev):
    """当前前台应用包名（多窗口时取 active 窗口）。"""
    r = dev.call("windows")
    for w in (r.get("windows") or []):
        if isinstance(w, dict) and w.get("active"):
            return str(w.get("pkg") or "")
    return ""


def ensure_taobao(dev, timeout=25):
    """淘宝在前台就不重启（避免把用户当前页面顶掉）；不在才拉起并等它到前台。"""
    pkg = foreground_pkg(dev)
    if pkg == TAOBAO_PKG:
        log("淘宝已在前台，跳过启动")
        return True
    log("启动淘宝 %s（当前前台 %s）" % (TAOBAO_PKG, pkg or "未知"))
    dev.call("launch_app", package=TAOBAO_PKG)
    t0 = time.time()
    while time.time() - t0 < timeout:
        time.sleep(0.8)
        if foreground_pkg(dev) == TAOBAO_PKG:
            log("淘宝已到前台 %.1fs" % (time.time() - t0))
            return True
    log("等淘宝到前台超时 %.1fs，继续尝试识别页面" % timeout)
    return False


def detect_page(dev, d=None):
    """一次 dump 判定当前在哪个页面（从具体到笼统），不做任何点击/等待。"""
    d = dump(dev) if d is None else d
    if counter(d) is not None or hits_in(d, "下滑寻找"):
        return PAGE_REDEEM, d
    if hits_in(d, "点击红包解锁"):
        return PAGE_UNLOCK, d
    if hits_in(d, "购物红包"):
        return PAGE_SQK, d
    if _pick_sqk(hits_in(d, "省钱卡")):
        return PAGE_MINE, d
    if any(h["clickable"] for h in hits_in(d, "我的淘宝")):
        return PAGE_NATIVE, d
    return PAGE_OTHER, d


def open_unlock(dev, timeout=20, d=None):
    """点「点击红包解锁」，进兑奖页（该节点在树的第 445+ 位，max_nodes 必须给足）。"""
    hits = hits_in(d, "点击红包解锁") if d is not None else []
    if not hits:
        hits = wait_text(dev, "点击红包解锁", timeout=timeout,
                         max_nodes=1600).get("hits") or []
    if not hits:
        log("没找到「点击红包解锁」")
        return False
    h = next((n for n in hits if n.get("clickable")), hits[0])
    if not 150 <= h["y"] <= 2250:               # 屏外（多发生在翻页后），先滚回顶部
        log("解锁入口在屏外(y=%d)，滚回顶部" % h["y"])
        dev.call("swipe", x1=540, y1=700, x2=540, y2=2000, duration_ms=600)
        time.sleep(1.0)
        hits = wait_text(dev, "点击红包解锁", timeout=6,
                         max_nodes=1600).get("hits") or []
        if not hits:
            return False
        h = next((n for n in hits if n.get("clickable")), hits[0])
    click_node(dev, h, "点击红包解锁")
    wait_text(dev, "下滑寻找", timeout=12, max_nodes=1200)
    return True


def open_redeem(dev, tries=10):
    """按“当前页面”逐跳导航到兑奖页：已在该页直接返回，不盲探入口。"""
    unknown_backs = 0
    relaunched = False
    for _ in range(tries):
        page, d = detect_page(dev)
        log("识别页面：%s（%s）" % (PAGE_NAME[page], summary(d)))
        if page == PAGE_REDEEM:
            return True
        if page in (PAGE_UNLOCK, PAGE_SQK):
            if open_unlock(dev, timeout=6 if page == PAGE_UNLOCK else 3, d=d):
                return True
            log("省钱卡页没有「点击红包解锁」入口，退回上一页")
            back(dev)
            continue
        if page == PAGE_MINE:
            n = _pick_sqk(hits_in(d, "省钱卡"))
            click_node(dev, n, n.get("text") or n.get("desc") or "省钱卡")
            continue
        if page == PAGE_NATIVE:
            n = next((h for h in hits_in(d, "我的淘宝") if h["clickable"]), None)
            if n is None:
                log("淘宝原生页没有可点的底部「我的淘宝」")
                return False
            click_node(dev, n, "我的淘宝")
            continue
        # 未知页面：先关开屏广告/浮层，再退回一次，最后才重新拉起淘宝
        n = next((h for h in hits_in(d, "跳过") if h["clickable"]), None)
        if n is not None:
            click_node(dev, n, "跳过")
            continue
        if unknown_backs < 2:
            unknown_backs += 1
            back(dev)
            continue
        if relaunched:
            return False
        relaunched = True
        ensure_taobao(dev)
    log("导航尝试 %d 次仍未到兑奖页" % tries)
    return False


def reenter(dev):
    """退回一页后重新按识别结果导航（重进后首屏就有角标，不需要下滑）。"""
    log("退回重新进入兑奖页")
    back(dev)
    if open_redeem(dev, tries=4):
        return True
    ensure_taobao(dev)
    return open_redeem(dev)


# ---------------------------------------------------------------------------
# 主循环
# ---------------------------------------------------------------------------

def run(dev, target=5, max_clicks=20, max_rounds=8):
    d, bs = wait_badges(dev, timeout=8)
    cur = counter(d) or 0
    log("兑奖页就绪，当前 %d/%d，可见角标 %d" % (cur, target, len(bs)))

    clicks = 0
    for rnd in range(1, max_rounds + 1):
        if cur >= target:
            break
        log("第%d轮开始" % rnd)
        hits_this_round = 0
        for _ in range(6):                      # 单轮内边翻边点
            if not bs:                          # 页面异步渲染：先等角标，别急着下滑
                d, bs = wait_badges(dev, timeout=4)
            if not bs:
                swipe_up(dev)
                d = dump(dev)
                bs = visible(badges(d))
                log(summary(d))
                continue
            b = bs.pop(0)
            log("点角标%s (%d,%d)" % (b["url"], b["cx"], b["cy"]))
            tap(dev, b["cx"], b["cy"])
            time.sleep(2.6)                     # 进详情页
            back(dev)                           # 立刻返回
            d = dump(dev)
            new = counter(d) or cur
            bs = visible(badges(d))
            clicks += 1
            log("返回后 %s" % summary(d))
            if new > cur:
                hits_this_round += 1
                log("计数 %d→%d/%d" % (cur, new, target))
                cur = new
            else:
                log("该卡片未计数，跳过")
            if cur >= target:
                break
        if cur >= target:
            break
        if hits_this_round == 0 or clicks >= max_clicks:
            log("本轮命中%d次，已点%d次，退回重进" % (hits_this_round, clicks))
            if not reenter(dev):
                log("重进兑奖页失败，结束")
                break
            bs = []                             # 重进后重新等首屏角标
            if hits_this_round == 0:
                time.sleep(random.uniform(1.5, 3.0))
    final = counter(dump(dev))
    log("结束：%s/%d，总点击 %d 次" % (final, target, clicks))
    return final


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

    log("连接 %s" % args.url)
    dev = Mcp(args.url)
    dev.initialize()
    status = dev.call("status")
    if not (status.get("status") or {}).get("connected"):
        print("手机未连上: %s" % status, file=sys.stderr)
        return 2
    log("手机已连接，开始导航")

    if not args.skip_nav:
        ensure_taobao(dev)
        if not open_redeem(dev):
            print("导航到兑奖页失败（当前页面无法识别或不含入口）", file=sys.stderr)
            return 3

    final = run(dev, target=args.target,
                max_clicks=args.max_clicks, max_rounds=args.max_rounds)
    if args.shot:
        print("截图已保存 %s（%d 字节）" % (args.shot_out, save_shot(args.shot, args.shot_out)))
    return 0 if (final or 0) >= args.target else 1


if __name__ == "__main__":
    sys.exit(main())
