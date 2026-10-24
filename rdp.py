"""极简 Windows 远程桌面控制页:纯 HTTP RPC 通道,无 WebSocket。

启动 RPC 服务(默认端口 1144):
    python server_http.py

浏览器访问(URL 即代码,等价于执行 ``import rdp;rdp.html(response)``):
    http://127.0.0.1:1144/import%20rdp;rdp.html(response)

RPC 入口(p 为 server_http 注入的请求级 response 对象):
    rdp.html(p)                       内嵌控制页(默认彩色、不自动刷新)
    rdp.html(p, bw=True)              打开即黑白模式;页面内亦可随时勾选切换
    rdp.screen(p)                     主屏截图 JPEG(含当前光标位置红圈)
    rdp.screen(p, 1)                  黑白(灰度)截图,显著省流量
    rdp.info(p)                       返回 "宽x高"
    rdp.move(p, x, y)                 移动光标
    rdp.click(p, x, y, 'left')        移动并点击;left / right
    rdp.drag(p, x1, y1, x2, y2)       按住拖动(选择模式松手时调用)
    rdp.key(p, 'ctrl+shift+esc')      按键 / 组合键,加号连接

仅适配 Windows:user32 的 keybd_event / mouse_event / SetCursorPos,
截图用 Pillow 的 ImageGrab(主屏,坐标系与 SetCursorPos 一致)。
"""
import ctypes
import io
import time
from ctypes import wintypes

try:
    from PIL import Image, ImageDraw, ImageGrab
except ImportError:          # 允许模块导入成功,调用截图时再给出明确报错
    Image = ImageDraw = ImageGrab = None

user32 = ctypes.windll.user32

jpeg_quality = 65            # 截图 JPEG 质量
click_delay = 0.02           # 按下/抬起之间的间隔(秒)

# ------------------------------------------------------------------ #
# 虚拟键码(VK);标点用 VK_OEM_* 固定码,避免依赖键盘布局
# ------------------------------------------------------------------ #
VK = {
    'ctrl': 0x11, 'control': 0x11, 'alt': 0x12, 'shift': 0x10, 'win': 0x5B,
    'menu': 0x5D, 'apps': 0x5D,
    'enter': 0x0D, 'return': 0x0D, 'esc': 0x1B, 'escape': 0x1B,
    'backspace': 0x08, 'tab': 0x09, 'space': 0x20, 'capslock': 0x14,
    'insert': 0x2D, 'delete': 0x2E, 'del': 0x2E,
    'home': 0x24, 'end': 0x23, 'pageup': 0x21, 'pagedown': 0x22,
    'printscreen': 0x2C, 'prtsc': 0x2C, 'scrolllock': 0x91, 'pause': 0x13,
    'numlock': 0x90,
    'left': 0x25, 'up': 0x26, 'right': 0x27, 'down': 0x28,
    'backtick': 0xC0, 'minus': 0xBD, 'equal': 0xBB,
    'lbracket': 0xDB, 'rbracket': 0xDD, 'backslash': 0xDC,
    'semicolon': 0xBA, 'quote': 0xDE,
    'comma': 0xBC, 'period': 0xBE, 'slash': 0xBF,
}
MODIFIERS = {'ctrl', 'alt', 'shift', 'win'}
# 这些 VK 必须带 KEYEVENTF_EXTENDEDKEY,否则会与小键盘/主键位混淆
EXTENDED_VK = {0x21, 0x22, 0x23, 0x24, 0x25, 0x26, 0x27, 0x28,
               0x2C, 0x2D, 0x2E, 0x5B, 0x5C, 0x5D}


def screen_size():
    return user32.GetSystemMetrics(0), user32.GetSystemMetrics(1)


def _cursor_pos():
    point = wintypes.POINT()
    if user32.GetCursorPos(ctypes.byref(point)):
        return point.x, point.y
    return 0, 0


def screenshot(quality=None, bw=False):
    if ImageGrab is None:
        raise RuntimeError('截图需要 Pillow: pip install pillow')
    image = ImageGrab.grab()                     # 主屏,坐标系同 SetCursorPos
    if bw:
        image = image.convert('L')               # 灰度:体积约为彩色的一半以下
    cursor = wintypes.POINT()
    if user32.GetCursorPos(ctypes.byref(cursor)):
        ring = ImageDraw.Draw(image)
        x, y, r = cursor.x, cursor.y, 7
        ring.ellipse([x - r - 2, y - r - 2, x + r + 2, y + r + 2],
                     outline=0 if bw else (0, 0, 0), width=2)
        ring.ellipse([x - r, y - r, x + r, y + r],
                     outline=255 if bw else (255, 60, 60), width=3)
    buffer = io.BytesIO()
    image.save(buffer, 'JPEG', quality=jpeg_quality if quality is None else quality)
    return buffer.getvalue()


def move_to(x, y):
    user32.SetCursorPos(int(x), int(y))
    return f'move to ({int(x)},{int(y)})'


def click_at(x, y, button='left'):
    width, height = screen_size()
    x = max(0, min(int(x), width - 1))
    y = max(0, min(int(y), height - 1))
    user32.SetCursorPos(x, y)
    time.sleep(click_delay)
    if str(button).lower() == 'right':
        down, up = 0x0008, 0x0010                # MOUSEEVENTF_RIGHTDOWN/UP
    else:
        down, up = 0x0002, 0x0004                # MOUSEEVENTF_LEFTDOWN/UP
    user32.mouse_event(down, 0, 0, 0, 0)
    time.sleep(click_delay)
    user32.mouse_event(up, 0, 0, 0, 0)
    return f'click {str(button).lower()} at ({x},{y})'


def drag_at(x1, y1, x2, y2, button='left', steps=12):
    """从 (x1,y1) 按住拖动到 (x2,y2) 再松开,分段移动贴近真实拖动。"""
    width, height = screen_size()
    x1 = max(0, min(int(x1), width - 1)); y1 = max(0, min(int(y1), height - 1))
    x2 = max(0, min(int(x2), width - 1)); y2 = max(0, min(int(y2), height - 1))
    down, up = (0x0008, 0x0010) if str(button).lower() == 'right' else (0x0002, 0x0004)
    user32.SetCursorPos(x1, y1)
    time.sleep(click_delay)
    user32.mouse_event(down, 0, 0, 0, 0)
    time.sleep(click_delay)
    for index in range(1, steps + 1):
        user32.SetCursorPos(x1 + (x2 - x1) * index // steps,
                            y1 + (y2 - y1) * index // steps)
        time.sleep(0.01)
    time.sleep(click_delay)
    user32.mouse_event(up, 0, 0, 0, 0)
    return f'drag {str(button).lower()} ({x1},{y1})->({x2},{y2})'


def _vk_of(name):
    text = str(name).strip().lower()
    if text in VK:
        return VK[text]
    if len(text) == 1:
        if text.isdigit():
            return 0x30 + int(text)
        if 'a' <= text <= 'z':
            return 0x41 + ord(text) - 97
        code = user32.VkKeyW(ord(text)) & 0xFF   # 布局相关字符回退
        if code:
            return code
    if len(text) in (2, 3) and text[0] == 'f' and text[1:].isdigit():
        index = int(text[1:])
        if 1 <= index <= 24:
            return 0x6F + index
    raise ValueError(f'unknown key: {name!r}')


def _key_event(vk, up=False):
    flags = 0x0002 if up else 0                 # KEYEVENTF_KEYUP
    if vk in EXTENDED_VK:
        flags |= 0x0001                         # KEYEVENTF_EXTENDEDKEY
    user32.keybd_event(vk, 0, flags, 0)


def press(combo):
    """按下组合键:'ctrl+a'、'ctrl+alt+del'、'a'、'shift' 均可。"""
    parts = [s.strip().lower() for s in str(combo).split('+') if s.strip()]
    if not parts:
        raise ValueError('empty key combo')
    vks = [_vk_of(part) for part in parts]
    for vk in vks[:-1]:                         # 依次按下修饰键
        _key_event(vk)
    time.sleep(click_delay)
    _key_event(vks[-1])
    time.sleep(click_delay)
    _key_event(vks[-1], up=True)
    for vk in reversed(vks[:-1]):               # 反序抬起
        _key_event(vk, up=True)
    return 'key ' + '+'.join(parts)


# ------------------------------------------------------------------ #
# HTTP RPC 入口:p / response 由 server_http.handle_rpc 注入
# ------------------------------------------------------------------ #
def html(p, bw=False):
    """控制页;bw=True 时页面默认进入黑白模式(截图走灰度,省流量)。"""
    p.set_header('Content-Type', 'text/html; charset=utf-8')
    p.set_header('Cache-Control', 'no-store')
    p.set_data(PAGE.replace('__BW__', 'true' if bw else 'false'))


def screen(p, bw=False):
    p.set_header('Content-Type', 'image/jpeg')
    p.set_header('Cache-Control', 'no-store')
    # 光标坐标随截图返回:页面据此把红点吸附到远端真实鼠标位置(与截图内空心圈重合)
    p.set_header('X-Cursor', '%d,%d' % _cursor_pos())
    p.set_data(screenshot(bw=bool(bw)))


def info(p):
    width, height = screen_size()
    return f'{width}x{height}'


def move(p, x, y):
    return move_to(x, y)


def click(p, x, y, button='left'):
    return click_at(x, y, button)


def drag(p, x1, y1, x2, y2, button='left'):
    return drag_at(x1, y1, x2, y2, button)


def key(p, combo):
    return press(combo)


PAGE = r'''<!doctype html>
<html lang="zh-CN">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>RDP over HTTP RPC</title>
<style>
:root { color-scheme: dark; --bg: #0e1214; --panel: #151b1f; --line: #2c373d; --text: #e6ecec; --muted: #8fa0a6; --accent: #58c4b2; --err: #ee8f82; }
* { box-sizing: border-box; }
body { margin: 0; overflow-x: hidden; background: var(--bg); color: var(--text); font: 13px/1.5 ui-monospace, SFMono-Regular, Consolas, monospace; }
#stage { position: relative; background: #000; }
#shot { display: block; width: 100%; height: auto; cursor: crosshair; user-select: none; -webkit-user-drag: none; }
/* 待点击坐标标记:大小由 JS 按图片显示比例锁定(与截图内红色空心圈一致,基准14px)、半透明 */
#dot { position: absolute; width: 14px; height: 14px; border-radius: 50%; background: rgba(229,57,53,.45); border: 1px solid rgba(255,255,255,.85); box-shadow: 0 0 6px rgba(0,0,0,.5); transform: translate(-50%,-50%); display: none; pointer-events: none; z-index: 2; }
/* 拖动选择预览:半透明红色长条 */
#dragrect { position: absolute; background: rgba(229,57,53,.25); border: 1px solid rgba(229,57,53,.8); display: none; pointer-events: none; z-index: 3; }
/* 控制条+键盘+日志整体容器:页面双指放大时反向缩放,保持原始视觉大小 */
#panel { transform-origin: 0 0; }
/* 控制行:绝不允许换行;left/right_click 悬浮在本行左右两端,跟随可视区域滑动,永不遮挡截图;
   touch-action:none 屏蔽移动端在本行上触发浏览器级页面拖动/缩放 */
#bar { position: relative; display: flex; flex-wrap: nowrap; gap: 8px; align-items: center; padding: 8px 10px; background: var(--panel); border-top: 1px solid var(--line); border-bottom: 1px solid var(--line); white-space: nowrap; overflow: hidden; touch-action: none; }
#bar .slidy { position: absolute; top: 50%; transform: translateY(-50%); z-index: 5; font-weight: 700; touch-action: none; }
#bar .slidy.dragging { background: var(--accent); color: #08201c; }
/* 刷新:浮动正方形,钉在 left/right 之间的中间 */
#bar .slidyc { position: absolute; top: 50%; transform: translate(-50%, -50%); z-index: 5; width: 36px; height: 36px; padding: 0; font-size: 12px; line-height: 1; touch-action: none; }
#bar > button:not(.slidy):not(.slidyc), #bar label { flex: 0 1 auto; min-width: 0; overflow: hidden; text-overflow: ellipsis; }
.bspace { flex: 1 1 0; min-width: 96px; }
.cspace { flex: 1 1 36px; min-width: 36px; }
#coord { color: var(--accent); flex: 0 1 auto; min-width: 0; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; text-align: center; }
button { background: #222b31; color: var(--text); border: 1px solid var(--line); border-radius: 6px; padding: 6px 12px; font: 12px ui-monospace, Consolas, monospace; cursor: pointer; touch-action: manipulation; user-select: none; -webkit-touch-callout: none; }
button:hover { background: #2c3740; }
button:active { background: var(--accent); color: #08201c; }
#info { color: var(--muted); flex: 0 1 auto; min-width: 0; overflow: hidden; text-overflow: ellipsis; }
label { color: var(--muted); display: flex; align-items: center; gap: 4px; cursor: pointer; }
#keyboard { padding: 8px 6px 12px; background: var(--panel); }
.krow { display: flex; gap: 3px; margin-bottom: 4px; min-width: 0; }
.key { flex: 1 1 0; min-width: 0; height: 34px; padding: 0 4px; overflow: hidden; white-space: nowrap; border-radius: 5px; background: #1d262c; border: 1px solid #33414a; color: var(--text); font: 11px ui-monospace, Consolas, monospace; cursor: pointer; touch-action: manipulation; user-select: none; -webkit-touch-callout: none; }
.key:hover { background: #28343c; }
.key.mod.held { background: var(--accent); border-color: var(--accent); color: #08201c; font-weight: 700; }
#log { height: 220px; overflow-y: auto; padding: 8px 10px; font-size: 12px; white-space: pre-wrap; word-break: break-all; background: #0a0e10; }
.line { border-bottom: 1px dashed #1b2429; padding: 2px 0; }
.err { color: var(--err); } .act { color: var(--accent); }
@media (max-width: 640px) {
  #bar button { padding: 4px 8px; font-size: 11px; }
  #bar label { font-size: 11px; gap: 3px; }
  #info { display: none; }
  .key { font-size: 10px; }
}
@media (max-width: 480px) {
  /* 极窄屏:隐藏标签文字(仅剩复选框)与坐标文本,把行中间让给刷新方块 */
  #bar label { font-size: 0; }
  #coord { display: none; }
}
</style>
</head>
<body>

<div id="stage"><img id="shot" alt="desktop"><div id="dragrect"></div><div id="dot"></div></div>

<div id="panel">
 <div id="panel-inner">
  <div id="bar">
    <button id="btn-left" class="slidy">left_click</button>
    <div class="bspace"></div>
    <label><input type="checkbox" id="auto"> 自动刷新 2s</label>
    <label><input type="checkbox" id="bw"> 黑白模式</label>
    <div class="cspace"></div>
    <span id="coord">坐标: 未选择</span>
    <span id="info"></span>
    <div class="bspace"></div>
    <button id="btn-right" class="slidy">right_click</button>
    <button id="btn-refresh" class="slidyc" title="刷新截图">刷新</button>
  </div>

  <div id="keyboard"></div>
  <div id="log"></div>
 </div>
</div>

<script>
const BASE = location.pathname.split(';')[0] + ';';   // /import%20rdp;
const shot = document.getElementById('shot');
const dot = document.getElementById('dot');
const coordEl = document.getElementById('coord');
const infoEl = document.getElementById('info');
const logEl = document.getElementById('log');
const autoEl = document.getElementById('auto');
const bwEl = document.getElementById('bw');
bwEl.checked = __BW__;                        // 由 URL 参数 rdp.html(p, bw=True) 注入
function screenExpr() { return bwEl.checked ? 'rdp.screen(p,1)' : 'rdp.screen(p)'; }
bwEl.onchange = () => { log('黑白模式: ' + (bwEl.checked ? '开' : '关'), 'act'); refresh(); };

// left/right_click 悬浮在控制行(bar)内左右两端,并跟随可视区域横向滑动:
// 移动端放大后拖动时,两键始终贴住当前可见范围的两边;按钮位于 bar 行内,不会遮挡截图
const bar = document.getElementById('bar');
const btnL = document.getElementById('btn-left');
const btnR = document.getElementById('btn-right');
const btnM = document.getElementById('btn-refresh');
const cspace = document.querySelector('.cspace');   // 行内为刷新方块预留的空位
const panel = document.getElementById('panel');
const panelInner = document.getElementById('panel-inner');
// 控制条+键盘的最大放大倍数:宽排 17 键,到顶时整排可见 ~11 键(17/1.55);
// 1.55 时可视窗口 232px,容纳 left+刷新+right 三键(223px)尚有余量不重叠
const PANEL_CAP = 1.55;
function slideButtons() {
    const vv = window.visualViewport;
    // 页面缩放 s:
    //   s ≤ CAP:panel 保持文档流,随页面自然放大、与图片一起平移;
    //   s > CAP:panel 反向缩放封顶(有效放大恒为 CAP,整排可见 ~10 键),
    //            左缘钉在可视片左缘(永远满屏宽、无空带),内部内容随平移左移
    //            (拖到最右即露出右侧按键,移到尽头则保持),按钮恒有充足空间。
    const zoomed = !!(vv && vv.scale && Math.abs(vv.scale - 1) > 0.01);
    const s = zoomed ? vv.scale : 1;
    const k = zoomed ? Math.min(1, PANEL_CAP / s) : 1;
    const capped = zoomed && k < 1;
    const pageLeft = zoomed ? vv.pageLeft : 0;
    const w = bar.clientWidth;
    let vpLeft, vpWidth;
    if (capped) {
        panel.style.transform = 'translateX(' + pageLeft + 'px) scale(' + k + ')';
        vpWidth = window.innerWidth / (s * k);                       // = innerWidth / CAP,恒定
        const shift = Math.max(0, Math.min(pageLeft, w - vpWidth));  // 内容随平移左移,到尽头保持
        panelInner.style.transform = 'translateX(' + (-shift) + 'px)';
        vpLeft = shift;
    } else {
        panel.style.transform = '';
        panelInner.style.transform = '';
        vpLeft = zoomed ? pageLeft : 0;
        vpWidth = zoomed ? (window.innerWidth / s) : w;
    }
    const pad = 6;
    const bwL = btnL.offsetWidth || 70;
    const bwR = btnR.offsetWidth || 70;
    const bwM = btnM.offsetWidth || 36;
    let lx = Math.max(pad, Math.min(vpLeft + pad, w - bwL - pad));
    let rx = Math.max(lx + bwL + pad, Math.min(vpLeft + vpWidth - bwR - pad, w - bwR - pad));
    btnL.style.transform = '';
    btnR.style.transform = '';
    btnM.style.transform = '';
    btnL.style.left = lx + 'px';
    btnR.style.left = rx + 'px';
    // 刷新方块:left 取其中心点。未缩放钉在行内预留位(.cspace)中心;
    // 缩放拖动跟随可见区域中心;始终夹在左右两键之间
    let target = vpLeft + vpWidth / 2;
    if (!zoomed && cspace) {
        const cr = cspace.getBoundingClientRect();
        const br = bar.getBoundingClientRect();
        target = cr.left + cr.width / 2 - br.left;
    }
    const mx = Math.max(lx + bwL + pad + bwM / 2, Math.min(target, rx - pad - bwM / 2));
    btnM.style.left = mx + 'px';
}
if (window.visualViewport) {
    window.visualViewport.addEventListener('resize', () => requestAnimationFrame(slideButtons));
    window.visualViewport.addEventListener('scroll', slideButtons);
}
window.addEventListener('resize', () => requestAnimationFrame(slideButtons));
window.addEventListener('scroll', slideButtons, true);
window.addEventListener('orientationchange', () => requestAnimationFrame(slideButtons));
window.addEventListener('load', slideButtons);
if (window.ResizeObserver) { new ResizeObserver(slideButtons).observe(bar);
                             new ResizeObserver(slideButtons).observe(cspace); }   // 尺寸实际变化后再校准
slideButtons();
let lastX = null, lastY = null, busy = false, refreshTimer = null;
let lastSx = null, lastSy = null;           // 锚点在截图内的显示像素坐标
const dragrect = document.getElementById('dragrect');

function log(msg, cls) {
    const line = document.createElement('div');
    line.className = 'line' + (cls ? ' ' + cls : '');
    line.textContent = new Date().toTimeString().slice(0, 8) + '  ' + msg;
    logEl.prepend(line);
    while (logEl.children.length > 400) logEl.lastChild.remove();
}

function rpc(expr) {
    const started = performance.now();
    return fetch(BASE + encodeURIComponent(expr), { cache: 'no-store' })
        .then(async resp => {
            const text = await resp.text();
            const ms = Math.round(performance.now() - started);
            log(expr + '  → [' + resp.status + ' ' + ms + 'ms] ' + text.slice(0, 200).trim(), resp.ok ? '' : 'err');
            if (!resp.ok) throw new Error(text.slice(0, 120));
            return text;
        })
        .catch(err => { log('请求失败: ' + err.message, 'err'); throw err; });
}

let lastCursor = null;                        // 最近一次截图携带的远端光标位置
function refresh() {
    if (busy) return;
    busy = true;
    fetch(BASE + screenExpr(), { cache: 'no-store' })
        .then(resp => {
            if (!resp.ok) throw new Error('HTTP ' + resp.status);
            lastCursor = resp.headers.get('X-Cursor');
            return resp.blob();
        })
        .then(blob => {
            const url = URL.createObjectURL(blob);
            const old = shot.src;
            shot.src = url;
            if (old && old.startsWith('blob:')) URL.revokeObjectURL(old);
            if (lastCursor) snapDotTo(lastCursor);   // 红点吸附到远端真实鼠标位置(与空心圈重合)
        })
        .catch(err => log('截图失败: ' + err.message, 'err'))
        .finally(() => { busy = false; });
}

// 把红点/待点击坐标吸附到远端屏幕光标实际位置(桌面像素坐标字符串 "x,y")
function snapDotTo(cursor) {
    if (!shot.naturalWidth) return;
    const parts = cursor.split(',');
    const cx = parseInt(parts[0], 10), cy = parseInt(parts[1], 10);
    if (!(cx >= 0) || !(cy >= 0)) return;
    lastSx = cx * shot.clientWidth / shot.naturalWidth;
    lastSy = cy * shot.clientHeight / shot.naturalHeight;
    lastX = cx; lastY = cy;
    dot.style.left = lastSx + 'px';
    dot.style.top = lastSy + 'px';
    dot.style.display = 'block';
    coordEl.textContent = '坐标: (' + cx + ', ' + cy + ')';
}

function scheduleRefresh() { clearTimeout(refreshTimer); refreshTimer = setTimeout(refresh, 400); }

shot.addEventListener('load', () => { infoEl.textContent = shot.naturalWidth + 'x' + shot.naturalHeight; sizeDot(); });

// 红点大小铆定图片显示比例:与截图内"红色空心圈"(截图坐标系中直径 14px)始终保持同尺寸
function sizeDot() {
    if (!shot.naturalWidth) return;
    const ratio = shot.clientWidth / shot.naturalWidth;
    const size = 14 * ratio;
    dot.style.width = size + 'px';
    dot.style.height = size + 'px';
    dot.style.borderWidth = Math.max(0.5, ratio) + 'px';
}
if (window.ResizeObserver) new ResizeObserver(sizeDot).observe(shot);
window.addEventListener('resize', () => requestAnimationFrame(sizeDot));

shot.addEventListener('click', e => {
    const rect = shot.getBoundingClientRect();
    if (!shot.naturalWidth) return;
    const px = e.clientX - rect.left, py = e.clientY - rect.top;
    lastSx = px; lastSy = py;
    lastX = Math.round(px / rect.width * shot.naturalWidth);
    lastY = Math.round(py / rect.height * shot.naturalHeight);
    dot.style.left = px + 'px';
    dot.style.top = py + 'px';
    dot.style.display = 'block';
    coordEl.textContent = '坐标: (' + lastX + ', ' + lastY + ')';
    log('选中坐标 (' + lastX + ', ' + lastY + ')  桌面 ' + shot.naturalWidth + 'x' + shot.naturalHeight, 'act');
});

function sendClick(button) {
    if (lastX === null) { log('请先点击上方截图选择坐标', 'err'); return; }
    rpc("rdp.click(p," + lastX + "," + lastY + ",'" + button + "')").then(scheduleRefresh).catch(() => {});
}
document.getElementById('btn-right').onclick = () => sendClick('right');
document.getElementById('btn-refresh').onclick = refresh;

// ---------- 选择模式:按住 left_click 拖动 → 半透明红条预览,松手才发送拖动请求 ----------
const btnLeft = document.getElementById('btn-left');
let dragState = null;                        // {id, sx, sy, moved, ex, ey}
let suppressLeftClick = false;               // 拖动后吞掉紧随的 click,避免重复发送
btnLeft.addEventListener('pointerdown', e => {
    if (lastX === null) return;              // 未选坐标时保持普通点击(由 click 提示)
    dragState = { id: e.pointerId, sx: e.clientX, sy: e.clientY, moved: false, ex: null, ey: null };
    try { btnLeft.setPointerCapture(e.pointerId); } catch (err) {}
    btnLeft.classList.add('dragging');
});
btnLeft.addEventListener('pointermove', e => {
    if (!dragState || e.pointerId !== dragState.id) return;
    const dx = e.clientX - dragState.sx, dy = e.clientY - dragState.sy;
    if (!dragState.moved && Math.abs(dx) < 6 && Math.abs(dy) < 6) return;
    if (!dragState.moved) { dragState.moved = true; log('拖动选择中…松手发送', 'act'); }
    // 终点 = 红点锚点 + 指针位移(显示像素),并夹在截图范围内
    const w = shot.clientWidth, h = shot.clientHeight;
    const ex = Math.max(0, Math.min(lastSx + dx, w));
    const ey = Math.max(0, Math.min(lastSy + dy, h));
    dragState.ex = ex; dragState.ey = ey;
    dragrect.style.left = Math.min(lastSx, ex) + 'px';
    dragrect.style.top = Math.min(lastSy, ey) + 'px';
    dragrect.style.width = Math.abs(ex - lastSx) + 'px';
    dragrect.style.height = Math.abs(ey - lastSy) + 'px';
    dragrect.style.display = 'block';
});
function finishLeftDrag(send) {
    const st = dragState; dragState = null;
    btnLeft.classList.remove('dragging');
    dragrect.style.display = 'none';
    if (!st) return;
    if (send && st.moved && st.ex !== null) {
        const ex = Math.round(st.ex / shot.clientWidth * shot.naturalWidth);
        const ey = Math.round(st.ey / shot.clientHeight * shot.naturalHeight);
        rpc("rdp.drag(p," + lastX + "," + lastY + "," + ex + "," + ey + ")").then(scheduleRefresh).catch(() => {});
    }
}
btnLeft.addEventListener('pointerup', e => {
    if (!dragState || e.pointerId !== dragState.id) return;
    const wasDrag = dragState.moved;
    if (wasDrag) { suppressLeftClick = true; setTimeout(() => { suppressLeftClick = false; }, 400); }
    finishLeftDrag(true);
});
btnLeft.addEventListener('pointercancel', () => finishLeftDrag(false));
btnLeft.addEventListener('click', () => {
    if (suppressLeftClick) { suppressLeftClick = false; return; }
    sendClick('left');
});

// 屏蔽按钮/按键上的长按复制粘贴与右键菜单(移动端)
document.addEventListener('contextmenu', e => {
    if (e.target.closest('button')) e.preventDefault();
});
document.addEventListener('selectstart', e => {
    if (e.target.closest('button')) e.preventDefault();
});

// ---------- 全键盘 ----------
const MODS = new Set(['ctrl', 'alt', 'shift', 'win']);
const ROWS = [
 [['Esc','esc'],['F1','f1'],['F2','f2'],['F3','f3'],['F4','f4'],['F5','f5'],['F6','f6'],['F7','f7'],['F8','f8'],['F9','f9'],['F10','f10'],['F11','f11'],['F12','f12'],['PrtSc','prtsc'],['ScrLk','scrolllock'],['Pause','pause']],
 [['`','backtick'],['1','1'],['2','2'],['3','3'],['4','4'],['5','5'],['6','6'],['7','7'],['8','8'],['9','9'],['0','0'],['-','minus'],['=','equal'],['Backspace','backspace',2],['Ins','insert'],['Home','home'],['PgUp','pageup']],
 [['Tab','tab',1.5],['Q','q'],['W','w'],['E','e'],['R','r'],['T','t'],['Y','y'],['U','u'],['I','i'],['O','o'],['P','p'],['[','lbracket'],[']','rbracket'],['\\','backslash'],['Del','delete'],['End','end'],['PgDn','pagedown']],
 [['Caps','capslock',1.8],['A','a'],['S','s'],['D','d'],['F','f'],['G','g'],['H','h'],['J','j'],['K','k'],['L','l'],[';','semicolon'],["'",'quote'],['Enter','enter',2.2]],
 [['Shift','shift',2.3],['Z','z'],['X','x'],['C','c'],['V','v'],['B','b'],['N','n'],['M','m'],[',','comma'],['.','period'],['/','slash'],['Shift','shift',2.3],['↑','up']],
 [['Ctrl','ctrl',1.4],['Win','win',1.2],['Alt','alt',1.2],['Space','space',6],['Alt','alt',1.2],['Win','win',1.2],['Menu','menu',1.2],['Ctrl','ctrl',1.4],['←','left'],['↓','down'],['→','right']]
];
const held = new Set();
const kb = document.getElementById('keyboard');
ROWS.forEach(row => {
    const rowEl = document.createElement('div');
    rowEl.className = 'krow';
    row.forEach(item => {
        const btn = document.createElement('button');
        btn.className = 'key' + (MODS.has(item[1]) ? ' mod' : '');
        if (item[2]) btn.style.flexGrow = item[2];
        btn.textContent = item[0];
        btn.onclick = () => onKey(item[1], btn);
        rowEl.appendChild(btn);
    });
    kb.appendChild(rowEl);
});

function onKey(code, btn) {
    if (MODS.has(code)) {                       // 修饰键:点击进入/退出组合状态
        if (held.has(code)) { held.delete(code); btn.classList.remove('held'); }
        else { held.add(code); btn.classList.add('held'); }
        log(held.size ? '组合修饰键: ' + [...held].join('+') + '+ …' : '修饰键已清空', 'act');
        return;
    }
    let combo = code;
    if (held.size) {
        combo = [...held].join('+') + '+' + code;
        held.clear();
        document.querySelectorAll('.key.mod.held').forEach(el => el.classList.remove('held'));
    }
    rpc("rdp.key(p,'" + combo + "')").then(scheduleRefresh).catch(() => {});
}

setInterval(() => { if (autoEl.checked) refresh(); }, 2000);
log('页面就绪,加载截图中…');
refresh();
</script>
</body>
</html>'''
