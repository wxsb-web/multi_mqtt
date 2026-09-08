import os
import sys
import time
import sqlite3
import threading
import logging
import collections
from multi_mqtt import MultiMQTTManager, BROKER_LIST

from rich.live import Live
from rich.table import Table
from rich.console import Console
from rich.panel import Panel
from rich.layout import Layout
from rich.text import Text

DB_FILE = "mqtt_topics_dump.db"
FLUSH_INTERVAL = 66          # 每 66 秒批量写入磁盘
MAX_PAYLOAD_SAVE = 1024       # 保存消息体前 1024 字节
MAX_LOG_LINES = 28             # 底部固定留给日志的行数

# ==========================================
# 1. UI 日志拦截器 (使用 Rich 格式化)
# ==========================================
log_queue = collections.deque(maxlen=MAX_LOG_LINES)
log_lock = threading.Lock()          # 保护 log_queue 的并发访问

class UILogHandler(logging.Handler):
    """将日志捕获到队列，由 Live UI 统一渲染"""
    def emit(self, record):
        msg = self.format(record)
        with log_lock:
            log_queue.append(msg)

# 【核心修复】：接管 Root Logger，拦截所有模块（包括 multi_mqtt）的日志
root_logger = logging.getLogger()
root_logger.setLevel(logging.INFO)

# 清理可能已经被其他模块设置的默认控制台 Handler，防止重复打印
for handler in root_logger.handlers[:]:
    root_logger.removeHandler(handler)

ui_handler = UILogHandler()
ui_handler.setFormatter(logging.Formatter('[%(asctime)s] %(levelname)s: %(message)s', datefmt='%H:%M:%S'))
root_logger.addHandler(ui_handler)

# 当前脚本专用的 logger
logger = logging.getLogger("MQTTSniffer")


# ==========================================
# 2. 一小时滑动时间窗口统计器
# ==========================================
class HourTracker:
    def __init__(self):
        self.buckets = {}
        self.lock = threading.Lock()

    def add(self, topic: str):
        now_minute = int(time.time()) // 60
        with self.lock:
            if now_minute not in self.buckets:
                self.buckets[now_minute] = {'msgs': 0, 'topics': set()}
            self.buckets[now_minute]['msgs'] += 1
            self.buckets[now_minute]['topics'].add(topic)
            
            expired = [m for m in self.buckets.keys() if m < now_minute - 60]
            for m in expired:
                del self.buckets[m]

    def get_stats(self):
        with self.lock:
            total_msgs = sum(b['msgs'] for b in self.buckets.values())
            all_topics = set()
            for b in self.buckets.values():
                all_topics.update(b['topics'])
            return total_msgs, len(all_topics)


# ==========================================
# 3. 核心 Engine
# ==========================================
class SnifferEngine:
    def __init__(self):
        self.buffer = {}
        self.db_totals = {}     # host -> {"msgs": 0, "topics": 0}
        self.hour_trackers = {} # host -> HourTracker
        self.latest_msg = {}    # host -> {"topic": str, "payload": str, "time": float}
        
        self.lock = threading.Lock()
        self.running = True

        self._init_db()
        self._load_historical_stats()

        # 启动后台线程
        self.flush_thread = threading.Thread(target=self._flush_loop, daemon=True)
        self.display_thread = threading.Thread(target=self._display_loop, daemon=True)
        self.flush_thread.start()
        self.display_thread.start()

    def _init_db(self):
        with sqlite3.connect(DB_FILE) as conn:
            conn.execute("PRAGMA journal_mode=WAL;")
            conn.execute("""
                CREATE TABLE IF NOT EXISTS topic_stats (
                    host TEXT,
                    topic TEXT,
                    msg_count INTEGER DEFAULT 0,
                    last_time REAL,
                    last_payload BLOB,
                    PRIMARY KEY (host, topic)
                )
            """)
            conn.execute("CREATE INDEX IF NOT EXISTS idx_host ON topic_stats(host);")
            conn.commit()

    def _load_historical_stats(self):
        logger.info("正在扫描 SQLite 数据库历史数据...")
        with sqlite3.connect(DB_FILE) as conn:
            cur = conn.cursor()
            cur.execute("SELECT host, COUNT(topic), SUM(msg_count) FROM topic_stats GROUP BY host")
            for host, topic_count, msg_count in cur.fetchall():
                self.db_totals[host] = {"topics": topic_count, "msgs": msg_count}
        logger.info("数据库历史统计加载完毕！")

    def push(self, host: str, topic: str, payload: bytes):
        if len(payload) > MAX_PAYLOAD_SAVE:
            payload = payload[:MAX_PAYLOAD_SAVE]

        now = time.time()
        
        # 1. 更新 1 小时统计
        if host not in self.hour_trackers:
            self.hour_trackers[host] = HourTracker()
        self.hour_trackers[host].add(topic)

        # 2. 清洗 Payload 文本，防止不可见乱码打乱 TUI 排版
        safe_str = "".join([c if c.isprintable() else "." for c in payload.decode('utf-8', errors='replace')])

        with self.lock:
            key = (host, topic)
            if key not in self.buffer:
                self.buffer[key] = {"count": 1, "time": now, "payload": payload}
            else:
                self.buffer[key]["count"] += 1
                self.buffer[key]["time"] = now
                self.buffer[key]["payload"] = payload
            
            self.latest_msg[host] = {
                "topic": topic, 
                "time": now, 
                "payload": safe_str
            }

    def _flush_loop(self):
        while self.running:
            time.sleep(FLUSH_INTERVAL)
            with self.lock:
                if not self.buffer:
                    continue
                batch = self.buffer
                self.buffer = {}

            records = [(h, t, d["count"], d["time"], d["payload"]) for (h, t), d in batch.items()]

            try:
                with sqlite3.connect(DB_FILE) as conn:
                    conn.executemany("""
                        INSERT INTO topic_stats (host, topic, msg_count, last_time, last_payload)
                        VALUES (?, ?, ?, ?, ?)
                        ON CONFLICT(host, topic) DO UPDATE SET
                            msg_count = msg_count + excluded.msg_count,
                            last_time = excluded.last_time,
                            last_payload = excluded.last_payload
                    """, records)
                    conn.commit()
                    
                    cur = conn.cursor()
                    cur.execute("SELECT host, COUNT(topic), SUM(msg_count) FROM topic_stats GROUP BY host")
                    fresh_totals = {row[0]: {"topics": row[1], "msgs": row[2]} for row in cur.fetchall()}
                    with self.lock:
                        self.db_totals = fresh_totals
            except Exception as e:
                logger.error(f"写入数据库失败: {e}")

    def generate_layout() -> Layout:
        """构建 Rich 分屏界面"""
        layout = Layout()
        layout.split(
            Layout(name="header", size=3),
            Layout(name="body", ratio=1),
            Layout(name="logs", size=MAX_LOG_LINES + 2)
        )
        return layout

    def _display_loop(self):
        console = Console()
        
        # 使用 Rich Live 机制接管屏幕，自适应分辨率，防闪烁、防排版错乱
        with Live(refresh_per_second=2, screen=True, console=console) as live:
            while self.running:
                layout = Layout()
                layout.split_column(
                    Layout(name="header", size=3),
                    Layout(name="main", ratio=1),
                    Layout(name="footer", size=MAX_LOG_LINES + 2)
                )

                # 1. 顶部 Header
                now_str = time.strftime('%Y-%m-%d %H:%M:%S')
                layout["header"].update(
                    Panel(f"[bold cyan]📡 MQTT 深度监听雷达 (自动去重持久化)[/bold cyan] [dim]| 系统时间: {now_str}[/dim]", style="blue")
                )

                # 2. 中间 Table 监控看板
                table = Table(expand=True, border_style="dim", header_style="bold magenta")
                table.add_column("Broker 节点", style="cyan", width=25, no_wrap=True)
                table.add_column("历史总 Msg / Topic", justify="right", width=22)
                table.add_column("近1小时 Msg / Topic", justify="right", width=22)
                table.add_column("最新消息接收 (时间 / Topic => Payload)", style="yellow", ratio=1, no_wrap=True)

                total_msgs_all = 0
                total_topics_all = 0

                with self.lock:
                    hosts = sorted(set(self.db_totals.keys()) | set(self.latest_msg.keys()))
                    
                    for host in hosts:
                        hist = self.db_totals.get(host, {"msgs": 0, "topics": 0})
                        total_msgs_all += hist["msgs"]
                        total_topics_all += hist["topics"]
                        
                        h1_msgs, h1_topics = 0, 0
                        if host in self.hour_trackers:
                            h1_msgs, h1_topics = self.hour_trackers[host].get_stats()
                        
                        latest = self.latest_msg.get(host, {"topic": "无", "time": 0, "payload": ""})
                        t_str = time.strftime('%H:%M:%S', time.localtime(latest["time"])) if latest["time"] else "--:--:--"
                        
                        hist_str = f"[bold green]{hist['msgs']:,}[/bold green] / {hist['topics']:,}"
                        h1_str = f"[bold green]{h1_msgs:,}[/bold green] / {h1_topics:,}"
                        latest_str = f"[{t_str}] [bold white]{latest['topic']}[/bold white] => {latest['payload']}"

                        table.add_row(host, hist_str, h1_str, latest_str)

                summary_text = f" [bold yellow]全局汇总[/bold yellow] => 历史总消息数: [bold green]{total_msgs_all:,}[/bold green] 条 | 捕获独立 Topic: [bold green]{total_topics_all:,}[/bold green] 个"
                
                # 将表格与汇总打包成主面板
                main_group = Panel(
                    table, 
                    title=summary_text, 
                    title_align="left", 
                    border_style="green"
                )
                layout["main"].update(main_group)

                # 3. 底部日志框（使用锁保护读取，避免并发修改 deque 引发异常）
                with log_lock:
                    logs_snapshot = list(log_queue)
                logs_text = "\n".join(logs_snapshot) if logs_snapshot else "暂无系统日志..."
                layout["footer"].update(
                    Panel(logs_text, title="[bold]系统日志[/bold]", border_style="grey50", height=MAX_LOG_LINES)
                )

                # 渲染整屏
                live.update(layout)
                time.sleep(0.5)

    def shutdown(self):
        self.running = False
        self.flush_thread.join(timeout=2)
        self.display_thread.join(timeout=2)


# ==========================================
# 4. MQTT 监听适配器
# ==========================================
class RawSnifferManager(MultiMQTTManager):
    def _make_on_message(self, host):
        def on_message(client, userdata, msg):
            try:
                if self.message_callback:
                    self.message_callback(msg.topic, msg.payload, host)
            except Exception:
                pass
        return on_message


def main():
    engine = SnifferEngine()

    def on_raw_message(topic, payload_bytes, host):
        if topic.startswith("$SYS/"):
            return
        engine.push(host, topic, payload_bytes)

    manager = RawSnifferManager(brokers=BROKER_LIST, log_messages=False, enable_crypto=False)
    manager.set_on_message(on_raw_message)

    logger.info("启动 MQTT 节点连接管理程序...")
    manager.start()
    manager.subscribe("#")

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("接收到退出信号 (Ctrl+C)，退出中...")
        engine.shutdown()
        manager.stop()

if __name__ == "__main__":
    main()