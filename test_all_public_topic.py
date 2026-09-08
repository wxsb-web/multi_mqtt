import os
import sys
import time
import sqlite3
import threading
import logging
import collections
from multi_mqtt import MultiMQTTManager, BROKER_LIST

# 开启 Windows 10/11 的终端 ANSI 转义码支持
if os.name == 'nt':
    os.system('color')

DB_FILE = "mqtt_topics_dump.db"
FLUSH_INTERVAL = 5.0          # 每 5 秒将内存数据批量写入磁盘
DISPLAY_INTERVAL = 1.0        # 每 1 秒刷新终端 UI
MAX_PAYLOAD_SAVE = 1024       # 只保存消息体前 1024 字节，防止磁盘爆炸
MAX_LOG_LINES = 15            # 底部保留的滚动日志行数

# ==========================================
# 1. 终端分屏 UI 日志拦截器
# ==========================================
log_queue = collections.deque(maxlen=MAX_LOG_LINES)

class UILogHandler(logging.Handler):
    """将日志捕获到队列，由 UI 线程统一渲染，避免打乱屏幕布局"""
    def emit(self, record):
        log_queue.append(self.format(record))

logger = logging.getLogger("MQTTSniffer")
logger.propagate = False
logger.setLevel(logging.INFO)
# 清除原有 handler，换成我们的 UI 专用 Handler
if logger.hasHandlers():
    logger.handlers.clear()
ui_handler = UILogHandler()
ui_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
logger.addHandler(ui_handler)

# ==========================================
# 2. 一小时滚动时间窗口统计器 (极低内存)
# ==========================================
class HourTracker:
    def __init__(self):
        # 记录 1小时(60分钟) 内每一分钟的数据: { 分钟戳: {'msgs': int, 'topics': set()} }
        self.buckets = {}
        self.lock = threading.Lock()

    def add(self, topic: str):
        now_minute = int(time.time()) // 60
        with self.lock:
            if now_minute not in self.buckets:
                self.buckets[now_minute] = {'msgs': 0, 'topics': set()}
            self.buckets[now_minute]['msgs'] += 1
            self.buckets[now_minute]['topics'].add(topic)
            
            # 清理 60 分钟之前的老旧数据 (滑动窗口移出)
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
# 3. 核心统计与数据库引擎
# ==========================================
class SnifferEngine:
    def __init__(self):
        self.buffer = {}  # 内存缓冲: {(host, topic): {"count": int, "time": float, "payload": bytes}}
        
        # UI 显示用的状态字典
        self.db_totals = {}     # host -> {"msgs": 0, "topics": 0} (来自数据库的历史总计)
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
            # 开启 WAL 模式极大提升并发写入性能
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
            # 建立索引以加速启动时的 GROUP BY 统计查询
            conn.execute("CREATE INDEX IF NOT EXISTS idx_host ON topic_stats(host);")
            conn.commit()

    def _load_historical_stats(self):
        logger.info("正在扫描历史数据库，计算总 Topic 和消息数目...")
        with sqlite3.connect(DB_FILE) as conn:
            cur = conn.cursor()
            cur.execute("SELECT host, COUNT(topic), SUM(msg_count) FROM topic_stats GROUP BY host")
            for host, topic_count, msg_count in cur.fetchall():
                self.db_totals[host] = {"topics": topic_count, "msgs": msg_count}
        logger.info("历史数据库统计完成！")

    def push(self, host: str, topic: str, payload: bytes):
        """高速处理接收到的消息"""
        if len(payload) > MAX_PAYLOAD_SAVE:
            payload = payload[:MAX_PAYLOAD_SAVE]

        now = time.time()
        
        # 1. 更新 1 小时滚动统计
        if host not in self.hour_trackers:
            self.hour_trackers[host] = HourTracker()
        self.hour_trackers[host].add(topic)

        # 2. 更新内存数据
        with self.lock:
            # 加入刷盘 Buffer
            key = (host, topic)
            if key not in self.buffer:
                self.buffer[key] = {"count": 1, "time": now, "payload": payload}
            else:
                self.buffer[key]["count"] += 1
                self.buffer[key]["time"] = now
                self.buffer[key]["payload"] = payload
            
            # 更新最新消息展示 (转为安全字符)
            safe_payload = payload.decode('utf-8', errors='replace').replace('\n', ' ')
            self.latest_msg[host] = {
                "topic": topic, 
                "time": now, 
                "payload": safe_payload[:80] + ("..." if len(safe_payload) > 80 else "")
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
                    
                    # 刷盘后重新查询准确的 DB 全局总数
                    cur = conn.cursor()
                    cur.execute("SELECT host, COUNT(topic), SUM(msg_count) FROM topic_stats GROUP BY host")
                    fresh_totals = {row[0]: {"topics": row[1], "msgs": row[2]} for row in cur.fetchall()}
                    with self.lock:
                        self.db_totals = fresh_totals
                        
            except Exception as e:
                logger.error(f"写入数据库失败: {e}")

    def _display_loop(self):
        """纯净无闪烁刷新算法"""
        print("\033[2J")  # 仅在启动时清空一次全屏
        
        while self.running:
            time.sleep(DISPLAY_INTERVAL)
            
            lines = []
            # 1. 移动光标到屏幕最左上角 (0行0列)，不清除屏幕内容，直接覆盖写入
            lines.append("\033[H") 
            lines.append("=" * 115)
            lines.append(f" 📡 MQTT 深度监听雷达 (自动去重持久化) - {time.strftime('%Y-%m-%d %H:%M:%S')}")
            lines.append("=" * 115)
            
            total_msgs_all = 0
            total_topics_all = 0

            with self.lock:
                hosts = sorted(set(self.db_totals.keys()) | set(self.latest_msg.keys()))
                
                if not hosts:
                    lines.append(" 正在等待连接与数据...")
                else:
                    for host in hosts:
                        # 历史数据
                        hist = self.db_totals.get(host, {"msgs": 0, "topics": 0})
                        total_msgs_all += hist["msgs"]
                        total_topics_all += hist["topics"]
                        
                        # 1小时数据
                        h1_msgs, h1_topics = 0, 0
                        if host in self.hour_trackers:
                            h1_msgs, h1_topics = self.hour_trackers[host].get_stats()
                        
                        # 最新消息
                        latest = self.latest_msg.get(host, {"topic": "无", "time": 0, "payload": ""})
                        t_str = time.strftime('%H:%M:%S', time.localtime(latest["time"])) if latest["time"] else "--:--:--"
                        
                        # 构建排版
                        lines.append(f" 🌐 \033[1;36m{host}\033[0m")
                        lines.append(f"  ├ 历史累计: {hist['msgs']:>8} Msg / {hist['topics']:>6} Topic  |  近1小时: {h1_msgs:>6} Msg / {h1_topics:>5} Topic")
                        lines.append(f"  └ 最新接收: [{t_str}] {latest['topic']} => {latest['payload']}")
                        lines.append("") # 空行分隔
            
            lines.append("-" * 115)
            lines.append(f" 📈 全局汇总 => 历史累计捕获消息: \033[1;33m{total_msgs_all}\033[0m 条 | 累计发现独立 Topic: \033[1;33m{total_topics_all}\033[0m 个")
            lines.append("=" * 115)
            
            # 2. 渲染底部滚动日志
            lines.append(" [系统运行日志]")
            # 为了防止旧文本残留，给每行日志补齐空格
            for log in list(log_queue): 
                lines.append(log.ljust(115)[:115]) 
            
            # 3. 清除光标以下所有多余的残影
            lines.append("\033[J") 
            
            # 一次性将所有字符串推向显存，达到彻底 0 闪烁
            sys.stdout.write('\n'.join(lines) + '\n')
            sys.stdout.flush()

    def shutdown(self):
        self.running = False
        self.flush_thread.join(timeout=2)
        self.display_thread.join(timeout=2)

# ==========================================
# 4. 原始抓包管理器 (无 JSON 解码要求)
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

# ==========================================
# 主程序入口
# ==========================================
def main():
    engine = SnifferEngine()

    def on_raw_message(topic, payload_bytes, host):
        # 丢弃系统层面的冗余主题(例如 hivemq 自己的系统信息)
        if topic.startswith("$SYS/"):
            return
        engine.push(host, topic, payload_bytes)

    manager = RawSnifferManager(brokers=BROKER_LIST, log_messages=False, enable_crypto=False)
    manager.set_on_message(on_raw_message)

    logger.info("正在启动全局监听网络及重连机制...")
    manager.start()
    manager.subscribe("#")

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("收到退出信号 (Ctrl+C)，正在安全关闭数据库和网络...")
        engine.shutdown()
        manager.stop()
        print("\n\n 程序已安全退出。")

if __name__ == "__main__":
    main()