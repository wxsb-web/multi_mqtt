import time
import sqlite3
import threading
import logging
from collections import defaultdict
from multi_mqtt import MultiMQTTManager, BROKER_LIST

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("MQTTSniffer")

DB_FILE = "mqtt_topics_dump.db"
FLUSH_INTERVAL = 5.0          # 每 5 秒将内存数据批量写入磁盘
DISPLAY_INTERVAL = 1.0        # 每 1 秒刷新命令行显示
MAX_PAYLOAD_SAVE = 1024       # 只保存消息体前 1024 字节，防止磁盘爆炸

class DBWriter:
    """
    带缓冲的 SQLite 异步写入器，同时维护内存中的实时统计信息（按服务器分组）。
    """
    def __init__(self):
        # 内存缓冲：{(host, topic): {"count": int, "time": float, "payload": bytes}}
        self.buffer = {}
        # 实时统计：{host: {"total": int, "latest_topic": str, "latest_time": float, "latest_payload": str}}
        self.host_stats = {}
        self.lock = threading.Lock()
        self.running = True

        # 初始化数据库表
        self._init_db()

        # 启动后台刷盘线程
        self.flush_thread = threading.Thread(target=self._flush_loop, daemon=True)
        self.flush_thread.start()

        # 启动后台显示线程
        self.display_thread = threading.Thread(target=self._display_loop, daemon=True)
        self.display_thread.start()

    def _init_db(self):
        """创建数据库表（含 host 列）"""
        with sqlite3.connect(DB_FILE) as conn:
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
            conn.commit()

    def push(self, host: str, topic: str, payload: bytes):
        """主线程快速推入内存缓冲，并更新实时统计"""
        # 截断超大 Payload
        if len(payload) > MAX_PAYLOAD_SAVE:
            payload = payload[:MAX_PAYLOAD_SAVE]

        now = time.time()
        with self.lock:
            # 更新缓冲（用于批量写入数据库）
            key = (host, topic)
            if key not in self.buffer:
                self.buffer[key] = {"count": 1, "time": now, "payload": payload}
            else:
                self.buffer[key]["count"] += 1
                self.buffer[key]["time"] = now
                self.buffer[key]["payload"] = payload

            # 更新实时统计（用于终端显示）
            if host not in self.host_stats:
                self.host_stats[host] = {
                    "total": 0,
                    "latest_topic": "",
                    "latest_time": now,
                    "latest_payload": ""
                }
            stats = self.host_stats[host]
            stats["total"] += 1
            stats["latest_topic"] = topic
            stats["latest_time"] = now
            # 将 payload 转为字符串用于显示（截断前 100 字符，避免刷屏）
            stats["latest_payload"] = payload.decode('utf-8', errors='replace')[:100]

    def _flush_loop(self):
        """后台独立线程：负责数据库批量写入"""
        conn = sqlite3.connect(DB_FILE)
        while self.running:
            time.sleep(FLUSH_INTERVAL)

            # 快速锁定并交接缓冲
            with self.lock:
                if not self.buffer:
                    continue
                batch = self.buffer
                self.buffer = {}

            # 组装批量 SQL 数据
            records = []
            for (host, topic), data in batch.items():
                records.append((host, topic, data["count"], data["time"], data["payload"]))

            # 批量 UPSERT
            try:
                conn.executemany("""
                    INSERT INTO topic_stats (host, topic, msg_count, last_time, last_payload)
                    VALUES (?, ?, ?, ?, ?)
                    ON CONFLICT(host, topic) DO UPDATE SET
                        msg_count = msg_count + excluded.msg_count,
                        last_time = excluded.last_time,
                        last_payload = excluded.last_payload
                """, records)
                conn.commit()
                logger.info(f"💾 成功刷盘 {len(records)} 个 (host, topic) 组合到 SQLite")
            except Exception as e:
                logger.error(f"写入数据库失败: {e}")

    def _display_loop(self):
        """后台线程：实时刷新命令行，显示各服务器消息总数及最近一条消息详情"""
        while self.running:
            time.sleep(DISPLAY_INTERVAL)
            with self.lock:
                # 复制当前统计快照，避免长时间持锁
                host_stats = {host: dict(info) for host, info in self.host_stats.items()}

            # 清屏并打印
            print("\033[2J\033[H", end="")  # ANSI 清屏
            print("=" * 70)
            print(" MQTT 消息接收统计（实时刷新）")
            print("=" * 70)
            if not host_stats:
                print("暂无数据")
            else:
                for host, info in sorted(host_stats.items()):
                    # 显示服务器、消息总数
                    line = f"  {host:<35} : {info['total']} 条消息"
                    # 如果有最近消息，附加详情
                    if info["latest_topic"]:
                        line += f"  | 最新: {info['latest_topic']} 摘要: {info['latest_payload']} 时间: {time.strftime('%H:%M:%S', time.localtime(info['latest_time']))}"
                    print(line)
            print("-" * 70)
            total = sum(info["total"] for info in host_stats.values())
            print(f"  总计：{total} 条消息")
            print("=" * 70)

    def shutdown(self):
        """停止所有后台线程"""
        self.running = False
        self.flush_thread.join(timeout=2)
        self.display_thread.join(timeout=2)

class RawSnifferManager(MultiMQTTManager):
    """继承并魔改：绕过原版的 JSON 解析和解密，直接抓取底层二进制 payload"""
    def _make_on_message(self, host):
        def on_message(client, userdata, msg):
            try:
                if self.message_callback:
                    self.message_callback(msg.topic, msg.payload, host)
            except Exception:
                pass
        return on_message

def main():
    db_writer = DBWriter()

    def on_raw_message(topic, payload_bytes, host):
        # 将服务器信息一并传给 DBWriter
        db_writer.push(host, topic, payload_bytes)

    manager = RawSnifferManager(brokers=BROKER_LIST, log_messages=False, enable_crypto=False)
    manager.set_on_message(on_raw_message)

    logger.info("启动全局监听网络...")
    manager.start()
    manager.subscribe("#")

    try:
        # 主循环保持程序运行，显示和刷盘由后台线程完成
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("收到退出信号，正在关闭...")
        db_writer.shutdown()
        manager.stop()

if __name__ == "__main__":
    main()