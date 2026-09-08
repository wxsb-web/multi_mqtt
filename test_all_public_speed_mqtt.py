import time
import uuid
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from paho.mqtt import client as mqtt_client
from paho.mqtt.enums import CallbackAPIVersion

# 全球公开免密 MQTT Broker 汇总列表
BROKER_LIST = [
    # 你的原有的 6 个节点
    ("broker-cn.emqx.io", 1883, "EMQX (中国)"),
    ("test.mosquitto.org", 1883, "Mosquitto 官方"),
    ("mqtt.loralab.org", 1883, "LoRaLab"),
    ("broker.mqtt.cool", 1883, "MQTT.Cool"),
    ("mqtt.tyckr.io", 1883, "Tyckr"),
    ("public-mqtt-broker.bevywise.com", 1883, "Bevywise"),
    
    # 补充补充的公开免密节点
    ("broker.hivemq.com", 1883, "HiveMQ 官方"),
    ("broker.emqx.io", 1883, "EMQX (国际)"),
    ("mqtt.eclipseprojects.io", 1883, "Eclipse 官方"),
    ("public.mqtthq.com", 1883, "MQTT HQ"),
    ("broker.mqtt-dashboard.com", 1883, "HiveMQ Dashboard"),
    ("mqtt.fluux.io", 1883, "Fluux"),
]

def test_single_broker(host, port, name, timeout=3.0):
    client_id = f"bench_{int(time.time())}_{uuid.uuid4().hex[:4]}"
    test_topic = f"sys/bench/{client_id}"
    
    res = {
        "name": name,
        "host": host,
        "port": port,
        "status": "FAIL",
        "conn_ms": None,
        "rtt_ms": None,
        "error": ""
    }
    
    recv_event = threading.Event()
    t_send = [0.0]
    t_recv = [0.0]

    def on_connect(client, userdata, flags, rc, properties=None):
        if rc != 0:
            res["error"] = f"Connect RC={rc}"

    def on_message(client, userdata, msg):
        t_recv[0] = time.perf_counter()
        recv_event.set()

    try:
        client = mqtt_client.Client(CallbackAPIVersion.VERSION2, client_id=client_id, protocol=mqtt_client.MQTTv311)
        client.on_connect = on_connect
        client.on_message = on_message

        # 1. 测量建连时间
        t0 = time.perf_counter()
        client.connect(host, port, keepalive=10)
        client.loop_start()
        
        start_wait = time.time()
        while not client.is_connected():
            if time.time() - start_wait > timeout:
                raise TimeoutError("连接超时")
            time.sleep(0.02)
            
        t1 = time.perf_counter()
        res["conn_ms"] = round((t1 - t0) * 1000, 1)

        # 2. 测量消息往返延时 (RTT)
        client.subscribe(test_topic, qos=0)
        time.sleep(0.1)  # 等待订阅确认完成
        
        t_send[0] = time.perf_counter()
        client.publish(test_topic, "ping", qos=0)

        if recv_event.wait(timeout=timeout):
            res["rtt_ms"] = round((t_recv[0] - t_send[0]) * 1000, 1)
            res["status"] = "OK"
        else:
            res["error"] = "消息响应超时"

    except Exception as e:
        if not res["error"]:
            res["error"] = str(e) or "网络故障"
    finally:
        try:
            client.loop_stop()
            client.disconnect()
        except Exception:
            pass

    return res

def run_benchmark():
    print("🚀 开始并发测试所有公开 MQTT 服务器...\n")
    results = []
    
    # 线程池并发测试所有 Broker
    with ThreadPoolExecutor(max_workers=len(BROKER_LIST)) as executor:
        futures = [
            executor.submit(test_single_broker, host, port, name) 
            for host, port, name in BROKER_LIST
        ]
        for future in as_completed(futures):
            results.append(future.result())

    # 按状态及往返延时排序
    results.sort(key=lambda x: (x["status"] != "OK", x["rtt_ms"] if x["rtt_ms"] is not None else 9999))

    # 输出格式化表格
    print(f"{'服务器名称':<20} | {'域名 (Host)':<32} | {'状态':<6} | {'连接延时':<10} | {'往返延时(RTT)':<12} | {'异常原因'}")
    print("-" * 115)
    for r in results:
        conn_str = f"{r['conn_ms']} ms" if r['conn_ms'] is not None else "N/A"
        rtt_str = f"{r['rtt_ms']} ms" if r['rtt_ms'] is not None else "N/A"
        status_symbol = "✅ OK" if r["status"] == "OK" else "❌ FAIL"
        print(f"{r['name']:<20} | {r['host']:<32} | {status_symbol:<6} | {conn_str:<10} | {rtt_str:<12} | {r['error']}")

if __name__ == "__main__":
    run_benchmark()