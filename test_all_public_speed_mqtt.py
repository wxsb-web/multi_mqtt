import time
import uuid
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from paho.mqtt import client as mqtt_client
from paho.mqtt.enums import CallbackAPIVersion

# 全球公开免密 MQTT Broker 汇总列表
BROKER_LIST = [
    ("broker-cn.emqx.io", 1883, "EMQX (中国)"),
    ("test.mosquitto.org", 1883, "Mosquitto 官方"),
    ("mqtt.loralab.org", 1883, "LoRaLab"),
    ("broker.mqtt.cool", 1883, "MQTT.Cool"),
    ("mqtt.tyckr.io", 1883, "Tyckr"),
    ("public-mqtt-broker.bevywise.com", 1883, "Bevywise"),
    ("broker.hivemq.com", 1883, "HiveMQ 官方"),
    ("broker.emqx.io", 1883, "EMQX (国际)"),
    ("broker.mqtt-dashboard.com", 1883, "HiveMQ Dashboard"),
    ("mqtt.iotbhai.io", 1883, "IoTbhai"),
    ("broker.codenow.cn", 1883, "CodeNow 国内公共MQTT"),
    # 新增，注意端口18831
    # ("mq.tongxinmao.com", 1883, "同心猫 MQTT 18831 也无法连接"),
    ("mqtt.touchsocket.net", 1883,'b 视频'),  #好像又可以了 国内最快  这个不能正确转发消息，显示连接成功但是 没有反应
]
#deepseek 2026年9月17日 全部不可用
BROKER_LIST = [
    ("mqtt.eclipseprojects.io", 1883, "Eclipse IoT 官方，部署在 Azure，支持 MQTT 3.1.1/5.0"),
    ("public.mqtthq.com", 1883, "MQTTHQ 公共服务器，支持 WebSocket"),
    ("broker.mqttx.io", 1883, "MQTTX 团队提供，支持 MQTT 3.1.1"),
    ("public.cloud.shiftr.io", 1883, "Shiftr.io 提供，需注册免费账号获取凭证"),
    ("io.adafruit.com", 1883, "Adafruit IO 平台，需注册免费账号并获取 API Key"),
    ("mqtt.myqtthub.com", 1883, "MyQttHub 免费公共服务器，也提供免费私有实例"),
    ("flespi.io", 1883, "Flespi 免费云 MQTT Broker，提供私有命名空间"),
    ("mqtt.ably.io", 1883, "Ably 提供的 MQTT 适配器服务，全球分布式"),
    ("public.mqtt.pro", 1883, "MQTT.pro 公共测试服务器，凭证定期轮换"),
    ("mqtt.flespi.io", 1883, "Flespi MQTT Broker，支持 MQTT 5.0 与 REST API"),
    ("node02.myqtthub.com", 1883, "MyQttHub 公共节点，支持 MQTT over WebSocket"),
    ("cloudmqtt.com", 1883, "CloudMQTT 免费套餐，最多支持 10 个设备连接"),
    ("aceautomation.ddns.net", 1883, "ACE Automation 测试 Broker，需联系获取密码"),
    ("101b7a0.online-server.cloud", 1883, "ACE Automation 备用地址，可用性 99.99%"),
    ("knotfree.net", 1883, "Knotfree.net 公共MQTT服务，支持MQTT 3.1/5.0，无需注册，通过Token认证"),
    ("freemqtt.com", 1883, "Zunoy FreeMQTT 免费公共MQTT代理，无需注册，支持TCP/WebSocket/TLS"),
    ("sas.theakiro.com", 1883, "Akiro MQTT Broker 免费SaaS实例，支持高并发连接"),
    ("mqtt.presov.sk", 1883, "斯洛伐克Prešov市公共测试服务器，用户名/密码均为 public"),
    ("mqtt.fluux.io", 1883, "Fluux公共MQTT服务器，支持TLS"),
    ("mqtt.openmarine.net", 1883, "OpenMarine公共服务器（Beta阶段，无需认证）"),
    ("iot.xpstem.com", 1883, "XPSTEM 公共MQTT服务器，无需认证，适合学习测试"),
    ("broker.xmqtt.net", 1883, "XMQTT 公共服务器，在多个开源项目中被引用"),
    ("demo.tbmq.io", 1883, "ThingsBoard TBMQ 提供的免费公共Broker，用户名 demo，密码为空"),
    ("zunoy.com", 1883, "Zunoy FreeMQTT 免费公共Broker，无需注册，支持TCP/WebSocket/TLS"),
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
    for n,r in enumerate(results):
        conn_str = f"{r['conn_ms']} ms" if r['conn_ms'] is not None else "N/A"
        rtt_str = f"{r['rtt_ms']} ms" if r['rtt_ms'] is not None else "N/A"
        status_symbol = "✅ OK" if r["status"] == "OK" else "❌ FAIL"
        print(f"{n} {r['name']:<20} | {r['host']:<32} | {status_symbol:<6} | {conn_str:<10} | {rtt_str:<12} | {r['error']}")

if __name__ == "__main__":
    run_benchmark()