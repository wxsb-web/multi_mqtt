降低 CPU 与耗电 (Low-Resource & Power Efficiency):

去磁盘 IO：丢弃了频繁写 SQLite 的操作，换用内存 TTLCache 实现 O(1) 时间复杂度的消息查重，显著降低 CPU 负载与磁盘读写耗电。

事件驱动唤醒：客户端使用 threading.Event() 实现零 CPU 占用率的等待机制，只要有一个 Broker 返回消息就立刻唤醒主线程。

多路抗丢包与首胜机制 (Racing Protocol):

客户端发送时：一条请求并发投递给 5 个不同的 MQTT Broker。

服务端收到后：仅处理最先到达的那一条，后续到达的相同 msg_id 被 TTLCache 直接抛弃。

服务端响应时：同样广播至所有 Broker；客户端仅接收最先到达的 Response 并解除阻塞。




mqtt_server.py 收到 code
调用 PythonExecutor.execute(code)
ast.parse() 解析代码
发现最后一条语句是表达式 3
把前面的语句执行完
对最后表达式执行 eval
eval("3") 返回整数 3
服务端 format_result(3) 转成字符串 "3"
返回：


r=3这不是最后表达式，而是赋值语句：
进入普通 exec
执行后 locals 中出现 r: 3
执行器读取 locals["r"]
返回结果 3
server 格式化后返回 "r": "3"

所以 MQTT 支持两种结果来源


#TODO
加入非对称加密与解密支持。