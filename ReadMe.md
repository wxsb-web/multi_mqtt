降低 CPU 与耗电 (Low-Resource & Power Efficiency):

去磁盘 IO：丢弃了频繁写 SQLite 的操作，换用内存 TTLCache 实现 O(1) 时间复杂度的消息查重，显著降低 CPU 负载与磁盘读写耗电。

事件驱动唤醒：客户端使用 threading.Event() 实现零 CPU 占用率的等待机制，只要有一个 Broker 返回消息就立刻唤醒主线程。

多路抗丢包与首胜机制 (Racing Protocol):

客户端发送时：一条请求并发投递给 5 个不同的 MQTT Broker。

服务端收到后：仅处理最先到达的那一条，后续到达的相同 msg_id 被 TTLCache 直接抛弃。

服务端响应时：同样广播至所有 Broker；客户端仅接收最先到达的 Response 并解除阻塞。





#TODO
加入非对称加密与解密支持。