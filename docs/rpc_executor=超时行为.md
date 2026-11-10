The user asks: rpc_executor.py 有超时 999s 机制吗？如果前一个 rpc 阻塞，为什么会影响下一步 rpc 执行？详细分析本地代码。

I need to read rpc_executor.py and the app-side server_mqtt / how RPC executes (threading model). Key question: is there a thread pool with limited workers? If one thread blocks forever, all workers get exhausted. Let me read.

Files: c:\test\github\multi_mqtt\rpc_executor.py, server_mqtt.py, app.py (maybe in project?). Let's search.

Now read the executor's execute/_run_bounded in detail, and how server_mqtt calls it (thread pool? one executor shared? per-message thread?).

Read rpc_executor.py lines 600-900.

Now let's understand the key question: why does one blocked RPC block subsequent RPCs even with timeout=999?

The executor has a global RLock. Each execute spawns a new daemon thread `_work` which acquires `self.lock`. So if one execution blocks forever, subsequent executes will wait on the lock for `timeout` (999s!) then return "executor busy: 等待上一个执行释放锁超过 999s". So with default timeout 999 the next RPC waits 999s before failing → practically appears "all RPCs hung".

Also `_stuck` flag is set only when the call itself timed out. In our case, the stuck call blocked in `_recover_client` inside... wait, the caller of `_recover_client` was a remote RPC call. That call itself runs inside `PythonExecutor.execute(...)` (MQTT channel). It blocked forever in `loop_stop()`. That worker thread holds `self.lock` forever. Then every subsequent execute: spawns a thread which waits on `self.lock`; the calling thread waits `done.wait(999)`.

So the answer: yes, there is a 999 s timeout, but:
1. The timeout only bounds the *caller's* wait, not the executing thread — the worker is a daemon thread that can't be interrupted; it keeps holding the RLock.
2. 999 s ≈ 16.6 min, so effectively "forever" from user's perspective; the client RPC timeout was 10s anyway.
3. When the first blocked call exceeded 999s, `_stuck=True` would be set; but for the first 999 s all subsequent requests queue on lock, each also waiting 999s... they don't fail fast, they just hang. Actually each new request spawns a thread that blocks on `self.lock.acquire()`. Multiple threads pile up (thread leak). Each waiting request holds its own MQTT dispatch? Let's check server_mqtt: does it execute in the MQTT on_message thread or a thread pool? Need to check.

Let me check server_mqtt's handle → executor call and whether it's in the network thread or a dispatcher thread pool. Also check the reply publish path. Let's grep.

Also need to check: does the MQTT channel call execute with timeout=999 default? And the HTTP server (server_http) worker threads.

Let me grep server_mqtt for execute( and thread pool.

Key: server_mqtt.handle_message runs in `MQTTMsgDispatch` thread (single, shared), and calls `self.executor.execute(code, format_result=True)` with default timeout=999. So a blocked execution blocks the single dispatch thread for 999 s → ALL MQTT messages (all brokers, all topics) stop being dispatched, including replies from other devices, ping/pong, periodic tasks? Wait — periodic tasks are scheduled in _dispatch_loop too (set_periodic_task). Let me check multi_mqtt._dispatch_loop: it processes queue and periodic tasks in the same thread. If handle_message blocks, the dispatch loop is stuck, so:
- no new messages processed (all brokers' queues buffer, paho's internal queue grows)
- periodic ping/global topic list ticks stop
- and crucially, `_on_publish` acks still work (paho network threads), but the dispatch is stuck.

Also, HTTP RPC? server_http uses executor.execute too. HTTP server: is it ThreadingHTTPServer? Let me check. If HTTP uses the same executor, then HTTP RPC also blocks on the executor lock (up to 999s), unless HTTP creates its own executor. In app.py (device), the MQTT and HTTP may share the same executor instance? Need to check: server_http.Server has `executor = None` class attr; probably set from app.py to the same PythonExecutor. Let me check app.py in xime remote? The local repo might have app.py? Not in list. The summary said remote `/root/build_xime_home/xime/app/src/main/python/.../app.py`. Local repo has no app.py. Hmm.

Let me check how server_http gets executor, and whether ThreadingHTTPServer.

Also important: the timeout=999 default is only for the caller, and the worker thread holds RLock. But there's a subtlety: `_run_bounded` spawns a NEW thread per request. The blocked thread holds `self.lock`. Subsequent requests: each spawns a thread that blocks on lock; the caller waits `done.wait(999)` on its own thread. In MQTT case the caller is the single dispatch thread → it blocks 999s per request, sequentially: request 2 waits 999s, returns busy error, then request 3 waits 999s... so the dispatch thread is stuck forever effectively, each queued request consuming 999s. Log shows "executor busy: 等待上一个执行释放锁超过 999s".

Also note: `_stuck` is only set when a *caller* times out after the job started. In our case, the stuck job never returns, so the first caller (the RPC that called _recover_client) waits 999s then sets `_stuck=True` and returns. After that, subsequent requests fail fast (until done.set() clears it — but the stuck thread never sets done, since it's blocked forever). Wait — `_stuck` clearing happens in `_work`'s finally, which never runs. So `_stuck` stays True forever → subsequent executes return immediately with busy error, not 999s. Hmm, but `done.set()` in finally never runs so `_stuck` remains True. So after the first 999s timeout, requests fail fast with "executor busy".

But before that first 999s elapses, requests block for up to 999s. And actually, the HTTP RPC also: if server_http shares the same executor, HTTP requests block too (each HTTP worker thread waits 999s) — that matches the observation "HTTP RPC 全部超时" from the client side with shorter client timeouts.

Now, what exactly did I observe: "HTTP RPC 全部超时（client 超时）" — because HTTP request thread waits on executor lock.

Also important: MQTT reply publishing happens in handle_message after execute returns; when dispatch thread is blocked, no replies are sent for any request — even if other devices' requests were already executed? No, they can't be executed because executor lock.

Another channel: `rpc_executor` also is used by `server_mqtt` only. And there might be a separate executor per server instance.

Let me verify:
1. `_dispatch_loop` in multi_mqtt.py — single thread, periodic tasks interleaved.
2. server_http: ThreadingHTTPServer? grep.
3. app.py: shared executor? Not local; but maybe `server_http` in local has a `main()` that creates executor. Let me check `server_http.py` for `ThreadingHTTPServer` — the grep didn't match, so maybe it's a custom WSGI/ASGI implementation. Let me grep for "HTTPServer" and "ThreadingMixIn".

Also check `server_http.py` timeout used in execute call: read around 1379.

Let me also check the devices' app.py is remote; the user says "详细分析 本地代码" so focus on local code: rpc_executor.py, multi_mqtt.py dispatch loop, server_mqtt.py, server_http.py.

Let's gather these.

Read `_dispatch_loop` fully to prove the single-thread coupling.

Confirmed: `message_callback` (server_mqtt.handle_message) is invoked synchronously in the single `MQTTMsgDispatch` thread, and that is also the thread that drives periodic tasks and stats pings. So a blocked handle_message freezes:
1. Dispatching of all MQTT messages from all 13 brokers (queue keeps filling, paho network threads keep receiving; notes they push to `_msg_queue`, which is bounded → drops).
2. Periodic tasks (global_topic_list ticks, stats pings/prints).
3. All reply publishing for any request (since replies happen in handle_message).

Now, the executor's 999s mechanism: the timeout only aborts the *caller's wait*, not the executing thread. The worker thread is a daemon thread with an uninterruptible Python call. And it holds `self.lock` (RLock). So:

- Call 1 (the `_recover_client` one) blocks in `loop_stop()` → 999s timeout → caller (dispatch thread) gets "execution timed out after 999s" and sets `_stuck=True`. But the worker thread still holds the lock.
- However during those 999s, the dispatch thread is blocked → nothing else processed. MQTT client-side timeouts (10s) already fired long ago.
- Requests arriving after the 999s: `_stuck` is True → immediate busy error, BUT crucially `_stuck` is never cleared because `done.set()` never happens (worker never returns). So it permanently rejects all new requests. That's "全部卡死" even after the timeout.

Wait — is `_stuck` set? Yes at line 827 when the caller times out and job started. So permanently True unless the worker exits. In our case the worker was blocked forever (loop_stop join with a blocked send) → `_stuck` stays True forever. So every later RPC returns quickly with "executor busy: 上一次执行已超时且仍在后台运行" — not even 999s. But if user's client had timeout of 10s, they'd see timeout before this... actually the busy error returns immediately, so they'd see an immediate error, not timeout. Hmm, but the observed symptom was timeouts on the RPC that called `_recover_client` (the diagnosis RPC) and all subsequent HTTP RPCs timed out on the client side.

But careful: is HTTP using the same executor? In server_http.start_rpc_server line 1506: `executor = PythonExecutor(globals=persistent_ns, main_loop=main_loop)`. That's the standalone `start_rpc_server` path. On the device (app.py), likely both MQTT server and HTTP server share one executor. Need to know. In app.py, they probably create `server_http.start_rpc_server(..., executor=executor)`. Since local repo has no app.py, I should check if there's a reference in server_http to external executor injection. Let's grep "executor=" in server_http and in tests.

Let me check `server_http.py` around 1500-1560 and the `executor` param.

Also relevant: even if HTTP has its own executor, HTTP requests would still not block on executor lock... but on device they timed out, suggesting shared executor OR HTTP handler itself blocked. Actually the summary says "HTTP RPC 全部超时" after calling `_recover_client` remotely. That could be because the shared executor was stuck. Also there's another possibility: the app's Python runtime has a GIL issue — the network thread blocked in send() doesn't hold the GIL though (it releases during syscall). But `sock.shutdown` from another thread... not relevant.

Actually, another important mechanism: `loop_stop()`'s `join()` is on a *different thread*, so a stuck network thread just blocks the joiner. But if the Python interpreter runs with... no.

Let me check whether app.py shares executor. Search local repo for "start_rpc_server(" usages and any doc/comments mentioning app.py sharing.

Check server_mqtt.py:1143 area — that's likely the `if __name__ == "__main__"` path creating HTTP + MQTT server with shared executor. And the device's app.py similarly. Let's read server_mqtt 1120-1170.

So HTTP and MQTT have **separate** PythonExecutors (server_http creates its own at 1506; MQTTServer creates its own at 383). Each has its own RLock. So a stuck MQTT executor would NOT block HTTP's executor lock.

Then why did HTTP RPC also time out during my diagnosis? Two candidate reasons:

1. The tool call I made was via MQTT RPC, and HTTP RPC calls were also made — maybe the HTTP requests came *after* the whole Python runtime was affected? Hmm.

2. The app might expose HTTP RPC through `server_http.start_rpc_server(globals=globals)` with a separate executor — separate lock. Then HTTP RPC should still work... unless the stuck thread holds the GIL... no, join() releases GIL.

Wait — but there's another candidate: the HTTP RPC request that called `_recover_client`? No, I called it via MQTT.

Hmm, actually the summary says: "诊断阶段 `_recover_client('192.168.1.20')` 把 app 的 Python RPC 卡死：paho `loop_stop()` 内部无超时 join，网络线程阻塞在 `send()` → 永久卡住，HTTP RPC 全部超时。"

Maybe what actually happened: `_recover_client` did `loop_stop()` on the paho client from the MQTT dispatch thread. paho's `loop_stop()` waits for the network thread. If the network thread was blocked in `send()`, it never returns. The dispatch thread is stuck. MQTT RPC dead.

For HTTP: maybe both are served through the *same* thread pool? ThreadedHTTPServer → each request its own thread. So HTTP shouldn't be blocked... unless app.py's HTTP server is passed the same `globals` but still separate executor. 

Hmm, but the observed fact: after my blocked MQTT RPC, HTTP RPC `r=1+1` also timed out, and only app restart fixed it. Another possible explanation: `_recover_client` was called from the **network thread / dispatch thread** and it blocked holding `MultiMQTTManager.lock` (self.lock of manager)? Let's look at `_recover_client`: does it acquire `self.lock`? If it holds `self.lock` while calling `_terminate_stuck_client`/`loop_stop`, then any code needing manager lock blocks: `publish` (broadcast), `subscribe`, `stats`, `_on_publish`? `_on_publish` doesn't take lock. HTTP RPC that publishes? The HTTP RPC `r=1+1` doesn't publish.

Hmm but HTTP RPC handler may call `_conf`, `format_result`... no manager lock.

Alternatively: app.py's HTTP server might run the RPC handler in the same global `_stdout_lock`? server_http comments mention previously using `_stdout_lock`... Not now.

Alternatively, the real reason HTTP timed out: the app process might have only one Python thread that can run because... no, GIL is released during blocking I/O.

Wait — paho's `loop_stop()` calls `self._thread.join()`. In Python 3.x, `Thread.join()` from another thread releases the GIL while waiting. The network thread blocked in `send()` also releases GIL. So other Python threads can run.

Hmm, but paho's `_thread_terminate`... Actually paho `loop_stop(force=False)` → `self._thread.join()`. Nothing global.

But: python-for-android's Python runtime... the HTTP server is `http.server` with ThreadingMixIn — should work.

Let me look at the actual `_recover_client` code in the *device* version? We don't have the device version locally (48706B, different). The local version's `_recover_client` acquires `self.lock`? Let's read local `_recover_client`.

Also — important: the manager's `_on_message` runs in paho network thread and calls `_msg_queue.put` (may drop), no manager lock. `_dispatch_loop` was stuck.

Hmm, if HTTP requests timed out, maybe the HTTP server itself was serving from the same thread... app.py might use `server_http_wsgi`/`asgi`? Or Android's Chaquopy? The app is Kotlin + Chaquopy Python; HTTP server is `http.server` on a Python thread.

Actually maybe the HTTP timeouts were on the *client* side due to the MQTT RPC blocking the caller: the "authorization" was in a notebook; my HTTP request via `:1144` was `basic auth q:q`. Requests to device HTTP RPC — the client-side requests timed out (connection accepted but no response). Hmm.

Wait — maybe the reason: the stuck `_recover_client` was called *via HTTP* in one attempt, not MQTT. But the summary says via remote RPC.

Could `_recover_client` hold `self.lock` (manager lock)? Then `_on_message` → ... does `_on_message` need manager lock? Let's check `_on_message` for `with self.lock`. If it does (e.g., for dedup?), then all incoming messages block in the paho network thread — but that still doesn't block HTTP.

Alternatively, the HTTP RPC handler may be executed with a global `_stdout_lock` in app.py? Or the app's Python log sink has a lock that the MQTT thread holds... Overthinking.

The user's actual question is narrower: "rpc_executor.py 不是有超时 999 s 机制吗。如果前一个rpc阻塞。为什么会影响下一步rpc执行。" — i.e., explain why, despite the 999s timeout, a blocked RPC blocks subsequent RPCs. They quoted my claim that `_recover_client('192.168.1.20')` froze all Python RPC.

So I should analyze the local code path precisely:

1. `PythonExecutor.execute()` → `_run_bounded(job, 999)`: spawn a daemon thread; caller waits `done.wait(999)`.
2. The worker holds `self.lock` (RLock) for the entire job.
3. Timeout does NOT kill the worker: it only makes the caller return a timeout dict and sets `_stuck=True`. The worker keeps holding the RLock forever (uninterruptible C-level `join`).
4. Therefore:
   - MQTT channel: one shared executor; `handle_message` is called on the single `MQTTMsgDispatch` thread. The first blocked request blocks that thread for 999s. During this period every subsequent MQTT message (all 13 brokers) is queued but not dispatched; the client-side 10s timeout fires → "所有 RPC 超时". After 999s the dispatch thread returns the timeout reply, then sets `_stuck=True` permanently (worker never finishes → `done` never set → `_stuck` never cleared), so every later request returns "executor busy" immediately (fast fail, not hang). Either way the channel is dead until process restart.
   - Even if `_stuck` were not set, each new request's worker thread would queue on the RLock, and the caller would wait another 999s → serial 999s hangs.
5. Why the timeout mechanism was designed for "long task/dead loop" not "blocked on an unconditional internal call": the timeout protects the caller (dispatch loop timer), but the semantics of "executor busy" is a *global circuit breaker* — one stuck execution denies the whole executor, by design (namespace serialization must not interleave). That's why an infinite block = permanent denial of service for that executor.

6. Note on thread leak: each request during the stuck window spawns a new daemon thread that blocks on the RLock (they exit after acquiring + seeing abandoned, but only after the stuck job ends → effectively leaked forever). So 100 requests = 100 stuck threads.

7. Why the whole thing (HTTP too): locally, MQTT and HTTP each create their own PythonExecutor (server_mqtt.py:393 own; server_http.py:1506 own), so a stuck MQTT executor shouldn't lock HTTP's executor. But there are paths that make the *process* look dead:
   - `MQTTServer.__init__` executor is separate; HTTP separate.
   - However `handle_message`'s reply publish etc. is blocked, and if `_recover_client` was invoked... Actually what did I actually observe? "HTTP RPC 全部超时" — On the device app.py, does HTTP share executor with MQTT? The local app.py doesn't exist; the device app.py may pass `globals=globals()` to both, but each creates its own executor → separate locks.

Hmm. So I should be precise and honest: with local code, MQTT and HTTP have separate executors; the shared bottleneck for MQTT is the single dispatch thread. For HTTP, the blocked part would be per-request threads; they'd hang only if they share the same executor or if the handler calls into MQTT manager lock, or if the Android-side Python RPC bridge (`rpc(...)` in Kotlin) routes HTTP requests through the same Python thread.

Maybe on device, `RpcUiController.buildInfo()` HTTP RPC is handled by the *Android* HTTP server (`com.kingzcheung.xime` may have its own HTTP endpoint 1144 in Kotlin?) Hmm, we observed `:1144` with basic auth q:q, `RpcUiController.buildInfo()` — that could be the Python `server_http` (port 1144) indeed.

Then why did `r=1+1` through HTTP time out? Possibly because `_recover_client` also holds the **manager lock** for a long time, and HTTP RPC handler... doesn't touch manager.

Alternatively: `_recover_client` old code did `old.loop_stop()` — paho's `loop_stop` sets `_thread_terminate = True` then joins. But if the client wasn't started with `loop_start()`, `_thread` is... it is started. Fine.

Hmm — but maybe the critical detail: I called `_recover_client` **via HTTP RPC** in one step, and then through MQTT in another. Let me not over-claim. I should answer the code-level question with what local code proves, and for the HTTP observation note the two possibilities:
(a) if the app wire-up shares one executor (app.py) → direct lock contention;
(b) even with separate executors, Android-side Python log proxy / GIL isn't the issue, but the whole RPC surface *appears* dead because replies are dispatched in the frozen MQTT dispatch thread and the HTTP handler thread is also starved if it publishes/logs via the same locks.

Hmm. Actually there's one more concrete shared resource: `logging`. In app.py, the memory log sink (`MemoryLogProxy` / `sinks`) — logging handlers have locks, but held briefly.

Let me check whether `handle_message`/MQTTServer shares executor with HTTP on device: search local repo for a doc or the `xime` app.py maybe in C:\test\github\Xime? The user's earlier task referenced `C:\test\github\Xime\app\src\main\python\task_manager.py` — a local mirror exists! Path c:\test\github\Xime. That's outside the workspace but readable? Let me check if app.py exists there to see the wiring. That directly answers whether the executors are shared.

Let me glob.

Now check the device's app.py to see how MQTT and HTTP servers are wired (executor sharing).

App.py uses server_mqtt but maybe not HTTP. Let me grep for "start_rpc_server" in app.py — didn't match? The grep pattern included start_rpc_server and only matched `import server_mqtt`. Let me search for "http" / "1144" in app.py.

Actually the device HTTP RPC on port 1144 is probably started by Kotlin (XimeApplication.startPythonRpc) using Python's server_http, or maybe the Kotlin side calls `app.start_rpc(...)`. Let's grep app.py for "rpc".

Confirmed: on device app.py, HTTP server and MQTT server each create their own PythonExecutor with `globals=globals()` (same dict, different executors/locks). So a stuck MQTT executor does NOT block HTTP's executor lock.

So why did HTTP RPC also time out during my troubleshooting? Let me think about the actual detail: on device the HTTP server is `ThreadedHTTPServer`; each request gets a thread. The HTTP handler calls `executor.execute(...)` on its own executor. That should work.

BUT — maybe the stuck thing wasn't the executor lock but the MQTT manager's `self.lock`. In old `_recover_client` code (device version), it did `with self.lock: old = ...` then outside. Hmm.

Actually, wait — the old device `_recover_client` might differ (device file is a different 48KB version). But local pre-fix code: `old._thread_terminate = True; old._sockpairW.send; old.loop_stop()`. `loop_stop()` joins the network thread. Was the network thread blocked in `send()`? paho's `loop_forever`/`_thread_main` calls `self.loop(timeout)` which may block in `select` up to keepalive; it wouldn't block indefinitely on `send()` unless the socket buffer is full and the OS didn't time out — possible with half-dead TCP (window zero, no RST). Then `loop_stop()` blocks forever.

The RPC that called `_recover_client` came through MQTT → executed in `MQTTMsgDispatch` thread → blocked. Then:
- All MQTT requests stop being processed → client-side timeouts. That's clear.
- HTTP RPC: hmm.

Maybe my actual diagnosis sequence: `_recover_client('192.168.1.20')` was called via the device HTTP RPC (the notebook request goes over HTTP 1144?) — the summary says my calls to the device were through MQTT RPC (request_topic='14c'). Then HTTP RPC `r=1+1` timed out. Both are Python; different executors.

Alternative explanation for HTTP timeouts: the app's Python process may have a **single** Chaquopy thread?? No, Chaquopy supports threads.

Or maybe HTTP requests to `:1144` were fine but our HTTP probe used `/r=rpc(...)` which internally called `_recover_client` again? If I called `_recover_client` over HTTP, then that HTTP request's executor is the HTTP one → blocked; and subsequent HTTP requests would hit the same HTTP executor lock → wait 999s → client timeout. That fits perfectly: HTTP lock was the one stuck because the `_recover_client` call was made through HTTP RPC.

And MQTT RPC timeouts would then be due to... `_recover_client` holding `MultiMQTTManager.lock`? Let's check: old code `with self.lock: old = self.clients.get(host)` — brief. But `_terminate_stuck_client`... not holding manager lock. However recovery `self.clients[new_host] = client` brief.

Hmm, but if HTTP's executor was stuck while executing `_recover_client`, MQTT messages are still dispatched — but replies come from `handle_message`, and MQTT RPC should work. Yet they timed out per the summary. Unless the reverse: MQTT executor stuck and HTTP timed out because...

Wait: does `server_http` handler use `globals` dict passed in? The `persistent_ns` — both servers hold the same dict object? HTTP creates `PythonExecutor(globals=persistent_ns)`. MQTT: `PythonExecutor(globals=globals)`. Both are the same dict (app globals). The executor locks are separate.

Hmm, but Python `dict` access is GIL-protected quickly.

OK. Let me consider that the more plausible user-facing explanation doesn't need to fully explain HTTP. The user asks specifically:

"rpc_executor.py 不是有超时999 s机制吗。如果前一个rpc阻塞。为什么会影响下一步rpc执行。"

So: explain the 999s semantics precisely: it bounds the *wait*, not the *execution*. The worker thread is a daemon and can't be killed; it holds the RLock. Therefore:
- During the first 999s, every new RPC waits on the same RLock → each caller blocks up to 999s (much longer than any client timeout) → looks like "all RPC hangs".
- After 999s, `_stuck=True` → fast-fail "executor busy" forever, since the worker never finishes (done never set). So the executor is permanently dead until process restart. This is exactly how the executor behaves with a truly uninterruptible block (C-level `thread.join()`).
- Additionally MQTT channel serializes at the dispatch thread: even before touching executor lock, all messages go through one thread → the whole MQTT surface (all 13 brokers, replies, periodic ticks) freezes during those 999s.

Also: `timeout=999` is only default; the client's own timeout (10s) fires way before; and the timeout mechanism is designed for "long computation / dead loop in Python bytecode" — but a blocked `thread.join()` is not a Python bytecode loop; it cannot be interrupted by anything short of process exit.

Also the executor thread leak: each pending request spawns a thread blocked on lock; they accumulate.

Also note: after the worker eventually unblocks (e.g., socket errors), `finally` sets `_stuck=False` and everything recovers — but that may take a very long time or never.

Also the fact that HTTP and MQTT on device use separate executors: so a stuck MQTT executor wouldn't lock HTTP's executor. The observed HTTP timeouts are more consistent with the HTTP request itself being the one that invoked `_recover_client` (or with the whole app's Python runtime being frozen if the stuck thread was holding the manager lock/GIL). Let me verify what exactly was observed in the prior conversation? We only have the summary. I should be careful and present both:

- If the blocking RPC came in via MQTT → MQTT dispatch thread + MQTT executor stuck; HTTP's own executor is separate, so HTTP should still answer. If HTTP also timed out, the shared resource was something else (e.g. same executor if wiring shares it, or the request went through the MQTT-backed path).
- Actually on device, app.py starts both with separate executors → HTTP should keep working. But my notebook HTTP attempts may have been made *while* the MQTT client (client-side paho) also... no, HTTP doesn't use MQTT.

Hmm, hold on. Maybe there's a shared lock in the **stats/report printing** or in `Tee`/logging sinks. The `MemoryLogProxy` and `LimitedLogFile` use locks. If the stuck thread holds it? It doesn't log.

Another candidate: `server_http` executes with `globals=globals()` (app module globals). The notebook's HTTP call `r=1+1`... fine.

Could the HTTP server thread itself block in `sys.stdout` writes? `Tee` writes to Android log via Chaquopy; `MemoryLogProxy.sinks[0].buffer`. If the MQTT thread is blocked in `send()`, it's not holding a Python lock... unless the block occurred while holding `logging` handler lock. paho's `loop_stop()` doesn't log.

Alternatively the paho network thread blocked in `send()` while holding the paho `_out_message_mutex`? That could block other clients' `publish` but not HTTP.

I think it is worth checking what the *device* version's `_recover_client` looked like — maybe it called `self.lock` (manager) and then `stats`? Let me diff the device multi_mqtt files? The device file is `C:\test\github\Xime\app\src\main\python\multi_mqtt\multi_mqtt.py` — the local mirror! It says device version 48706B. Let's inspect its `_recover_client` and see whether it holds `self.lock` for the whole teardown (that would block HTTP indirectly? no, HTTP doesn't touch manager lock...).

Wait, but HTTP RPC `r=1+1` might get blocked by something else: maybe the notebook request wasn't HTTP but MQTT, and "HTTP RPC 全部超时" refers to *the device HTTP control interface used by the Android app UI* (`RpcUiController`), which is a Kotlin object invoked from Python via Chaquopy. `RpcUiController.buildInfo()` runs Kotlin code on the Python thread? When called from Python via Chaquopy, Chaquopy acquires the Java attachment thread; Kotlin call may be quick.

I think I shouldn't speculate too much about HTTP. Answer the direct question thoroughly, and add a short, clearly-labeled note: "如果你当时观察到 HTTP 也超时，本地代码里 HTTP 是独立 executor（server_http.py:1506），所以原因不是这把锁；要么那次 `_recover_client` 本身是从 HTTP 通道发起的（那就是 HTTP 自己的 executor 被卡），要么是 app 侧接线不同（app.py 里两个 server 各自 new executor，我确认过）。"

Hmm, but wait: maybe the actual observed behavior is explained by this: I called `_recover_client` and it blocked. The MQTT dispatch thread stuck. Then I *restarted* nothing; then I tried HTTP `r=1+1`, which timed out. Why?

Let me check the device's `multi_mqtt.py` version of `_recover_client` and whether recovery thread holds `self.lock` while doing loop_stop. If `_recover_client` runs in the *recovery watchdog thread* and holds `self.lock`... but I invoked it manually via RPC, so it ran in the executor worker thread. It acquires `self.lock` briefly. Not it.

BUT: paho `loop_stop()` joins `_thread`; `_thread` is the network thread. Suppose the network thread is blocked in `client._send_publish` while holding `self._out_message_mutex`... irrelevant to HTTP.

Alternatively: maybe the Chaquopy Python runtime was actually running the HTTP server on the same thread? No, ThreadedHTTPServer spawns threads.

Hmm, one more: `server_http` handler at line 1281 gets executor via `_conf(self,'executor')`. OK.

Another thought: The HTTP request `r=1+1` may have gone through `server_http`'s **basic auth** and streamed response... no.

I'll answer precisely on the executor question and note the HTTP separation with an honest caveat: the HTTP timeouts I observed are best explained by the fact that the blocking call was itself made over the HTTP channel in that experiment, or that app-side wiring shares one executor (verify by app.py — it doesn't share). Hmm, but I confirmed app.py creates separate.

Hold on! Let me re-read app.py: `http_server = server_http.start_rpc_server(globals=globals(), ...)` — this creates executor A. `mqtt_server = server_mqtt.MQTTServer(globals=globals())` → executor B. Two locks. So HTTP unaffected by MQTT executor.

Unless: *both* executors share the **same globals dict**, and my `_recover_client` call over **HTTP** blocked HTTP's executor while MQTT requests failed for a different reason: the MQTT RPC handler `handle_message` executes on the dispatch thread — it should be fine. But the MQTT requests' *replies* are published by the MQTT net — fine.

Wait, maybe the truth is the opposite direction, and both channels share something: `globals` mutation. When executor A runs `_recover_client`, it holds `self.lock` (executor A's RLock) — not B's. But both write to the same dict? Only if the code defines names.

Hmm. What if the HTTP RPC call was `r=rpc(...)`? `rpc(...)` in the globals dict might be the MQTT RPC client function (client_mqtt.rpc) hitting the network... no.

OK, I'll present the analysis with a clear "本地代码可证" section and a short "关于当时 HTTP 也超时" section listing two possibilities and how to distinguish (check `_stuck`? logs "execution timed out after 999s" vs "executor busy"). Actually logs would tell: the 999s timeout message appears in app memory log. And the log line "executor busy: 上一次执行已超时且仍在后台运行" is immediate.

Hmm, one more consideration that makes HTTP timeouts likely even with separate executors: if `_recover_client` blocked while holding **`MultiMQTTManager.lock`** — let's actually verify old device code doesn't hold the lock across teardown. Local pre-fix code: `with self.lock: old = ...` then teardown outside. But device version may differ. Let me check the device mirror's `_recover_client`.

And also whether `_on_message` in the device version takes `self.lock`. If the network thread is blocked inside `send()` while holding `self.lock`... no, `_on_message` runs in network thread.

Why would HTTP `r=1+1` need manager lock? It doesn't.

Fine. Let me check device mirror file quickly for `_recover_client` to be accurate about what I actually ran on device.

Confirmed device rpc_executor is same design (999s, _stuck).

Now, one important extra piece: the MQTT dispatch thread. Let me double-check `handle_message` and where the reply is published (same thread). Yes.

Also, is there any chance that MQTT RPC's `_stuck` scenario explains "后续 RPC 全部超时" — yes for MQTT. For HTTP, separate executor.

But note the user's notebook call: `client_mqtt.rpc('3', request_topic='14c', timeout=10, ...)` — client-side timeout 10s. So the user sees timeout at 10s while the device is stuck.

Now also: How did the stuck RPC get *triggered*? I ran `_recover_client('192.168.1.20')` via RPC on device → it executed in MQTT executor worker thread. It blocked in `loop_stop()` join on the network thread.

Wait — the network thread is blocked in `send()`? Hmm, if the network thread were blocked in `send()`, paho wouldn't be *receiving* messages in `on_message`. But observed: 收报文正常. So maybe the *supposed* blocking wasn't in send; maybe `loop_stop()` blocked for a different reason: `_thread_terminate` + join, and the network thread kept looping with select timeouts... Then join should return quickly. Unless paho's `loop_stop` and the network thread had a race. Hmm.

Actually paho 2.x `loop_stop(force=False)`:
```
with self._thread_terminate_lock:
    self._thread_terminate = True
    if force: ... shutdown socket
    if self._thread is not None:
        self._thread.join()
        self._thread = None
```
In paho 2.x, loop_stop sets terminate then joins. The network thread runs `loop_forever` → `loop(timeout)` → `select` with timeout = keepalive; when `_thread_terminate` set, after select returns it exits. Should be quick unless blocked in `_packet_write` → `sock.send()` — which could block if send buffer full (TCP zero window). But then receiving would still work (separate direction). Yes! That's consistent: socket can receive (RX) while send (TX) blocks when the peer's window is zero or the peer stopped ACKing data. So network thread blocked in `send()` → `loop_stop()` join never returns. And `is_connected()` True.

But wait: our new `_terminate_stuck_client` does `sock.shutdown(SHUT_RDWR)` from another thread — will that unblock a `send()` on Windows/Linux? Generally yes: shutdown causes send to fail with EPIPE/ENOTCONN. On Android/Linux, shutdown(SHUT_WR) on a socket blocked in send → send returns EPIPE. Good enough, plus join timeout as backstop.

Now write the answer. Structure:

## 1. 999s 到底限制了什么
Code refs: rpc_executor.py#L769-L838. Key: `_run_bounded` spawns daemon thread; caller waits `done.wait(timeout)`; timeout path sets `abandoned`, `_stuck=True`, returns error. It never stops/kills worker. Worker holds `self.lock` (RLock) for whole job.

## 2. 为什么阻塞会传导到下一个 RPC
- 锁是全局串行点：一个 executor 一把锁，所有请求（MQTT 全部 topic、所有 broker；HTTP 同一台 server 的所有请求）都排队。
- 等待时间不是 10s，而是 999s：下一个请求会先 `done.wait(999)`。客户端 10s 早就超时。
- 999s 之后仍不恢复，而是永久拒绝：`_stuck=True` 只能由 worker 的 finally 清除，而 worker 卡在一个不可中断的 C 调用（`thread.join()` 无超时）里，永远走不到 finally → 之后每个请求立刻返回 "executor busy"（快速失败），直到进程重启。
- 线程泄漏：卡住期间每个新请求都 `threading.Thread(...)` 起一个线程去抢锁，全部永久阻塞堆积。

## 3. MQTT 通道还多一层串行：单分发线程
`_dispatch_loop` 单线程：queue.get → message_callback(handle_message)；同一线程还驱动 ping/periodic。handle_message 里 `executor.execute` 是同步等待。所以哪怕没有 executor 锁，第一个请求卡住就已冻结全部 13 个 broker 的分发和周期任务（global_topic_list、stats ping）；reply 也在这条线程上发不出去。

计算：999s ≈ 16.6 分钟；期间 13 条 broker 的 paho 网络线程仍在收报文 → `_msg_queue` 堆积直到 MSG_QUEUE_MAXSIZE 丢弃；客户端全部超时。

## 4. 为什么"超时机制"在这里失效（设计边界）
- 它是"调用方等待上限"，不是"执行上限"。Python 无法强杀线程，尤其阻塞在 `Thread.join()` 这类 C 层。
- `_stuck` 的设计前提是"后台代码最终会结束"（死循环还能被……其实也不能，但至少可以在下次解锁后恢复）。真正无限阻塞时 busy 永久成立。
- 所以真实结论：任何可能无限阻塞的调用都不能在 executor 里直接做（尤其不能从 RPC 里调 `loop_stop()` 这类无超时 join）。这正是本次修复把 `_recover_client` 换成 `_terminate_stuck_client`（有界 join）的原因。

## 5. 为什么当时 HTTP RPC 也超时（口径校正）
Local app.py: HTTP 和 MQTT 各自 `PythonExecutor`（server_http.py:1506；server_mqtt.py:393），锁不同。所以严格说：MQTT 的 executor 卡死不会直接锁住 HTTP 的 executor。当时 HTTP 也表现为超时，只有两种解释：
(a) 那次 `_recover_client` 是通过 HTTP 通道发起的（被卡的是 HTTP 自己的 executor）；
(b) 客户端侧感知：`client_mqtt.rpc` 走 MQTT；
其余若真出现 HTTP 同时超时，应检查 app 侧接线（是否把同一个 executor 传给了两个 server）或日志里是否出现 "execution timed out after 999s"（说明该请求确实进入执行且超时）还是 "executor busy"（说明之前已有卡死任务）。

Hmm — the user's question may expect a crisper answer. But honesty is important. Actually maybe I should verify: did my diagnosis call `_recover_client` over HTTP? The summary says "诊断阶段 _recover_client('192.168.1.20') 把 app 的 Python RPC 卡死". And the notebook `client_mqtt.rpc` was the channel used. If MQTT-only stuck, HTTP shouldn't fail. But the observed "HTTP RPC 全部超时" might have been because the *device UI* (RpcUiController) calls go through Python→Kotlin on... no.

Hmm, wait. There's one more possibility I should check: does the device's **HTTP server** share the MQTT manager's lock via `globals`? `r=1+1` no.

Or maybe the HTTP server on device isn't `server_http` but Kotlin-based UI bridge on 1144 that calls Python on the *main* thread, and the main Python thread... Chaquopy calls from Java run on the calling thread. If the UI's buildInfo call is executed on the Python main thread while... no.

I'll keep section 5 short and factual, focusing on what local code shows, and note it's a separate executor — i.e., correcting the earlier claim in my commit message slightly. That's valuable and honest: "严格按本地代码，MQTT 的卡死不会连带锁死 HTTP executor；如果当时 HTTP 也超时，说明那次阻塞调用是走 HTTP 通道发出的（或者 app 侧共用 executor 的版本差异）". Actually app.py confirms no sharing. So most likely my `_recover_client` experiment was issued via HTTP (port 1144) at some point. Hmm, the summary says via MQTT RPC. I can say "这两条通道各自的锁是独立的，所以准确说法是：卡死的是'发起那次调用'的通道的 executor；另一个通道只有在等待同一把锁时才受影响" and note the observed HTTP timeouts likely came from the same channel being used.

Let me also mention exact line numbers for citations:
- rpc_executor.py L691 execute default timeout=999; L769-838 _run_bounded; L789-808 worker holds lock; L821 done.wait; L825-834 timeout sets _stuck; L779-782 fast fail.
- multi_mqtt.py L1147-1150 dispatch thread creation; L1886-1899 single-thread callback; L1834 _dispatch_loop; L1911-1935 periodic tasks same thread.
- server_mqtt.py L755 executor.execute in handle_message; L670 comment "该线程是所有 broker 共享的单线程".
- server_http.py L1379 execute; L1548 ThreadedHTTPServer; L1506 own executor.
- app.py L338-354 separate servers.

Also mention `_stuck` never clears: L804-808 finally; L827.

Length: moderately detailed but concise-ish. Use a small diagram? Inline visuals could help but the user asked for code analysis; text is fine. Maybe a compact flow diagram via PureShowWidget is overkill and adds latency; skip? The guidance encourages visuals for explanation, but here the user wants code-level analysis in Chinese. I'll skip the widget and keep it tight. Hmm, "Inline Visuals... proactively use when visual genuinely aids". A timeline diagram would help show 0s→999s→forever. But risk vs benefit: moderate. I'll skip to stay concise per output efficiency.

Let me write with file links.
## 结论先说

999s 不是“执行超时”，它是**调用方等待上限**。执行体跑在另一个 daemon 线程里，超时只让调用方提前返回，**不能停掉那个线程**。而那个线程全程持有执行器唯一的一把 `RLock`，所以它一卡死，后面所有请求都得排队等同一把锁——`done.wait(999)`，客户端 10s 早就超时了。

## 一、999s 机制的准确语义（[rpc_executor.py](file:///c:/test/github/multi_mqtt/rpc_executor.py#L769-L838)）

`execute()` 默认 `timeout=999`（[rpc_executor.py#L691](file:///c:/test/github/multi_mqtt/rpc_executor.py#L691)），然后：

```python
threading.Thread(target=_work, name="rpc-exec", daemon=True).start()   # L810
...
if done.wait(timeout):        # 调用方最多等 999s
    return ...
abandoned.set()
if started.is_set():
    self._stuck = True        # 只打了个标记
    return {"ok": False, "error": "execution timed out after 999s..."}
```

关键点：

1. `_work` 一进来就 `with self.lock`（[L791](file:///c:/test/github/multi_mqtt/rpc_executor.py#L789-L800)），**整个 job 期间独占这把锁**，这是保证命名空间串行、请求间不互相污染的唯一手段。
2. 超时路径只做三件事：作废标记、置 `_stuck`、返回错误 dict。**没有任何手段终止 worker**——Python 没有安全的线程强杀，何况阻塞点常常在 C 层（`Thread.join()`、`sock.send()`）。
3. `_stuck` 的清除只在 `_work` 的 `finally: done.set(); self._stuck = False`（[L804-L808](file:///c:/test/github/multi_mqtt/rpc_executor.py#L804-L808)）。worker 永远不返回，`finally` 就永远不执行。

## 二、为什么“下一个 RPC”一定被拖住

同一个 executor（同一把锁）上：

| 阶段 | 新请求的行为 | 客户端看到 |
|---|---|---|
| 0 ~ 999s | 新请求照样起线程，卡在 `self.lock.acquire()`；调用方 `done.wait(999)` | 10s 就超时，且看不出原因 |
| 999s 之后 | `if self._stuck: return 立即失败`（[L779-L782](file:///c:/test/github/multi_mqtt/rpc_executor.py#L779-L782)） | 立刻收到 “executor busy: 上一次执行已超时且仍在后台运行” |
| worker 最终返回（可能永不） | 才恢复 | 恢复正常 |

所以“999s 兜底”在**真正无限阻塞**的场景下等于永久熔断：`_stuck` 一旦置位就再也没有清的机会，直到进程重启。这和我 11-11 在 14c 上观察到的一致——`_recover_client` 里的 `old.loop_stop()` 内部是 `self._thread.join()`（无超时），网络线程阻塞在 TCP `send()`（对端零窗口 / 半死 socket），join 永不返回。

还有一个次要代价：卡住期间每个新请求都 `Thread(...).start()` 一个线程去抢锁（[L810](file:///c:/test/github/multi_mqtt/rpc_executor.py#L810)），它们全部永久阻塞——线程数随请求数线性泄漏。

## 三、MQTT 通道还多一层串行：单分发线程

比 executor 锁更早卡住的是分发线程：

```python
self._dispatch_thread = threading.Thread(target=self._dispatch_loop, name="MQTTMsgDispatch")
```
（[multi_mqtt.py#L1147-L1150](file:///c:/test/github/multi_mqtt/multi_mqtt.py#L1147-L1150)）

`_dispatch_loop` 在**同一条线程**里做三件事（[multi_mqtt.py#L1886-L1935](file:///c:/test/github/multi_mqtt/multi_mqtt.py#L1886-L1935)）：

1. `queue.get()` 取消息 → `self.message_callback(topic, data, host)`；
2. 驱动 stats ping / 报告；
3. 驱动 `set_periodic_task` 注册的周期任务（`global_topic_list_tick`）。

`message_callback` 就是 `server_mqtt.handle_message`，里面是同步的 `self.executor.execute(code, format_result=True)`（[server_mqtt.py#L755](file:///c:/test/github/multi_mqtt/server_mqtt.py#L755)，[server_mqtt.py#L670](file:///c:/test/github/multi_mqtt/server_mqtt.py#L662-L668) 明确写了“该线程是所有 broker 共享的单线程”）。

于是那 999 秒里：

- 13 条 broker 的 paho 网络线程还在正常收报文（所以“收得到请求”），全部塞进 `_msg_queue`，满了就丢；
- **一条都不会被分发执行**（不是只有那一条卡住）；
- 回包也发不出去——`_reply_publish` 在 `handle_message` 里，同一条线程；
- 周期 ping / 全局 topic 上报一起停摆。

这解释了现场最重要的那个矛盾现象：**手机日志显示收到了请求并执行了，客户端却完全收不到回包，且之后所有 RPC 都无响应**。

## 四、口径校正：HTTP 通道是另一把锁

严格按本地代码，`server_http` 和 `server_mqtt` 各自 new 一个 `PythonExecutor`：

- HTTP：`executor = PythonExecutor(globals=persistent_ns, main_loop=main_loop)`（[server_http.py#L1506](file:///c:/test/github/multi_mqtt/server_http.py#L1506)），服务是 `ThreadedHTTPServer`（[L1548](file:///c:/test/github/multi_mqtt/server_http.py#L1548)），每请求一线程；
- MQTT：`self.executor = PythonExecutor(globals=globals)`（[server_mqtt.py#L393](file:///c:/test/github/multi_mqtt/server_mqtt.py#L393)）；
- 设备侧 [app.py#L337-L356](file:///c:/test/github/Xime/app/src/main/python/app.py#L337-L356) 也是两个 server 独立创建，不共用 executor。

所以准确说法是：**卡死的只是“发起那次调用”的通道的 executor**。如果当时 HTTP RPC 也全部超时，只有两种可能：那次 `_recover_client` 实验本身就是从 HTTP（`:1144`）发起的，被卡的是 HTTP 自己的 executor；或者 app 侧存在共用 executor 的接线差异。区分方法很简单——看 app 日志里是 `execution timed out after 999s`（请求已执行、卡在执行体）还是 `executor busy: ...`（之前已有卡死任务占着锁）。我上一版 commit message 里“把 HTTP RPC 一起卡死”的表述按本地代码看不成立，应改为“卡死该通道的 executor，并使该通道所有后续请求永久 busy”。

## 五、对本次修复的意义

这正是新代码要绕开的东西：

- `_terminate_stuck_client()` 用 `sock.shutdown(SHUT_RDWR)` 逼阻塞的 `send()` 立刻报错，再 `thread.join(2.0)` 有界等待，线程不退出只 WARNING；
- `_publish_health_scan` 在**独立的看门狗线程**里跑，不占用 `MQTTMsgDispatch`，也不进 `PythonExecutor`；
- 所有新增状态读写都是短临界区 + try/except，绝不出现“持有 executor 锁去做无超时阻塞调用”。

一句话总结：`timeout=999` 保护的是“调用方不会永远等”，不是“执行体不会永远跑”；只要 `executor` 里出现无超时 join / 阻塞 send 这类不可中断调用，这把全局串行锁就会把该通道的所有 RPC 变成永久熔断，直到进程重启。