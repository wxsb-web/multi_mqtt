from client.pty_client_mqtt import ai_pty_run
import json

cmd = (
    "cd /root/build_xime_home && "
    "git ls-files multi_mqtt | head -5; echo COUNT:; git ls-files multi_mqtt | wc -l; "
    "git log --oneline -1 -- multi_mqtt"
)
r = ai_pty_run(cmd, timeout=60)
print("REMOTE:", json.dumps(r, ensure_ascii=False)[:2000])
