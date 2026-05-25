#!/usr/bin/env python3
"""
Hermes 共享记忆同步脚本
========================
将 Relay 服务器上的共享记忆拉取到本地，并将本地的共有记忆上传。

设计原则:
  - 零依赖（纯 Python 标准库）
  - 增量同步（since timestamp）
  - 本地状态文件追踪已同步的条目
  - 适配 no_agent=true cron 模式（stdout 输出即为 Agent 可见内容）

用法:
  # 手动同步
  python3 sync_memory.py

  # Cron (每5分钟):
  */5 * * * * cd /root/hermes-relay && python3 scripts/sync_memory.py >> sync_memory.log 2>&1

环境变量:
  HERMES_RELAY_URL    Relay 服务器地址 (默认: http://39.104.86.113:8765)
  HERMES_RELAY_TOKEN  认证 Token
  HUB_AGENT_NAME      本机 Agent 名称 (用于上传时的 author 字段)
  SYNC_MEMORY_SCOPE   本机作用域 (windows / linux / cloud / all)
"""

import os, sys, json, time
import urllib.request, urllib.error

RELAY_URL = os.environ.get("HERMES_RELAY_URL", "http://39.104.86.113:8765")
TOKEN = os.environ.get("HERMES_RELAY_TOKEN", "my-relay-secret-2025")
AGENT = os.environ.get("HUB_AGENT_NAME", os.environ.get("HERMES_AGENT_NAME", "unknown"))
SCOPE = os.environ.get("SYNC_MEMORY_SCOPE", "all")

STATE_DIR = os.path.expanduser("~/.hermes")
STATE_FILE = os.path.join(STATE_DIR, f"sync_memory_{AGENT}.json")


def _req(method, path, data=None):
    url = f"{RELAY_URL}{path}"
    kwargs = {"method": method, "headers": {
        "Authorization": f"Bearer {TOKEN}",
        "Content-Type": "application/json",
    }}
    if data is not None:
        kwargs["data"] = json.dumps(data, ensure_ascii=False).encode()
    try:
        resp = urllib.request.urlopen(urllib.request.Request(url, **kwargs))
        return json.loads(resp.read().decode())
    except urllib.error.HTTPError as e:
        return json.loads(e.read().decode()) if e.code != 204 else {}


def load_state():
    try:
        with open(STATE_FILE) as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return {"since": 0, "seen_ids": [], "uploaded_keys": []}


def save_state(state):
    os.makedirs(STATE_DIR, exist_ok=True)
    with open(STATE_FILE, "w") as f:
        json.dump(state, f, ensure_ascii=False, indent=2)


def main():
    state = load_state()
    reports = []
    has_new = False

    # ─── 1. Pull: 拉取增量共享记忆 ────────────────────────
    since = state.get("since", 0)
    path = f"/hub/memory/shared?since={since}&limit=500"
    result = _req("GET", path)
    entries = result.get("entries", [])

    if entries:
        # 过滤掉已见过的 ID
        seen = set(state.get("seen_ids", []))
        new_entries = [e for e in entries if e.get("id") not in seen]
        seen_ids = set(state.get("seen_ids", []))
        for e in new_entries:
            seen_ids.add(e["id"])

        if new_entries:
            has_new = True
            reports.append(f"📚 同步到 {len(new_entries)} 条新共享记忆:")
            for e in new_entries:
                sc = f"[{e.get('scope', 'all')}]" if e.get("scope", "all") != "all" else ""
                author = e.get("author", "?")
                key = e.get("key", "")
                val = e.get("value", "")[:100]
                reports.append(f"  {sc} {key} = {val}  (by {author})")

        # 更新 since 到最新时间戳
        max_ts = max(e.get("timestamp", 0) for e in entries)
        if max_ts > state.get("since", 0):
            state["since"] = max_ts
            reports.append(f"  📌 下次 since: {max_ts}")

        state["seen_ids"] = list(seen_ids)

    # ─── 2. Push: 上传本地共享记忆给 Relay ────────────────
    # 本地共享记忆文件格式: ~/.hermes/shared_memory_push.json
    # Agent 或其他脚本可以向此文件追加条目，sync_memory.py 负责上传
    push_file = os.path.join(STATE_DIR, "shared_memory_push.json")
    try:
        with open(push_file) as f:
            pending = json.load(f)
        if pending:
            uploaded = []
            for entry in pending:
                key = entry.get("key", "")
                value = entry.get("value", "")
                entry_scope = entry.get("scope", SCOPE)
                author = entry.get("author", AGENT)
                result = _req("POST", "/hub/memory/shared", {
                    "key": key, "value": value, "author": author, "scope": entry_scope,
                })
                if result.get("success"):
                    uploaded.append(key)
                else:
                    reports.append(f"  ⚠️ 上传失败: {key} - {result.get('error', '?')}")
            if uploaded:
                has_new = True
                reports.append(f"📤 已上传 {len(uploaded)} 条本地记忆到 Relay: {', '.join(uploaded[:5])}" +
                               (f" ... 等 {len(uploaded)} 条" if len(uploaded) > 5 else ""))
            # 清空推送队列
            with open(push_file, "w") as f:
                json.dump([], f)
    except (FileNotFoundError, json.JSONDecodeError):
        pass

    save_state(state)

    if not has_new:
        return  # 静默

    print(f"🧠 [{AGENT}] 共享记忆同步 ({time.strftime('%H:%M:%S')}):")
    print("\n".join(reports))
    print()


if __name__ == "__main__":
    main()
