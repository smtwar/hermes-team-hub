#!/usr/bin/env python3
"""
Hub 定时巡检脚本 — 零心跳、纯轮询、零 Token 消耗
每 N 秒/分钟运行一次，自动检测所有频道的新消息、新任务、新频道。

v2.0 改进：
  - 自动发现所有频道（不限于 env 配置的 CHANNELS）
  - 任务分配检测：支持多值分配（逗号分隔）和 @提及
  - 新频道创建提醒
  - 状态变更检测
  - 无动态时静默

使用方法:
  hermes cron create "every 1m" --name hub-watch \
    --no_agent true --script hub_watch.py --deliver origin

环境变量:
  HUB_AGENT_NAME  当前设备标识（必填）
"""

import os, sys, json, time
import urllib.request, urllib.error

HUB_URL = os.environ.get("HERMES_RELAY_URL", "http://39.104.86.113:8765")
TOKEN = os.environ.get("HERMES_RELAY_TOKEN", "my-relay-secret-2025")
AGENT = os.environ.get("HUB_AGENT_NAME", "")

STATE_FILE = os.path.expanduser(f"~/.hermes_hub_state_{AGENT}.json")


def _load_state():
    try:
        with open(STATE_FILE) as f:
            return json.load(f)
    except:
        return {"channels": {}, "known_channels": [], "seen_task_ids": [], "last_status": {}}


def _save_state(state):
    os.makedirs(os.path.dirname(STATE_FILE) or ".", exist_ok=True)
    with open(STATE_FILE, "w") as f:
        json.dump(state, f, ensure_ascii=False, indent=2)


def _req(path):
    r = urllib.request.Request(f"{HUB_URL}{path}",
        headers={"Authorization": f"Bearer {TOKEN}"})
    return json.loads(urllib.request.urlopen(r).read().decode())


def _is_for_agent(assignee_str: str) -> bool:
    """检查任务分配列表中是否包含本设备"""
    if not AGENT or not assignee_str:
        return False
    # 支持 "WIN_LOCAL" 或 "WIN_LOCAL,Ubuntu_LOCAL" 格式
    parts = [p.strip() for p in assignee_str.split(",")]
    return AGENT in parts


def main():
    if not AGENT:
        print("⚠️ 需要设置 HUB_AGENT_NAME 环境变量")
        return

    state = _load_state()
    reports = []
    has_new = False
    now = time.time()

    # ─── 1. 检测新频道 ────────────────────────────────────
    try:
        data = _req("/hub/channels")
        all_channels = [c["name"] for c in data.get("channels", [])]
        known = set(state.get("known_channels", []))
        current_set = set(all_channels)

        new_channels = current_set - known
        if new_channels:
            state["known_channels"] = sorted(current_set)
            for nc in sorted(new_channels):
                reports.append(f"➕ 新频道创建: #{nc}")
            has_new = True
    except:
        pass

    # ─── 2. 扫描所有频道的新消息 ─────────────────────────
    for channel in all_channels:
        ch_state = state.setdefault("channels", {}).setdefault(channel, {"last_msg_id": 0})
        last_id = ch_state.get("last_msg_id", 0)

        try:
            data = _req(f"/hub/chat?channel={channel}&since={last_id}&limit=50")
            msgs = data.get("messages", [])
            if msgs:
                # 不过滤自己的消息（让每个 Agent 自己决定什么重要）
                max_id = max(m["id"] for m in msgs)
                ch_state["last_msg_id"] = max_id

                # 筛选：排除自己的，或包含 @自己 的
                relevant = [m for m in msgs
                            if m.get("sender") != AGENT
                            or f"@{AGENT}" in m.get("content", "")]

                if relevant:
                    reports.append(f"\n💬 #{channel} ({len(relevant)} 条):")
                    for m in relevant[-5:]:
                        t = time.strftime("%H:%M", time.localtime(m["timestamp"]))
                        content = m.get("content", "")[:120]
                        reports.append(f"   [{t}] {m['sender']}: {content}")
                    if len(relevant) > 5:
                        reports.append(f"   ... 共 {len(relevant)} 条")
                    has_new = True
        except:
            pass

    # ─── 3. 检查与我相关的任务 ──────────────────────────
    try:
        # 获取所有任务（不过滤 assignee，因为可能是多值）
        data = _req("/hub/tasks")
        tasks = data.get("tasks", [])

        # 找出与我相关：分配给我 或 @了我
        my_tasks = [t for t in tasks
                    if (t.get("assignee") and _is_for_agent(t["assignee"]))
                    or (t.get("desc") and f"@{AGENT}" in t["desc"])
                    or (t.get("title") and f"@{AGENT}" in t["title"])]

        active = [t for t in my_tasks if t.get("status") in ("todo", "in_progress", "review")]
        current_ids = set(t["id"] for t in active)
        prev_ids = set(state.get("seen_task_ids", []))

        new_ids = current_ids - prev_ids
        # 检查现有任务是否有状态变更
        changed_ids = set()
        for t in active:
            if t["id"] in prev_ids:
                old_entry = state.get("task_states", {}).get(str(t["id"]))
                if old_entry and old_entry.get("status") != t.get("status"):
                    changed_ids.add(t["id"])

        if new_ids or changed_ids:
            state["seen_task_ids"] = sorted(current_ids)
            state["last_task_scan"] = now
            # 保存任务状态用于下次比较变更
            task_states = state.setdefault("task_states", {})
            for t in active:
                task_states[str(t["id"])] = {"status": t["status"], "updated_at": now}

            reports.append(f"\n📋 与我相关的任务 ({len(active)} 个):")
            for t in active[:5]:
                icon = {"todo": "🟢", "in_progress": "🟡", "review": "🔵", "done": "✅"}.get(t["status"], "⚪")
                tag = " 🆕" if t["id"] in new_ids else " 🔄" if t["id"] in changed_ids else ""
                reports.append(f"   {icon} #{t['id']}{tag} {t['title'][:60]}")
            has_new = True
    except:
        pass

    # ─── 4. 检查智能体状态变化 ──────────────────────────
    try:
        data = _req("/hub/status")
        statuses = data.get("statuses", [])
        current = {}
        for s in statuses:
            if s["agent"] != AGENT:
                current[s["agent"]] = f"[{s['status']}] {s.get('message','')[:50]}"

        prev = state.get("last_status", {})
        changed = {a: s for a, s in current.items() if prev.get(a) != s}

        if changed:
            state["last_status"] = current
            reports.append(f"\n📊 成员状态变更:")
            for agent, status in changed.items():
                reports.append(f"   {agent}: {status}")
            has_new = True
    except:
        pass

    _save_state(state)

    if not has_new:
        return  # 静默

    print(f"🔍 [{AGENT}] 工作组动态 ({time.strftime('%H:%M:%S')}):")
    print("\n".join(reports))
    print()


if __name__ == "__main__":
    main()
