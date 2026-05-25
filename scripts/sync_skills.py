#!/usr/bin/env python3
"""
Hermes 共享技能同步脚本
========================
将 Relay 服务器上的共享技能拉取到本地 skills 目录，
并将本地 skills 推送到共享仓库。

设计原则:
  - 纯 Python 标准库，零依赖
  - 增量同步（基于版本号）
  - 本地 skills 优先（同名不覆盖）
  - 适配 no_agent=true cron 模式

用法:
  python3 sync_skills.py

环境变量:
  HERMES_RELAY_URL      Relay 服务器地址
  HERMES_RELAY_TOKEN    认证 Token
  HUB_AGENT_NAME        本机 Agent 名称
  SYNC_SKILLS_DIR       本地 skills 目录 (默认: ~/.hermes/skills/)
  SYNC_SKILLS_PUSH      是否允许推送本地 skills (默认: true)
"""

import os, sys, json, time, hashlib
import urllib.request, urllib.error
from pathlib import Path

RELAY_URL = os.environ.get("HERMES_RELAY_URL", "http://39.104.86.113:8765")
TOKEN = os.environ.get("HERMES_RELAY_TOKEN", "my-relay-secret-2025")
AGENT = os.environ.get("HUB_AGENT_NAME", os.environ.get("HERMES_AGENT_NAME", "unknown"))
LOCAL_SKILLS_DIR = Path(os.environ.get("SYNC_SKILLS_DIR", os.path.expanduser("~/.hermes/skills")))
PUSH_ENABLED = os.environ.get("SYNC_SKILLS_PUSH", "true").lower() == "true"

STATE_DIR = os.path.expanduser("~/.hermes")
STATE_FILE = os.path.join(STATE_DIR, f"sync_skills_{AGENT}.json")


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
        body = e.read().decode()
        try:
            return json.loads(body)
        except:
            return {"success": False, "error": f"HTTP {e.code}"}


def load_state():
    try:
        with open(STATE_FILE) as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return {"known_skills": {}, "last_sync": 0}


def save_state(state):
    os.makedirs(STATE_DIR, exist_ok=True)
    with open(STATE_FILE, "w") as f:
        json.dump(state, f, ensure_ascii=False, indent=2)


def local_skill_name(path: Path) -> str:
    """从本地 skills 文件路径提取技能名（取第一级子目录名或文件名）"""
    rel = path.relative_to(LOCAL_SKILLS_DIR)
    parts = rel.parts
    # 格式: category/name/SKILL.md 或 name.py/name.md
    if len(parts) >= 2 and parts[-1] == "SKILL.md":
        return parts[-2]  # category/skill-name/SKILL.md → skill-name
    return path.stem  # name.md → name


def local_skill_description(path: Path) -> str:
    """从 SKILL.md 的第一行中提取描述"""
    try:
        with open(path, "r") as f:
            first_line = f.readline().strip()
        if first_line.startswith("# "):
            return first_line[2:].strip()
        return first_line[:80]
    except:
        return ""


def main():
    state = load_state()
    reports = []
    has_new = False

    # ─── 1. 获取远程共享技能列表 ──────────────────────────
    result = _req("GET", "/hub/skills/shared")
    remote_skills = {s["name"]: s for s in result.get("skills", [])}
    known = state.get("known_skills", {})

    # ─── 2. 下载新增/更新的共享技能 ──────────────────────
    downloaded = []
    for name, meta in remote_skills.items():
        known_ver = known.get(name, {}).get("version", 0)
        remote_ver = meta.get("version", 1)

        if remote_ver > known_ver:
            # 需要下载
            detail = _req("GET", f"/hub/skill/shared/{name}")
            if detail.get("success"):
                skill = detail["skill"]
                content = skill.get("content", "")

                # 写入本地 skills 目录（不覆盖已有文件）
                skill_path = LOCAL_SKILLS_DIR / f"{name}.md"
                if not skill_path.exists():
                    skill_path.parent.mkdir(parents=True, exist_ok=True)
                    with open(skill_path, "w", encoding="utf-8") as f:
                        f.write(content)
                    downloaded.append({"name": name, "version": remote_ver})
                    has_new = True
                else:
                    reports.append(f"  ⏭️ {name}: 本地已有同名技能，跳过")

    if downloaded:
        names_str = ", ".join([f"{d['name']} v{d['version']}" for d in downloaded])
        reports.append(f"📥 下载 {len(downloaded)} 个新共享技能: {names_str}")
        # 更新已知技能版本
        for d in downloaded:
            known[d["name"]] = {"version": d["version"]}

    # ─── 3. (可选) 上传本地技能到共享仓库 ────────────────
    if PUSH_ENABLED:
        uploaded = []
        # 扫描本地 skills 目录
        local_files = []
        for pattern in ("**/*.md", "**/SKILL.md"):
            for fp in LOCAL_SKILLS_DIR.glob(pattern):
                if fp.is_file() and fp not in local_files:
                    local_files.append(fp)

        for fp in local_files:
            name = local_skill_name(fp)
            if not name:
                continue
            remote_ver = remote_skills.get(name, {}).get("version", 0)
            known_ver = known.get(name, {}).get("version", 0)

            # 如果远程没有，或者本地版本号 > 远程版本号
            # 检查本地 skill 是否有 version 信息
            with open(fp, "r") as f:
                content = f.read()

            # 简单版本检测: 检查 frontmatter 中的 version
            local_ver = 1
            if content.startswith("---"):
                try:
                    parts = content.split("---", 2)
                    if len(parts) >= 3:
                        import re
                        vm = re.search(r'version:\s*([\d.]+)', parts[1])
                        if vm:
                            local_ver = float(vm.group(1))
                except:
                    pass

            if name not in remote_skills or local_ver > remote_ver:
                desc = local_skill_description(fp)
                result = _req("POST", "/hub/skills/shared", {
                    "name": name,
                    "content": content,
                    "description": desc,
                    "author": AGENT,
                    "version": int(local_ver) if local_ver == int(local_ver) else local_ver,
                })
                if result.get("success"):
                    sk = result["skill"]
                    uploaded.append(f"{name} v{sk['version']}")
                    has_new = True
                    known[name] = {"version": sk["version"]}
                else:
                    reports.append(f"  ⚠️ 上传失败 {name}: {result.get('error', '?')}")

        if uploaded:
            reports.append(f"📤 上传 {len(uploaded)} 个本地技能: {', '.join(uploaded)}")

    # 更新状态
    state["known_skills"] = known
    state["last_sync"] = time.time()
    save_state(state)

    if not has_new:
        return  # 静默

    print(f"📦 [{AGENT}] 共享技能同步 ({time.strftime('%H:%M:%S')}):")
    print("\n".join(reports))
    print()


if __name__ == "__main__":
    main()
