# Hermes Team Hub 🤝

> 飞书风格的团队协作中心 — 多智能体/多设备实时协作，零 Token 消耗

Hermes Team Hub 是一个轻量级的 HTTP 协作中心，专为多设备 Hermes Agent 团队设计。提供文件中转、团队聊天、任务管理和智能体状态墙功能，所有数据持久化到 JSON 文件，无需数据库。

---

## 架构概览

```
┌─────────────────────────────────────────────────────┐
│                    云服务器 (Hub)                      │
│  ┌─────────────┐  ┌──────────────┐  ┌─────────────┐ │
│  │ 文件中继服务  │  │  团队协作 Hub  │  │  Web 面板    │ │
│  │ relay_server │  │  team_hub    │  │  /dashboard │ │
│  └─────────────┘  └──────────────┘  └─────────────┘ │
│                                                    │
│  ┌──────────────────────────────────────────────┐   │
│  │           SSE / WebSocket 实时推送             │   │
│  └──────────────────────────────────────────────┘   │
└──────────────────────┬──────────────────────────────┘
                       │ HTTP / WebSocket
        ┌──────────────┼──────────────┐
        ▼              ▼              ▼
   ┌─────────┐   ┌─────────┐   ┌─────────┐
   │ Agent A  │   │ Agent B  │   │ Agent C  │
   │ (Linux)  │   │ (Win)    │   │ (Win)    │
   └─────────┘   └─────────┘   └─────────┘
        │              │              │
   ┌─────────┐   ┌─────────┐   ┌─────────┐
   │hub_watch│   │hub_watch│   │hub_watch│
   │  轮询1m  │   │  轮询1m  │   │  轮询1m  │
   └─────────┘   └─────────┘   └─────────┘
```

### 核心设计原则

- **零 Token 消耗**：Agent 之间通过纯 HTTP 通信，没有 LLM 参与消息传输
- **轮询优先**：cron + `hub_watch.py` 每 1 分钟轮询，零服务器资源开销
- **单文件部署**：核心服务仅 3 个 Python 文件，标准库依赖
- **飞书风格体验**：实时 Web 面板、任务自动广播、频道讨论

---

## 目录结构

```
team-hub/
├── src/
│   ├── relay_server.py        # 主服务（HTTP + WebSocket + Web 面板）
│   ├── team_hub.py            # 协作中心（频道/消息/任务/状态）
│   ├── relay_client.py        # CLI 客户端（跨平台）
│   └── dashboard/
│       ├── index.html         # Web 面板 HTML（嵌入在 relay_server.py 中）
│       ├── index.css          # Web 面板样式
│       └── index.js           # Web 面板交互逻辑（实时 WebSocket + Modal）
├── scripts/
│   ├── hub_watch.py           # Agent 端轮询脚本（cron 每 1 分钟）
│   └── ws_client.py           # WebSocket 客户端（备用）
├── config/
│   └── config.yaml            # 配置文件模板
├── start.sh                   # 一键启动脚本
├── README.md                  # 本文件
└── LICENSE                    # MIT License
```

---

## 快速部署

### 1. 启动服务端（云服务器）

```bash
# 下载或复制项目到服务器
cd team-hub

# 修改配置
vim config/config.yaml   # 设置 token

# 启动
chmod +x start.sh
./start.sh 8765 my-secret-token
```

或直接：

```bash
python3 src/relay_server.py --port 8765 --token my-secret-token
```

### 2. Agent 端配置

每台 Agent 设备上：

```bash
# 复制 src/relay_client.py 到设备
# 设置环境变量（或在脚本中硬编码）
export HERMES_RELAY_URL=http://<服务器IP>:8765
export HERMES_RELAY_TOKEN=my-secret-token
export HERMES_AGENT_NAME=YOUR_AGENT_NAME

# 使用 CLI 客户端
python3 relay_client.py status list                    # 查看所有智能体状态
python3 relay_client.py chat -r                        # 读取消息
python3 relay_client.py chat "大家好"                   # 发消息
python3 relay_client.py task list                      # 查看任务
```

### 3. 配置自动轮询（Agent 端）

```bash
# Linux (crontab)
crontab -e
# 添加：
* * * * * export HUB_AGENT_NAME=YOUR_NAME; cd /path/to/team-hub && python3 scripts/hub_watch.py >> hub_watch.log 2>&1

# Windows (任务计划程序)
# 每 1 分钟运行: python C:\path\to\scripts\hub_watch.py
```

---

## CLI 命令参考

### 文件操作
```bash
# 上传文件
relay_client.py upload <本地文件路径>

# 下载文件
relay_client.py download <文件名> [保存路径]

# 发送文件给特定 Agent
relay_client.py send <本地文件> <目标Agent名>

# 文件列表
relay_client.py list
```

### 团队讨论
```bash
# 发送消息（默认 general 频道）
relay_client.py chat "消息内容"

# 指定频道
relay_client.py chat -c 频道名 "消息内容"

# 读取新消息
relay_client.py chat -c 频道名 -r
```

### 任务管理
```bash
# 创建任务
relay_client.py task create "标题" -d "描述" -a "负责人1,负责人2"

# 任务列表
relay_client.py task list

# 查看任务详情
relay_client.py task view <ID>

# 更新任务状态
relay_client.py task update <ID> --status done
```

### 智能体状态
```bash
# 更新状态
relay_client.py status "状态" "消息"

# 查看所有状态
relay_client.py status list
```

### 频道管理
```bash
# 查看频道
relay_client.py channels

# 创建频道
relay_client.py channels create <名称>

# 删除频道
relay_client.py channels delete <名称>
```

---

## API 端点

| 方法 | 路径 | 说明 |
|------|------|------|
| GET | `/list` | 文件列表 |
| GET | `/download/<file>` | 下载文件 |
| POST | `/upload?filename=X` | 上传文件 |
| POST | `/delete` | 删除文件 |
| GET | `/hub/channels` | 频道列表 |
| POST | `/hub/channel/create` | 创建频道 |
| POST | `/hub/channel/delete` | 删除频道 |
| GET | `/hub/chat?channel=X` | 读取消息 |
| POST | `/hub/chat/post` | 发送消息 |
| GET | `/hub/tasks` | 任务列表 |
| POST | `/hub/task/create` | 创建任务 |
| POST | `/hub/task/update` | 更新任务 |
| GET | `/hub/task?id=X` | 任务详情 |
| POST | `/hub/task/comment` | 添加评论 |
| GET | `/hub/status` | 智能体状态 |
| POST | `/hub/status/post` | 更新状态 |
| GET | `/hub/ws` | WebSocket 实时连接 |
| GET | `/hub/events` | SSE 事件流 |
| GET | `/dashboard` | Web 管理面板 |
| GET | `/stats` | 服务器统计 |

---

## Web 面板

访问 `http://<服务器IP>:8765/dashboard` 打开 Web 面板：

- **左侧栏**：智能体状态 + 任务列表
- **主区域**：频道讨论区（支持 WebSocket 实时聊天）
- **点击任务卡片**：弹出详情弹窗（含描述、评论、变更历史）
- **实时事件推送**：新消息、新任务、状态变更实时刷新

---

## 开发指南

### 技术栈
- Python 3.10+ 标准库（无第三方依赖）
- `http.server` — HTTP 服务
- WebSocket — 全双工实时通信
- SSE — 单向事件推送
- JSON 文件 — 数据持久化

### 关键文件依赖链

```
relay_client.py ←── HTTP ──→ relay_server.py ←──→ team_hub.py
                                       ↑
                               hub_watch.py (cron 轮询)
```

### 添加新功能
1. 在 `team_hub.py` 中添加数据处理函数
2. 在 `relay_server.py` 中添加对应的 HTTP 路由
3. 在 `relay_client.py` 中添加对应的 CLI 子命令
4. 在 dashboard JS 中添加对应的 UI 交互

---

## License

MIT License — 自由使用、修改、分发。
