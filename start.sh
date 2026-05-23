#!/bin/bash
# Hermes Team Hub - 一键启动脚本
# 用法: ./start.sh [port] [token]

PORT=${1:-8765}
TOKEN=${2:-$(openssl rand -hex 16)}
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

echo "🚀 Hermes Team Hub 启动中..."
echo "   端口: $PORT"
echo "   Token: $TOKEN"

cd "$SCRIPT_DIR"
python3 src/relay_server.py --port "$PORT" --token "$TOKEN"
