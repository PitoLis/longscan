#!/usr/bin/env bash
# 启动 LONG 榜单监控(端口 8610)
cd "$(dirname "$0")"
exec python3 -m uvicorn backend.server:app --host 0.0.0.0 --port 8610
