#!/usr/bin/env bash
# LONG 榜单监控一键启动
# 用法: longscan [start|stop|restart|status|log]   (默认 start)
# 安装: ln -sf /SSD/projects/longscan/longscan.sh ~/.local/bin/longscan
set -euo pipefail

# 经软链调用时 $0 指向软链,readlink -f 回到真实脚本所在目录
SELF=$(readlink -f "$0")
cd "$(dirname "$SELF")"

PORT=8610
PID_FILE=.server.pid
LOG=server.log
HEALTH="http://127.0.0.1:$PORT/api/board?n=1"

lan_ip() { hostname -I 2>/dev/null | awk '{print $1}'; }

# 取运行中 pid:优先 pid 文件,失效则 pgrep 兜底(能抓到手工启动的旧实例)
running_pid() {
    local pid
    if [[ -f $PID_FILE ]]; then
        pid=$(cat "$PID_FILE" 2>/dev/null || true)
        if [[ -n $pid ]] && kill -0 "$pid" 2>/dev/null; then
            echo "$pid"; return 0
        fi
    fi
    pgrep -f "uvicorn.*server:app.*--port $PORT" | head -1 || true
}

start() {
    local pid
    pid=$(running_pid)
    if [[ -n $pid ]]; then
        echo "✅ 已在运行 (pid $pid) → http://$(lan_ip):$PORT/"
        return 0
    fi
    # 依赖检查,缺什么补什么(装到用户目录,无需 sudo)
    if ! python3 -c "import fastapi, uvicorn, curl_cffi" 2>/dev/null; then
        echo "⚙️  安装缺失依赖 fastapi uvicorn curl_cffi ..."
        python3 -m pip install --user fastapi uvicorn curl_cffi
    fi
    echo "🚀 启动 LONG 榜单监控 (端口 $PORT)..."
    nohup python3 -m uvicorn backend.server:app --host 0.0.0.0 --port "$PORT" >>"$LOG" 2>&1 &
    echo $! >"$PID_FILE"
    # 健康检查:最多等 30 秒
    local i
    for i in $(seq 1 30); do
        sleep 1
        if ! kill -0 "$(cat "$PID_FILE")" 2>/dev/null; then
            echo "❌ 进程启动失败,最近日志:"
            tail -5 "$LOG"
            rm -f "$PID_FILE"
            return 1
        fi
        if curl -sf -o /dev/null "$HEALTH" --max-time 3; then
            echo "✅ 就绪 → http://$(lan_ip):$PORT/  (本机 http://127.0.0.1:$PORT/)"
            return 0
        fi
    done
    echo "⚠️  30 秒仍未就绪,跟踪日志: longscan log"
    return 1
}

stop() {
    local pid
    pid=$(running_pid)
    if [[ -z $pid ]]; then
        echo "ℹ️  未在运行"
        rm -f "$PID_FILE"
        return 0
    fi
    echo "🛑 停止 pid $pid ..."
    kill "$pid" 2>/dev/null || true
    local i
    for i in $(seq 1 10); do
        if ! kill -0 "$pid" 2>/dev/null; then
            rm -f "$PID_FILE"; echo "✅ 已停止"; return 0
        fi
        sleep 1
    done
    kill -9 "$pid" 2>/dev/null || true
    rm -f "$PID_FILE"
    echo "✅ 已停止 (kill -9)"
}

status() {
    local pid
    pid=$(running_pid)
    if [[ -n $pid ]]; then
        echo "🟢 运行中 pid $pid  端口 $PORT  (已运行 $(ps -o etime= -p "$pid" | tr -d ' '))"
        if curl -sf -o /dev/null "$HEALTH" --max-time 3; then
            echo "   健康检查 OK → http://$(lan_ip):$PORT/"
        else
            echo "   ⚠️ 健康检查失败,看日志: longscan log"
        fi
    else
        echo "⚪ 未运行,执行 longscan 启动"
    fi
}

case "${1:-start}" in
    start)   start ;;
    stop)    stop ;;
    restart) stop; start ;;
    status)  status ;;
    log)     tail -n 100 -f "$LOG" ;;
    *) echo "用法: longscan [start|stop|restart|status|log]   (默认 start)"; exit 1 ;;
esac
