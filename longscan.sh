#!/usr/bin/env bash
# LONG 榜单监控一键启动
# 用法: longscan [start|stop|restart|status|log|tg on|tg off]   (默认 start;--help 看完整说明)
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

# ---------- TG 频道推送开关 ----------
# 服务只在启动时读取一次配置(server.py Notifier.__init__),开关切换后需 restart 生效
TG_CONF=tg/tg.json
TG_OFF=tg/tg.json.disabled

tg_summary() {
    local f=$TG_CONF
    [[ -f $f ]] || f=$TG_OFF
    [[ -f $f ]] || return 0
    python3 - "$f" <<'PY'
import json, sys
cfg = json.load(open(sys.argv[1]))
t = str(cfg.get("bot_token", ""))
bid, _, sec = t.partition(":")
print(f"   bot_token: {bid}:{sec[:3]}…  chat_id: {cfg.get('chat_id', '(缺)')}  proxy: {cfg.get('proxy') or '直连'}")
PY
}

tg_status() {
    if [[ -f $TG_CONF ]]; then
        echo "🟢 TG 频道推送:开启 ($TG_CONF)"
    elif [[ -f $TG_OFF ]]; then
        echo "⚪ TG 频道推送:关闭 (配置在 $TG_OFF,执行 longscan tg on 开启)"
    else
        echo "⚪ TG 频道推送:未配置 (模板: tg/tg.example.json)"
    fi
    tg_summary
    [[ -n ${TG_BOT_TOKEN:-} && -n ${TG_CHAT_ID:-} ]] && \
        echo "   ⚠️ 环境变量 TG_BOT_TOKEN/TG_CHAT_ID 已设置,忽略文件开关仍会推送"
}

tg_on() {
    if [[ -f $TG_CONF ]]; then
        echo "✅ 已是开启状态"
    elif [[ -f $TG_OFF ]]; then
        mv "$TG_OFF" "$TG_CONF"
        echo "✅ TG 推送已开启 ($TG_OFF → $TG_CONF)"
    else
        echo "❌ 未找到配置,先从 tg/tg.example.json 复制一份 $TG_CONF 填好 token"
        return 1
    fi
    tg_summary
    restart_hint
}

tg_off() {
    if [[ -f $TG_CONF ]]; then
        mv "$TG_CONF" "$TG_OFF"
        echo "✅ TG 推送已关闭 ($TG_CONF → $TG_OFF)"
        tg_summary
    elif [[ -f $TG_OFF ]]; then
        echo "✅ 已是关闭状态"
    else
        echo "ℹ️  本来就未配置"
    fi
    [[ -n ${TG_BOT_TOKEN:-} && -n ${TG_CHAT_ID:-} ]] && \
        echo "   ⚠️ 环境变量 TG_BOT_TOKEN/TG_CHAT_ID 仍在,推送不会真正关闭"
    restart_hint
}

restart_hint() {
    local pid
    pid=$(running_pid)
    [[ -n $pid ]] && echo "ℹ️  服务启动时读取配置,执行 longscan restart 生效"
}

usage() {
    cat <<'EOF'
LONG 榜单监控

用法: longscan <命令>   (无参数 = start)

服务:
  start        启动服务(缺失依赖自动安装,后台运行)
  stop         停止服务
  restart      重启服务
  status       运行状态
  log          跟踪日志(Ctrl-C 退出)

TG 频道推送:
  tg           查看推送状态(token 打码显示)
  tg on        开启推送 (tg/tg.json.disabled → tg/tg.json)
  tg off       关闭推送 (tg/tg.json → tg/tg.json.disabled)

说明:
  · 服务只在启动时读取一次 TG 配置,开关后需 longscan restart 生效
  · 配置模板: tg/tg.example.json,或用环境变量 TG_BOT_TOKEN/TG_CHAT_ID/TG_PROXY
EOF
}

case "${1:-start}" in
    start)   start ;;
    stop)    stop ;;
    restart) stop; start ;;
    status)  status ;;
    log)     tail -n 100 -f "$LOG" ;;
    tg)
        case "${2:-}" in
            "")  tg_status ;;
            on)  tg_on ;;
            off) tg_off ;;
            *)   echo "用法: longscan tg [on|off]"; exit 1 ;;
        esac ;;
    -h|--help|help) usage ;;
    *) usage; exit 1 ;;
esac
