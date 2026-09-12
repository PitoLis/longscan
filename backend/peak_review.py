#!/usr/bin/env python3
"""峰值复盘:信号触发 → 当天结束(或现在)的完整走势分析。

用法:
  python3 peak_review.py            # 今天(北京时间)的推送信号,统计到现在
  python3 peak_review.py 2026-09-08 # 指定日期的推送信号,统计到当天24点

数据源:平台 getBars 1分钟K线(可回放任意历史区间),无需自建存储;
对每条信号输出:最高点/到峰时间/峰值收益/终点收益,汇总实际 vs 理论(全部按最高点卖)。
"""
import sqlite3, sys, time, statistics
from datetime import datetime, timezone, timedelta
from pathlib import Path
from curl_cffi import requests as cffi_requests

CST = timezone(timedelta(hours=8))
DB = "file:" + str(Path(__file__).resolve().parent / "board.db") + "?mode=ro"
GRAPHQL = "https://api.long.xyz/v1/graphql"
CHAIN = 4663
STAKE = 10.0          # 每笔假设仓位(U),与跟单账本口径一致
Q = ('query Bars($symbol: String!, $from: Int!, $to: Int!) '
     '{ getBars(symbol: $symbol, from: $from, to: $to, resolution: "1") { s t o h c } }')

day = sys.argv[1] if len(sys.argv) > 1 else datetime.now(CST).strftime("%Y-%m-%d")
d0 = datetime.strptime(day, "%Y-%m-%d").replace(tzinfo=CST).timestamp()
d1 = d0 + 86400
end = min(d1, time.time())          # 往日统计到24点,今天统计到现在

db = sqlite3.connect(DB, uri=True)
sigs = db.execute("""SELECT address,symbol,ts,price,tags FROM rising_hist
    WHERE ts>=? AND ts<? AND pushed=1 AND price IS NOT NULL ORDER BY ts""",
    (d0, min(d1, time.time()))).fetchall()
print(f"{day} 推送信号 {len(sigs)} 条,统计区间: 信号 → "
      f"{'24:00' if end == d1 else '现在 ' + datetime.now(CST).strftime('%H:%M')}\n")

sess = cffi_requests.Session(impersonate="chrome")
trades = []
for addr, sym, ts, price, tags in sigs:
    try:
        r = sess.post(GRAPHQL, json={"query": Q, "variables": {
            "symbol": f"{addr}:{CHAIN}", "from": int(ts) - 5, "to": int(end)}}, timeout=20)
        d = r.json()["data"]["getBars"]
    except Exception as e:
        print(f"  {sym:12s} 拉取失败: {str(e)[:60]}")
        continue
    if not d or d.get("s") != "ok" or not d.get("t"):
        print(f"  {sym:12s} 无K线数据")
        continue
    bars = [(t, h, c) for t, h, c in zip(d["t"], d["h"], d["c"]) if c is not None]
    if not bars:
        continue
    peak_t, peak_h, _ = max(bars, key=lambda b: b[1])
    last_c = bars[-1][2]
    peak_ret = (peak_h - price) / price
    last_ret = (last_c - price) / price
    trades.append((sym, tags or "", ts, peak_ret, (peak_t - ts) / 60, last_ret))
    time.sleep(0.8)

if not trades:
    sys.exit("无可用数据")
print(f"{'时间':<6}{'代币':<13}{'标记':<12}{'峰值收益':>9} {'到峰':>7} {'终点收益':>9}")
for sym, tags, ts, pr, pmin, lr in trades:
    print(f"{datetime.fromtimestamp(ts).strftime('%H:%M'):<6}{sym[:12]:<13}{tags[:11]:<12}"
          f"{pr*100:>8.1f}% {pmin:>5.0f}m {lr*100:>+8.1f}%")

peak_pnl = sum(STAKE * t[3] for t in trades)
last_pnl = sum(STAKE * t[5] for t in trades)
cap = len(trades) * STAKE
print(f"\n== {STAKE:.0f}U/笔 等额跟单,共 {len(trades)} 笔(投入 {cap:.0f}U) ==")
print(f"理论最大(全部按最高点卖出): {peak_pnl:+.2f}U ({peak_pnl/cap*100:+.2f}%)")
print(f"持有到终点(24点/现在):      {last_pnl:+.2f}U ({last_pnl/cap*100:+.2f}%)")
print(f"捕获率(终点/峰值):          {last_pnl/peak_pnl*100:.0f}%" if peak_pnl > 0 else "")
mins = [t[4] for t in trades]
if mins:
    ms = sorted(mins)
    print(f"到峰时间: 平均 {statistics.mean(mins):.0f} 分钟 | 中位 {statistics.median(mins):.0f} 分钟 | "
          f"P25 {ms[len(ms)//4]:.0f}m | P75 {ms[3*len(ms)//4]:.0f}m")
