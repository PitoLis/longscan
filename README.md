# LONG 股票 × Meme 榜单监控

监控 [app.long.xyz/tokens](https://app.long.xyz/tokens) 的 meme 代币,按**底池股票**(NVDA / TSLA / SPCX…)分组展示,实时刷新。

## 快速开始

```bash
pip install curl_cffi fastapi uvicorn
./start.sh          # 前台运行
```

推荐一键脚本(支持 start / stop / restart / status / log,`longscan --help` 查看全部命令):

```bash
ln -sf /SSD/projects/longscan/longscan.sh ~/.local/bin/longscan   # 已装好
longscan            # 启动;longscan log 看日志
```

浏览器打开 `http://<本机IP>:8610/`

## 架构

- **backend/server.py** — FastAPI 服务,数据全量落 SQLite(`backend/board.db`)
  - 增量轮询 15s(新 meme 秒级发现)+ 全量刷新 30min;curl_cffi 模拟浏览器指纹过 Cloudflare
  - **连涨信号**(RisingWatcher):watchlist 每 30s 粗筛 → 1 分钟 K 线右侧趋势确认(逐根收高 + 突破前高),只做趋势确立后的行情;阈值在 `server.py` 顶部 `RIGHT_*` / `RISE_*` 常量可调
  - **信号分层**:高换手博弈盘只归档、当日重复信号不推 TG、追高加 ⚠️ 警示、市值 ≥$2M 加 🔴 高置信标识
  - **复盘数据链**:信号后 +1/3/5/15/30/60 分钟价格回填、零点快照、`rising_hist` 永久存档;每日零点 DailyDigest 向频道推胜率复盘,TG 每小时心跳
- **web/static/index.html** — 多列看板:10s 自动刷新、行情变动行内闪光、新币 toast 通知(可开系统通知+提示音)、🕘 通知历史面板、OKX 快捷链接
- **web/static/longx.html** — LongX Vaults 独立板块:`LongXPoller` 每 5 分钟采样 vault 池子(TVL/supply/cap/池深度,落 `longx_hist` 永久存档),折线图看资金进出与平台扩缩容,24h 增减标注
- **过滤规则**(`server.py` 顶部可调):只显示官方配对资产(`official.json` 白名单)、市值 ≥ $20K、24h 成交量非零
- **API**:`/api/board`(榜单)、`/api/memes?n=<底池地址>`(某底池全部 meme)

## Telegram 频道推送

1. 找 [@BotFather](https://t.me/BotFather) 创建 bot,拿到 `bot_token`;
2. 建频道(私有频道用 `-100…` 的 chat_id),把 bot 拉进频道并**设为管理员**;
3. `cp tg/tg.example.json tg/tg.json`,填入 `bot_token` / `chat_id`(也可用环境变量 `TG_BOT_TOKEN` / `TG_CHAT_ID` / `TG_PROXY`);
4. `longscan tg on && longscan restart`,频道收到 🟢 启动确认即成功。

日常开关:`longscan tg`(看状态)/ `longscan tg on|off`,切换后需 `longscan restart` 生效。
`api.telegram.org` 不可直连时,在配置里加 `"proxy": "http://127.0.0.1:7890"`;发送失败自动重试,状态见 `/api/board` 的 `tg_ok` / `tg_error` 字段。

## 参考

UI 参考 [Topledger/trending-pairs-dashboard](https://github.com/Topledger/trending-pairs-dashboard) 与 [coinpulse](https://github.com/adrianhajdin/coinpulse)。
