# LONG 股票 × Meme 榜单监控

监控 [app.long.xyz/tokens](https://app.long.xyz/tokens) 上的 meme 代币，
按其**底池股票**（numeraire，如 NVDA / TSLA / SPCX）分组展示：一个真实股票卡片下面是
配对在该股票上的 meme 列表，实时刷新。

## 运行

```bash
pip install curl_cffi fastapi uvicorn
./start.sh          # 前台运行,或: python3 -m uvicorn backend.server:app --host 0.0.0.0 --port 8610
```

一键后台启动(依赖缺失自动补装,含 stop/restart/status/log 子命令):

```bash
ln -sf /SSD/projects/longscan/longscan.sh ~/.local/bin/longscan   # 已装好
longscan            # = longscan start;重启 longscan restart;看日志 longscan log
```

浏览器打开 `http://<本机IP>:8610/`

## 架构

- `backend/server.py` — FastAPI 服务。数据全量落 SQLite（`backend/board.db`）：
  - **增量轮询**（15 秒）：按创建时间倒序分页拉取，遇到整页已入库就停，新 meme 秒级发现；
  - **全量刷新**（30 分钟）：完整翻页更新全部行情（平台总量 2 万+，全量约 3 分钟）；
  - GraphQL 单页 limit>100 时 `pool_market_data` 会失效，固定 100/页，用 curl_cffi 模拟 Chrome 指纹过 Cloudflare。
- **连续上涨监控**（`RisingWatcher`，K 线右侧趋势判定）：watch list（24h 量前 600 + 每底池
  市值前 10 + 近 30 分钟新币，上限 1000）每 30 秒按地址 `where:_in` 批量采样价格做**粗筛**
  （顺带写回 `assets`，是看板涨跌闪光的数据源）；最近一步在涨的候选再拉交易所
  **1 分钟 K 线**（`getBars`，symbol=`地址:4663`，窗口 40 根）做**右侧趋势确认**——
  ① 最后 3 根（或更多）已收盘 K 线逐根收高、净涨幅 ≥2%；② 收盘价**突破此前 30 根 K 线
  的最高 high**（且此前至少 5 根 K 线构成前高基准）。未破前高的反弹、新币头几根 K 线
  都**不通知**（右侧交易：只做趋势确立后的行情，`RIGHT_*` 常量可调）。判定窗口来自
  交易所 K 线，**与服务启动时间无关**。事件经 `/api/board` 的 `rising` 字段下发，前端
  toast 📈 通知（🔔 可开系统通知+提示音），同时落 `rising_hist` 表（**永久存档不清理**，
  当日基线/🕘 历史面板/今日计数都按"今天"过滤，互不影响）经 `rising_hist` 字段下发；
  **信号分层**（09-07 全天 398 条复盘标定）：① 高换手博弈盘（24h 量/市值 ≥3，
  `RISE_CHURN_RATIO`）只归档不上看板不推 TG；② 同币**当日重复信号**照常上看板但不推
  TG（复盘胜率 19% vs 首次 28%）；③ 连涨窗口涨幅 ≥8%（`RISE_CHASE_GAIN`）保留推送但加
  **⚠️ 追高极顶警示**（此档复盘中位 -19.6%）；④ 市值 ≥$2M（`RISE_HI_MCAP`）加
  **🔴 高置信**标识（此档中位 -3.5%，远稳于小市值）；`tags` 列留档、`pushed` 列标记
  是否推送过 TG；**信号后跟踪**（`signal_followup` 表）：每条信号 +1/3/5/15/30/60
  分钟价格由采样线程顺带回填（近 60 分钟信号地址钉在 watchlist 保证不断档），
  供后续复盘最优离场窗口；**每日零点快照**（`day_snapshot` 表）：过零点立即对当日有信号
  的币拉实时价存档（10 分钟窗口内执行、幂等、失败重试 3 次），每日复盘的"截至 24 点"
  终点价从此精确，DailyDigest 也优先采用快照市值；
  **每日零点（北京时间）`DailyDigest` 推送胜率复盘到 TG 频道**：每个币取「首发信号时
  市值 vs 当日结束时现价市值」算涨跌（不能用首末信号对比——基线过滤已保证末次必高于
  首发恒 100%），汇总整体胜率/平均涨跌/最佳最差 TOP3/高频币；`digests` 表防重发，
  服务跨零点重启自动补发漏掉的总结；**TG 每小时心跳**（今日信号/推送/拦截计数）+
  采样异常 10 分钟节流告警，监控失效不再静默。
- **过滤规则**（默认开启，`backend/server.py` 顶部可调）：
  - `OFFICIAL_ONLY`：只显示官方配对资产（`official.json` 白名单，58 只股票/ETF + ETH/USDG）的 meme，fake asset 一律隐藏；
  - `MIN_MCAP=20000`：市值低于 $20K 不显示；
  - 24h 成交量为 0 不显示（新币上线通知不受此限制）。
- `web/static/index.html` — 多列看板：一列一个底池，条目只显示市值+成交量（悬停看更多），
  超过 40 个 meme 的列点"展开全部"懒加载；新 meme 右下角悬浮通知；10 秒自动刷新；
  行情变动行内动效（参考币安：行闪光 + ▲▼ 箭头浮入淡出 + 市值染方向色保持到下次变动，
  样例对比页 `flash-demo.html`）；条目右侧附 OKX 交易快捷链接；标题栏 🔕🔔 旁 **🕘
  通知历史面板**（下拉展开，今日连涨通知全留存、显示触发时刻，条数角标，点外部收起）。
- `backend/official.json` / `backend/stocks.json` — 官方底池注册表（从前端 JS 提取），`backend/extract_stocks.py` 可重新提取。
- API：`/api/board`（榜单，每列市值前 40）、`/api/memes?n=<底池地址>`（某底池全部 meme）。

## Telegram 频道推送

连涨事件（连续 3 分钟上涨）可同步推送到 TG 频道，文案与前端 toast 一致：

1. 找 [@BotFather](https://t.me/BotFather) 创建 bot，拿到 `bot_token`；
2. 建频道（公开频道填 `@username`，私有频道填 `-100…` 的 chat_id），把 bot 拉进频道并
   **设为管理员**（允许发消息）；
3. `cp tg/tg.example.json tg/tg.json`，填入 `bot_token` / `chat_id`（也可用环境变量
   `TG_BOT_TOKEN` / `TG_CHAT_ID` / `TG_PROXY`）；
4. 重启服务，频道会先收到一条 🟢 启动确认，之后每次连涨事件即时推送。

未配置 `tg/tg.json` 时推送自动关闭，不影响看板；发送失败自动重试 3 次（429 按
`retry_after` 退避），状态见 `/api/board` 的 `tg_ok` / `tg_error` 字段。
`api.telegram.org` 不可直连的网络环境，在 `tg/tg.json` 加 `"proxy": "http://127.0.0.1:7890"`。

## 参考

UI 设计参考了 [Topledger/trending-pairs-dashboard](https://github.com/Topledger/trending-pairs-dashboard)
（分组卡片 + 实时刷新 + 暗色榜单）与 [coinpulse](https://github.com/adrianhajdin/coinpulse)。
