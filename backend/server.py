"""LONG (app.long.xyz) 股票→meme 榜单监控服务.

数据全部落在 SQLite(board.db):
- 增量轮询:每 POLL_INTERVAL 秒按创建时间倒序分页拉取,遇到整页都已入库就停(水位),
  保证新 meme 秒级发现,又不会每次都打 30+ 页。
- 全量刷新:每 FULL_REFRESH 秒完整走一遍分页,更新全部 meme 的行情/阶段。
- GraphQL 单页 limit>100 时 market_data 会整体失效,固定 100/页。
"""
import html
import json
import os
import queue
import re
import sqlite3
import threading
import time
import traceback
from datetime import datetime, timedelta, timezone
from pathlib import Path

from curl_cffi import requests as cffi_requests
from fastapi import FastAPI
from fastapi.responses import JSONResponse, Response
from fastapi.staticfiles import StaticFiles

BASE = Path(__file__).parent
ROOT = BASE.parent
DB_PATH = BASE / "board.db"
GRAPHQL = "https://api.long.xyz/v1/graphql"
POLL_INTERVAL = 15      # 秒,增量轮询间隔(新 meme 秒级发现)
FULL_REFRESH = 3600     # 秒,全量刷新间隔(平台2万+,全量约10分钟)
PAGE = 100              # 每页条数(上限100,超过 market_data 会失效)
MAX_PAGES = 400         # 全量刷新安全上限(4万,覆盖平台全部历史)
COL_PRESET = 40         # /api/board 每列默认只带市值前 N,其余按需 /api/memes 懒加载
MIN_MCAP = 20000        # 市值低于此值不显示
OFFICIAL_ONLY = True    # 只显示官方配对资产(official.json 白名单)的 meme
TOKEN_URL = "https://app.long.xyz/tokens/"

# ---- 连续上涨监控:采样粗筛 + K线右侧趋势确认(连涨+破前高) → 通知 ----
RISE_SAMPLE = 30        # 秒,采样间隔(粗筛候选 + 回写看板价格)
RISE_KLINE = 3          # 连续收高的1分钟K线根数(3根=3分钟,以交易所K线为准)
RISE_KLINE_FETCH = 40   # 确认时拉的K线根数(供突破回看)
RISE_CAND_MAX = 25      # 每轮最多确认候选数,防K线接口爆量
RISE_KLINE_GAP = 60     # 秒,同一地址两次拉K线的最小间隔(节流)
RISE_MIN_GAIN = 0.02    # 窗口内最小累计涨幅,过滤毛刺
RIGHT_LOOKBACK = 30     # 突破回看:连涨段收盘须高于此前30根K线的最高high
RIGHT_MIN_BASE = 5      # 连涨段之前至少几根K线,才够构成"前高"基准
RISE_COOLDOWN = 900     # 秒,同一 meme 两次通知的最小间隔
RISE_WATCH_MAX = 1000   # watch list 上限(24h量前600 + 每底池市值前10 + 近30分钟新币)
RISE_KEEP = 600         # 秒,事件保留时长(前端 10 秒一轮,足够轮询到)
# ---- 信号分层(09-07 全天 398 条复盘标定) ----
RISE_CHURN_RATIO = 3.0  # 24h量/市值 ≥ 此值:高换手博弈盘(41条仅1条活),归档不推送不看板
RISE_CHASE_GAIN = 0.08  # 连涨窗口涨幅 ≥ 此值:追高极顶警示(中位 -19.6%),保留推送但加 ⚠️
RISE_HI_MCAP = 2_000_000  # 市值 ≥ 此值:🔴高置信标识(中位 -3.5%,远好于小市值 -16.8%)
FOLLOWUP_STEPS = ((1, 60), (3, 180), (5, 300), (15, 900), (30, 1800), (60, 3600))  # 信号后跟踪(分钟,秒)
FOLLOWUP_PIN = 3600     # 秒,信号后 60 分钟内地址钉在 watchlist,保证跟价不断档
CHAIN_ID = 4663         # Robinhood Chain(meme 都在此链,K线 symbol=地址:链ID)

# ---- TG 推送:连涨事件同步发 Telegram 频道,配置见 tg/tg.json(缺省关闭) ----
TG_CONF = ROOT / "tg" / "tg.json"
TG_API = "https://api.telegram.org"
CST = timezone(timedelta(hours=8))    # 北京时间,信号计数按此划分当天

# ---- 新闻监控:华尔街见闻快讯流(单平台覆盖全部底池,无需 key) ----
LIVES_URL = "https://api-one.wallstcn.com/apiv1/content/lives"
NEWS_INTERVAL = 300     # 秒,快讯轮询间隔(单页200条已覆盖20h+)
NEWS_PAGE = 200         # 单页条数
NEWS_KEEP = 48 * 3600   # 新闻保留窗口
NEWS_SKIP = {"SGOV", "USDG", "AI"}             # 无独立新闻语义的资产,不匹配
NEWS_ALIAS = {"NVDAx3L": "NVDA"}               # 复用底池的新闻
# zh_names 的中文名带"ETF/公司"后缀,快讯里不会那样写,整体替换:
NEWS_KW_OVERRIDE = {
    "GLD": ["黄金"], "SLV": ["白银"], "USO": ["原油"], "SPY": ["标普"],
    "QQQ": ["纳指", "纳斯达克"], "XLK": ["科技股"], "QUBT": ["量子计算"],
    "USAR": ["美国稀土", "稀土"], "SKHY": ["海力士"], "SNDK": ["闪迪", "SanDisk"],
    "GME": ["游戏驿站", "GameStop"], "MSTR": ["MicroStrategy", "MSTR"],
    "DJT": ["特朗普", "Trump"], "SPCX": ["SpaceX", "星链"],
    "ETH": ["以太坊", "Ethereum", "ETH"],
}
NEWS_KW_EXTRA = {                              # 中文名之外补充英文别名
    "TSLA": ["Tesla"], "NVDA": ["Nvidia"], "META": ["Meta"], "COIN": ["Coinbase"],
    "CRCL": ["Circle", "USDC"], "RDDT": ["Reddit"], "RBLX": ["Roblox"],
    "TTWO": ["Take-Two", "GTA"], "FIG": ["Figma"], "CRWV": ["CoreWeave"],
    "MU": ["Micron"], "HIMS": ["Hims"], "BE": ["Bloom"], "SNAP": ["Snapchat"],
    "NFLX": ["Netflix"], "ORCL": ["Oracle"], "PLTR": ["Palantir"], "IBM": ["IBM"],
    "INTC": ["Intel"], "DELL": ["Dell"], "COST": ["Costco"], "LULU": ["Lululemon"],
    "GOOGL": ["Google"], "AMZN": ["Amazon"], "AAPL": ["Apple"], "MSFT": ["Microsoft"],
    "AMD": ["AMD"], "BA": ["Boeing"], "BABA": ["Alibaba"], "BB": ["BlackBerry"],
    "NET": ["Cloudflare"], "NU": ["Nubank"], "SOFI": ["SoFi"], "AMC": ["AMC"],
    "MRNA": ["Moderna"], "TSM": ["TSMC"], "CCL": ["Carnival"], "UPS": ["UPS"],
}

# ---- LongX vaults 监控:杠杆金库代币池子规模采样(app.long.xyz/longx) ----
LONGX_URL = "https://api.long.xyz/v1/longx/vaults"
LONGX_KEY = os.environ.get("LONGX_API_KEY") or \
    "lxyz_49534dc2febae30294149790a8152f44bf915ebbe0332213"  # 平台公开客户端 key(打包在其前端 JS)
LONGX_INTERVAL = 300     # 秒,快照采样间隔
LONGX_POINTS = 1500      # 每 vault 图表历史点数上限,超出按步长抽稀

QUERY = """
query Board($limit: Int, $offset: Int) {
  assets: Asset(order_by: {asset_creation_timestamp: desc}, limit: $limit, offset: $offset) {
    asset_address
    asset_numeraire_address
    asset_creation_timestamp
    asset_current_pool
    asset_categories
    auction_pool {
      pool_config_starting_time
      pool_config_ending_time
      pool_current_fdv_usd
      pool_current_sale_progress_percentage
      pool_market_data { marketCap price priceChange24 volumeUSD24 }
      quote_token { token_address token_symbol token_name token_image_public_url }
      base_token { token_symbol token_name token_image_public_url }
    }
    graduation_pool {
      pool_market_data { marketCap price priceChange24 volumeUSD24 }
      quote_token { token_address token_symbol token_name token_image_public_url }
      base_token { token_symbol token_name token_image_public_url }
    }
  }
}
"""


def fnum(v):
    try:
        return float(v) if v is not None else None
    except (TypeError, ValueError):
        return None


_tls = threading.local()


def http():
    """每线程独立的 curl_cffi Session(模块级 Session 非线程安全,共享会卡死)。"""
    s = getattr(_tls, "s", None)
    if s is None:
        s = cffi_requests.Session(impersonate="chrome")
        _tls.s = s
    return s


def init_db():
    con = sqlite3.connect(DB_PATH, check_same_thread=False)  # 轮询线程与 API 线程共用,靠 db_lock 串行化
    con.execute("PRAGMA journal_mode=WAL")
    con.execute("""CREATE TABLE IF NOT EXISTS assets(
        address TEXT PRIMARY KEY, numeraire TEXT, symbol TEXT, name TEXT, image TEXT,
        stage TEXT, created_at TEXT, price REAL, change24 REAL, mcap REAL,
        volume24 REAL, fdv_usd REAL, progress REAL, quote_json TEXT, updated_at REAL)""")
    for ddl in ("ALTER TABLE assets ADD COLUMN start_time TEXT",
                "ALTER TABLE assets ADD COLUMN end_time TEXT"):
        try:
            con.execute(ddl)
        except sqlite3.OperationalError:
            pass                                   # 列已存在
    con.execute("""CREATE TABLE IF NOT EXISTS news(
        symbol TEXT, url TEXT, title TEXT, source TEXT,
        ts REAL, published_at TEXT, fetched_at REAL, PRIMARY KEY(symbol,url))""")
    con.execute("CREATE INDEX IF NOT EXISTS idx_news_symbol ON news(symbol, ts)")
    con.execute("""CREATE TABLE IF NOT EXISTS rising_hist(     -- 连涨通知历史,永久存档(复盘/回测数据源)
        ts REAL, address TEXT, symbol TEXT, name TEXT, image TEXT, stock TEXT,
        url TEXT, price REAL, gain REAL, span REAL, market_cap REAL,
        volume_24h REAL, fired_at TEXT, PRIMARY KEY(address, ts))""")
    try:
        con.execute("ALTER TABLE rising_hist ADD COLUMN brk REAL")   # 超前高幅度,供每日总结
    except sqlite3.OperationalError:
        pass                                   # 列已存在
    try:
        con.execute("ALTER TABLE rising_hist ADD COLUMN pushed INTEGER DEFAULT 1")  # 1=已推送TG
    except sqlite3.OperationalError:
        pass
    try:
        con.execute("ALTER TABLE rising_hist ADD COLUMN tags TEXT DEFAULT ''")     # churn/repeat/chase/hi
    except sqlite3.OperationalError:
        pass
    con.execute("""CREATE TABLE IF NOT EXISTS signal_followup(  -- 信号后跟踪:+1/3/5/15/30/60分钟价
        address TEXT, ts REAL, p1 REAL, p3 REAL, p5 REAL, p15 REAL, p30 REAL, p60 REAL,
        done INTEGER DEFAULT 0, PRIMARY KEY(address, ts))""")
    con.execute("""CREATE TABLE IF NOT EXISTS day_snapshot(  -- 每日零点快照:当日有信号币的精确午夜价
        day TEXT, address TEXT, price REAL, mcap REAL, ts REAL,
        PRIMARY KEY(day, address))""")
    con.execute("CREATE INDEX IF NOT EXISTS idx_rising_hist_ts ON rising_hist(ts)")
    con.execute("""CREATE TABLE IF NOT EXISTS digests(       -- 已发送的每日总结,防重发/补发依据
        day TEXT PRIMARY KEY, sent_at REAL)""")
    con.execute("""CREATE TABLE IF NOT EXISTS longx_hist(  -- LongX vault 快照:池子增减趋势(永久保留)
        ts REAL, address TEXT, ticker TEXT, name TEXT,
        nav REAL, supply REAL, tvl REAL, cap REAL, pool_liquidity REAL,
        paused INTEGER, unwound INTEGER, PRIMARY KEY(address, ts))""")
    con.execute("CREATE INDEX IF NOT EXISTS idx_longx_ts ON longx_hist(ts)")
    con.commit()
    return con


def parse_asset(a):
    pool = a["auction_pool"] if a["asset_current_pool"] == "auction" else a["graduation_pool"]
    if not pool:
        pool = a["graduation_pool"] or a["auction_pool"]
    md = (pool or {}).get("pool_market_data") or {}
    tok = ((pool or {}).get("base_token") or {})
    auc = a["auction_pool"] or {}
    return (
        a["asset_address"],
        (a["asset_numeraire_address"] or "").lower() or "0x" + "0" * 40,
        tok.get("token_symbol") or "???",
        tok.get("token_name") or "Unknown",
        tok.get("token_image_public_url"),
        a["asset_current_pool"],
        a["asset_creation_timestamp"],
        fnum(md.get("price")),
        fnum(md.get("priceChange24")),
        fnum(md.get("marketCap")),
        fnum(md.get("volumeUSD24")),
        (lambda v: v / 1e18 if v is not None else None)(
            fnum(auc.get("pool_current_fdv_usd"))),
        fnum(auc.get("pool_current_sale_progress_percentage")),
        json.dumps(((pool or {}).get("quote_token") or None)),
        time.time(),
        auc.get("pool_config_starting_time"),
        auc.get("pool_config_ending_time"),
    )


class Poller(threading.Thread):
    def __init__(self):
        super().__init__(daemon=True)
        self.db_lock = threading.Lock()
        self.con = init_db()
        self.state = {"ok": False, "error": None, "updated_at": None,
                      "count": 0, "last_full": None}
        self.lock = threading.Lock()
        self.registry = json.loads((BASE / "stocks.json").read_text())
        self.zh = {}
        zh_path = BASE / "zh_names.json"
        if zh_path.exists():
            self.zh = json.loads(zh_path.read_text())
        self.official = None
        self.official_sym = {}
        if OFFICIAL_ONLY and (BASE / "official.json").exists():
            entries = json.loads((BASE / "official.json").read_text())
            self.official = {x["address"].lower() for x in entries}
            self.official_sym = {x["address"].lower(): x["symbol"] for x in entries}
            self.official_meta = {x["address"].lower(): x for x in entries}
        self._full_at = 0.0

    def gql_page(self, offset):
        r = http().post(
            GRAPHQL,
            json={"query": QUERY, "variables": {"limit": PAGE, "offset": offset}},
            timeout=30)
        return r.json()["data"]["assets"]

    def upsert(self, rows):
        self.con.executemany(
            """INSERT INTO assets(address,numeraire,symbol,name,image,stage,created_at,
               price,change24,mcap,volume24,fdv_usd,progress,quote_json,updated_at,start_time,end_time)
               VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
               ON CONFLICT(address) DO UPDATE SET
               numeraire=excluded.numeraire, symbol=excluded.symbol, name=excluded.name,
               image=excluded.image, stage=excluded.stage,
               price=COALESCE(excluded.price, assets.price),
               change24=COALESCE(excluded.change24, assets.change24),
               mcap=COALESCE(excluded.mcap, assets.mcap),
               volume24=COALESCE(excluded.volume24, assets.volume24),
               fdv_usd=COALESCE(excluded.fdv_usd, assets.fdv_usd),
               progress=COALESCE(excluded.progress, assets.progress),
               quote_json=COALESCE(excluded.quote_json, assets.quote_json),
               start_time=COALESCE(excluded.start_time, assets.start_time),
               end_time=COALESCE(excluded.end_time, assets.end_time),
               updated_at=excluded.updated_at""",
            rows)
        self.con.commit()

    def fetch(self, full):
        with self.db_lock:
            known = None if full else {r[0] for r in self.con.execute("SELECT address FROM assets")}
        rows, new_pages = [], 0
        for page in range(MAX_PAGES):
            assets = self.gql_page(page * PAGE)
            if not assets:
                break
            parsed = [parse_asset(a) for a in assets]
            with self.db_lock:
                self.upsert(parsed)
            if known is not None:
                fresh = [p for p in parsed if p[0] not in known]
                new_pages += 1
                if not fresh:          # 整页都是旧数据 => 已追上水位,停止翻页
                    break
            time.sleep(0.15 if full else 0.4)   # 全量时提速,增量时保守
        with self.db_lock:
            cnt = self.con.execute("SELECT count(*) FROM assets").fetchone()[0]
        self.state.update(ok=True, error=None,
                          updated_at=time.strftime("%Y-%m-%d %H:%M:%S"),
                          count=cnt,
                          last_full=time.strftime("%Y-%m-%d %H:%M:%S") if full else self.state["last_full"])

    def run(self):
        while True:
            try:
                self.fetch(full=False)
            except Exception as e:
                with self.lock:
                    self.state["ok"] = False
                    self.state["error"] = str(e)
                traceback.print_exc()
            time.sleep(POLL_INTERVAL)

    def run_full(self):
        while True:
            try:
                self.fetch(full=True)   # 冷启动时立即全量回填
            except Exception as e:
                traceback.print_exc()
            time.sleep(FULL_REFRESH)

    # ---- 从库里组榜单 ----
    @staticmethod
    def derive_stage(raw_stage, created_at):
        """平台实际生命周期:创建即开盘,无毕业数据。
        new=1小时内上线(观察窗,不受市值/成交过滤) / open=已开盘交易 / graduation=保留"""
        if raw_stage == "graduation":
            return "graduation"
        try:
            from datetime import datetime, timezone
            age = datetime.now(timezone.utc) - datetime.fromisoformat(created_at).astimezone(timezone.utc)
            if age.total_seconds() < 3600:
                return "new"
        except (ValueError, TypeError):
            pass
        return "open"

    def board(self):
        reg = {x["address"].lower(): x for x in self.registry}
        for num, meta in getattr(self, "official_meta", {}).items():
            reg[num] = meta     # official.json 最全(含 ETF),优先于旧 stocks.json
        with self.db_lock:
            rows = self.con.execute("""SELECT address,numeraire,symbol,name,image,stage,created_at,
                                       price,change24,mcap,volume24,progress,start_time FROM assets""").fetchall()
            qrows = self.con.execute(
                """SELECT numeraire, quote_json FROM assets WHERE quote_json!='null'
                   GROUP BY numeraire""").fetchall()
        quotes = {num: json.loads(qj) for num, qj in qrows}
        groups = {}
        raw_latest = []
        for addr, num, sym, name, img, raw_stage, created, price, chg, mcap, vol, prog, start_t in rows:
            stage = self.derive_stage(raw_stage, created)
            if self.official is None or num in self.official:
                raw_latest.append((created, num, {
                    "address": addr, "symbol": sym, "name": name, "image": img,
                    "price": price, "price_change_24h": chg, "market_cap": mcap,
                    "volume_24h": vol, "progress": prog, "stage": stage,
                    "created_at": created, "url": TOKEN_URL + addr,
                }))
            if self.official is not None and num not in self.official:
                continue                       # 非官方配对资产(fake asset)不显示
            if stage != "new" and ((mcap or 0) < MIN_MCAP or not (vol or 0)):
                continue                       # 市值<20K 或零成交不显示(1小时内新币除外)
            groups.setdefault(num, []).append({
                "address": addr, "symbol": sym, "name": name, "image": img,
                "price": price, "price_change_24h": chg, "market_cap": mcap,
                "volume_24h": vol, "progress": prog, "stage": stage,
                "created_at": created, "url": TOKEN_URL + addr,
            })
        stocks = []
        zero = "0x" + "0" * 40
        for num, memes in groups.items():
            info = reg.get(num)
            if not info:   # 静态注册表没有的,用 pool quote_token 兜底(如 GLD 黄金信托)
                qt = quotes.get(num) or {}
                if (qt.get("token_address") or "").lower() == num:
                    info = {"symbol": qt.get("token_symbol"), "name": qt.get("token_name"),
                            "kind": "asset", "image": qt.get("token_image_public_url")}
            info = info or {}
            sym = info.get("symbol")
            zh = self.zh.get(sym) or {}
            memes.sort(key=lambda m: m["market_cap"] or 0, reverse=True)
            preset = [m for m in memes if m["stage"] == "new"] + \
                     [m for m in memes if m["stage"] != "new"][:COL_PRESET]
            stocks.append({
                "numeraire": num,
                "symbol": sym or ("ETH" if num == zero else num[:8] + "…"),
                "name": info.get("name") or ("Ether" if num == zero else "Unknown"),
                "zh": zh.get("zh") or "",
                "biz": zh.get("biz") or "",
                "image": info.get("image"),
                "kind": info.get("kind") or ("native" if num == zero else "unknown"),
                "meme_count": len(memes),
                "hidden_count": max(0, len(memes) - len(preset)),
                "total_volume_24h": sum(m["volume_24h"] or 0 for m in memes),
                "top_market_cap": memes[0]["market_cap"] if memes else 0,
                "memes": preset,
            })
        stocks.sort(key=lambda s: (s["kind"] == "stock", s["total_volume_24h"]), reverse=True)
        # 全站最新 meme(新上线通知用;不受市值/成交过滤,但仅官方底池)
        sym_by_num = {s["numeraire"]: s["symbol"] for s in stocks}
        sym_by_num.update(self.official_sym)
        latest = sorted(({**m, "stock": sym_by_num.get(num)} for _, num, m in raw_latest),
                        key=lambda m: m["created_at"], reverse=True)[:20]
        with self.lock:
            state = dict(self.state)
        try:                                # 每列附带最新5条新闻(news_poller 在模块后部实例化)
            news_by = news_poller.top()
            state.update(news_poller.state)
        except NameError:
            news_by = {}
        try:                                # 连续上涨事件(rising_watcher 同样在模块后部)
            rising = rising_watcher.recent()
            state.update(rising_watcher.state)
        except NameError:
            rising = []
        try:                                # 当天通知历史(前端 🔘 历史面板)
            rising_hist = rising_watcher.history()
        except NameError:
            rising_hist = []
        try:                                # TG 推送状态(tg_pusher 同样在模块后部)
            state.update(tg_pusher.state)
        except NameError:
            pass
        for s in stocks:
            s["news"] = news_by.get(NEWS_ALIAS.get(s["symbol"], s["symbol"]), [])[:5]
        for s in stocks:                    # 预取图标的下载任务
            queue_icon(s["numeraire"], s.get("image") or "")
            for m in s["memes"]:
                queue_icon(m["address"], m.get("image") or "")
        return {"stocks": stocks, "latest": latest, "rising": rising,
                "rising_hist": rising_hist, **state}

    def memes_for(self, numeraire):
        numeraire = numeraire.lower()
        with self.db_lock:
            rows = self.con.execute(
                """SELECT address,symbol,name,image,stage,created_at,
                   price,change24,mcap,volume24,progress,start_time FROM assets
                   WHERE numeraire=? ORDER BY mcap DESC""", (numeraire,)).fetchall()
        out = []
        for r in rows:
            stage = self.derive_stage(r[4], r[5])
            if self.official is not None and numeraire not in self.official:
                continue
            if stage != "new" and ((r[8] or 0) < MIN_MCAP or not (r[9] or 0)):
                continue
            out.append({
                "address": r[0], "symbol": r[1], "name": r[2], "image": r[3],
                "stage": stage, "created_at": r[5], "price": r[6],
                "price_change_24h": r[7], "market_cap": r[8],
                "volume_24h": r[9], "progress": r[10], "url": TOKEN_URL + r[0],
            })
        for m in out:
            queue_icon(m["address"], m.get("image") or "")
        return out


poller = Poller()
poller.start()
full_thread = threading.Thread(target=poller.run_full, daemon=True, name="full-refresh")
full_thread.start()


class NewsPoller(threading.Thread):
    """华尔街见闻快讯流 → 按底池关键词匹配入库。

    搜索接口按相关度排序(旧文靠前),快讯流按时间排,故以快讯为主源:
    每 NEWS_INTERVAL 秒拉一页(覆盖20h+),冷启动时翻页回填48h。
    """

    def __init__(self, owner):
        super().__init__(daemon=True, name="news")
        self.owner = owner                    # 共用 Poller 的 con/db_lock/zh/official_meta
        self.state = {"news_at": None, "news_count": 0, "news_error": None}
        kw = {}
        for meta in getattr(owner, "official_meta", {}).values():
            sym = meta["symbol"]
            if sym in NEWS_SKIP or sym in NEWS_ALIAS:
                continue
            kws = list(NEWS_KW_OVERRIDE.get(sym) or
                       [owner.zh.get(sym, {}).get("zh") or meta["name"]])
            kws += NEWS_KW_EXTRA.get(sym, [])
            kw[sym] = {k for k in kws if k}
        self.matchers = [(sym, [self._compile(k) for k in kws])
                         for sym, kws in kw.items()]

    @staticmethod
    def _compile(k):
        # ASCII 词加边界(wETH 不算 ETH),中文直接子串
        if k.isascii():
            return re.compile(r"(?<![A-Za-z0-9])" + re.escape(k) + r"(?![A-Za-z0-9])",
                              re.IGNORECASE)
        return re.compile(re.escape(k))

    def match(self, text):
        return [sym for sym, pats in self.matchers if any(p.search(text) for p in pats)]

    def fetch_page(self, cursor=None):
        params = {"channel": "global-channel", "limit": NEWS_PAGE}
        if cursor:
            params["cursor"] = cursor
        r = http().get(LIVES_URL, params=params, timeout=20)
        return r.json()["data"]

    def ingest(self, items):
        now = time.time()
        rows = []
        for it in items:
            ts = fnum(it.get("display_time")) or 0
            if not ts or now - ts > NEWS_KEEP:
                continue
            ct = re.sub(r"<[^>]+>", "", it.get("content_text") or "")
            title = (re.sub(r"<[^>]+>", "", it.get("title") or "") or ct)[:120].strip()
            url = (it.get("uri") or "").strip()
            if not (title and url):
                continue
            m = re.search(r"（([^（）]{2,12})）\s*$", ct)     # 快讯尾部常有（来源名）
            source = m.group(1) if m and not re.search(r"[，。：；、！？%时点]", m.group(1)) \
                else "华尔街见闻"                             # 更正备注等不是来源
            body = (title + "\n" + ct).replace("黄金周", "")  # "黄金周"不是黄金行情
            for sym in self.match(body):
                rows.append((sym, url, title, source, ts,
                             datetime.fromtimestamp(ts, timezone.utc).isoformat(), now))
        if rows:
            with self.owner.db_lock:
                self.owner.con.executemany(
                    """INSERT INTO news(symbol,url,title,source,ts,published_at,fetched_at)
                       VALUES(?,?,?,?,?,?,?) ON CONFLICT(symbol,url) DO NOTHING""", rows)
                self.owner.con.commit()
        return len(rows)

    def run(self):
        deep = True                          # 冷启动翻页回填48h
        while True:
            try:
                cursor, oldest = None, time.time()
                for _ in range(4 if deep else 1):
                    data = self.fetch_page(cursor)
                    items = data.get("items") or []
                    if not items:
                        break
                    self.ingest(items)
                    oldest = min(fnum(i.get("display_time")) or 0 for i in items)
                    cursor = data.get("next_cursor")
                    if not cursor or time.time() - oldest > NEWS_KEEP:
                        break
                    time.sleep(1.0)
                deep = False
                with self.owner.db_lock:
                    self.owner.con.execute(
                        "DELETE FROM news WHERE fetched_at < ?",
                        (time.time() - NEWS_KEEP - 3600,))
                    cnt = self.owner.con.execute("SELECT count(*) FROM news").fetchone()[0]
                    self.owner.con.commit()
                self.state.update(news_at=time.strftime("%Y-%m-%d %H:%M:%S"),
                                  news_count=cnt, news_error=None)
            except Exception as e:
                self.state["news_error"] = str(e)
                traceback.print_exc()
            time.sleep(NEWS_INTERVAL)

    def top(self):
        """按 symbol 分组的最新新闻(已按时间倒序)。"""
        with self.owner.db_lock:
            rows = self.owner.con.execute(
                """SELECT symbol,title,url,source,published_at FROM news
                   WHERE ts >= ? ORDER BY ts DESC LIMIT 3000""",
                (time.time() - NEWS_KEEP,)).fetchall()
        by = {}
        for sym, title, url, source, pub in rows:
            by.setdefault(sym, []).append(
                {"title": title, "url": url, "source": source, "published_at": pub})
        return by


news_poller = NewsPoller(poller)
news_poller.start()


class LongXPoller(threading.Thread):
    """LongX vaults 快照采样:每 LONGX_INTERVAL 秒拉一次 REST 落 longx_hist。

    池子增减信号:supply(mint/redeem 资金进出)、cap(平台扩缩容)、
    pool_liquidity(交易池深度);TVL=NAV×supply,cap 顶满时基本不动。
    """

    def __init__(self, owner):
        super().__init__(daemon=True, name="longx")
        self.owner = owner                    # 共用 Poller 的 con/db_lock
        self.state = {"longx_at": None, "longx_count": 0, "longx_error": None}

    @staticmethod
    def _metrics(v):
        nav = int(v.get("navPerToken") or 0) / 1e6
        supply = int(v.get("totalSupply") or 0) / 1e18
        return (v["address"], v.get("ticker") or v.get("symbol") or "?", v.get("name") or "",
                nav, supply, nav * supply, int(v.get("equityCapUsdg") or 0) / 1e6,
                int((v.get("pool") or {}).get("liquidity") or 0),
                int(bool(v.get("paused"))), int(bool(v.get("unwound"))))

    def run(self):
        while True:
            try:
                r = http().get(LONGX_URL, headers={"x-api-key": LONGX_KEY}, timeout=20)
                rows = [self._metrics(v) for v in (r.json().get("vaults") or [])]
                if rows:
                    with self.owner.db_lock:
                        self.owner.con.executemany(
                            """INSERT INTO longx_hist(ts,address,ticker,name,nav,supply,tvl,cap,
                               pool_liquidity,paused,unwound) VALUES(?,?,?,?,?,?,?,?,?,?,?)
                               ON CONFLICT(address, ts) DO NOTHING""",
                            [(time.time(),) + row for row in rows])
                        self.owner.con.commit()
                self.state.update(longx_at=time.strftime("%Y-%m-%d %H:%M:%S"),
                                  longx_count=len(rows), longx_error=None)
            except Exception as e:
                self.state["longx_error"] = str(e)
                traceback.print_exc()
            time.sleep(LONGX_INTERVAL)

    def snapshot(self):
        """当前 vault 状态 + 24h 增减 + 图表历史(/api/longx 数据源)。"""
        out = {"longx_at": self.state.get("longx_at"),
               "longx_error": self.state.get("longx_error"),
               "vaults": [], "history": {}, "since": None}
        with self.owner.db_lock:
            con = self.owner.con
            since = con.execute("SELECT min(ts) FROM longx_hist").fetchone()[0]
            if since is None:
                return out                    # 尚无采样
            out["since"] = since
            latest = con.execute("""
                SELECT address,ticker,name,nav,supply,tvl,cap,pool_liquidity,paused,unwound
                FROM longx_hist h
                WHERE ts = (SELECT max(ts) FROM longx_hist x WHERE x.address = h.address)
            """).fetchall()
            target = time.time() - 86400
            for a, tick, name, nav, sup, tvl, cap, liq, pa, un in latest:
                v = {"address": a, "ticker": tick, "name": name, "nav": nav,
                     "supply": sup, "tvl": tvl, "cap": cap,
                     "cap_pct": (tvl / cap * 100) if cap else None,
                     "pool_liquidity": liq, "paused": bool(pa), "unwound": bool(un),
                     "d24": None}
                ref = con.execute(
                    """SELECT tvl,supply,cap,pool_liquidity FROM longx_hist
                       WHERE address=? ORDER BY abs(? - ts) LIMIT 1""",
                    (a, target)).fetchone()
                if ref:
                    v["d24"] = {"tvl": tvl - ref[0], "supply": sup - ref[1],
                                "cap": cap - ref[2], "pool_liquidity": liq - ref[3]}
                out["vaults"].append(v)
                rows = con.execute(
                    """SELECT ts,tvl,supply,cap,pool_liquidity FROM longx_hist
                       WHERE address=? ORDER BY ts""", (a,)).fetchall()
                if len(rows) > LONGX_POINTS:
                    step = -(-len(rows) // LONGX_POINTS)   # ceil 除法
                    thinned = rows[::step]
                    if thinned[-1] is not rows[-1]:
                        thinned.append(rows[-1])
                    rows = thinned
                out["history"][a] = [list(r) for r in rows]
        out["vaults"].sort(key=lambda v: -(v["tvl"] or 0))
        return out


longx_poller = LongXPoller(poller)
longx_poller.start()


# ---- 连续上涨监控 ----
RISE_QUERY = """
query Watch($addrs: [String!]) {
  assets: Asset(where: {asset_address: {_in: $addrs}}, limit: 100) {
    asset_address
    asset_current_pool
    auction_pool { pool_market_data { marketCap price volumeUSD24 } }
    graduation_pool { pool_market_data { marketCap price volumeUSD24 } }
  }
}
"""

BARS_QUERY = """
query Bars($symbol: String!, $from: Int!, $to: Int!) {
  getBars(symbol: $symbol, from: $from, to: $to, resolution: "1") { s t o h c }
}
"""


def rising_kline_run(bars):
    """从尾部回溯,取收盘价逐根严格上涨的连续K线段。bars=[(ts, open, high, close)…]升序。"""
    if len(bars) < 2:
        return []
    k = len(bars) - 1
    while k > 0 and bars[k][3] > bars[k - 1][3]:
        k -= 1
    return bars[k:]


def is_right_side(bars):
    """右侧趋势确认(规则1+2):通过返回 (连涨段, 超前高幅度),不通过返回 None。

    规则1 连涨结构:结尾 ≥RISE_KLINE 根已收盘K线逐根收高,净涨幅 ≥RISE_MIN_GAIN;
    规则2 突破新高:段尾收盘价 > 此前 RIGHT_LOOKBACK 根K线(连涨段之前)的最高 high,
    且连涨段之前至少有 RIGHT_MIN_BASE 根K线构成前高基准——
    未破前高的只是反弹(左侧信号),突破前高趋势才算确立(右侧进场位)。
    """
    run = rising_kline_run(bars)
    if len(run) < RISE_KLINE:
        return None
    o0, c_last = run[0][1], run[-1][3]
    if not o0 or (c_last - o0) / o0 < RISE_MIN_GAIN:
        return None
    base = bars[:len(bars) - len(run)][-RIGHT_LOOKBACK:]
    if len(base) < RIGHT_MIN_BASE:
        return None                    # 前面K线太少,构不成"前高"基准(新币晚几分钟再判)
    prior_high = max(b[2] for b in base)
    if c_last <= prior_high:
        return None                    # 收盘仍在前高之下:反弹,不是右侧趋势
    return run, c_last / prior_high - 1


class RisingWatcher(threading.Thread):
    """采样粗筛 + K线右侧趋势确认:连涨 ≥RISE_KLINE 根且突破前高才通知。

    主轮询只覆盖最新创建的 meme,其余行情最长滞后一小时,做不了分钟级检测;
    故用 where:_in 批量采样做粗筛(顺带写回 assets 让看板价格实时),
    候选再逐个拉交易所 K 线确认(is_right_side)—— 判定窗口来自市场数据,
    与服务启动时间无关;只有右侧趋势(连涨+破前高)才发通知,
    未破前高的反弹、新币头几根K线都不发。
    """

    def __init__(self, owner):
        super().__init__(daemon=True, name="rising")
        self.owner = owner                    # 共用 Poller 的 con/db_lock/official_sym
        self.hist = {}                        # addr -> [(ts, price)…],升序(粗筛用)
        self.kline_at = {}                    # addr -> 上次拉K线时间(节流)
        self.fired_at = {}                    # addr -> 上次通知时间(冷却用)
        self.events = []                      # 通知事件,/api/board 随榜单下发
        self.filtered = 0                     # 当日基线过滤掉的无效信号数(进程累计)
        self.churned = 0                      # 高换手拦截数(进程累计,只归档不展示)
        self.state = {"rise_at": None, "rise_watch": 0, "rise_error": None,
                      "rise_filtered": 0, "rise_churn": 0}

    def watchlist(self):
        """采样对象:官方底池且达到展示门槛,按 24h 量排序 + 近 30 分钟新币。"""
        official = self.owner.official
        out, seen = [], set()

        def take(rows, gate):
            for addr, num, sym, name, img, created, mcap, vol in rows:
                if len(out) >= RISE_WATCH_MAX:
                    return
                if addr in seen:                 # 已收录:跳过继续,不能 return(会截断后续来源)
                    continue
                if official is not None and num not in official:
                    continue
                if gate == "fresh":           # 新币:创建 30 分钟内即可采样(尚无成交也算)
                    try:
                        age = datetime.now(timezone.utc) - \
                            datetime.fromisoformat(created).astimezone(timezone.utc)
                        if age.total_seconds() > 1800:
                            continue
                    except (ValueError, TypeError):
                        continue
                elif gate == "vol" and ((mcap or 0) < MIN_MCAP or not (vol or 0)):
                    continue                  # 常规门槛与看板一致:市值≥20K 且有成交
                # gate=="mcap":SQL 已保证市值≥20K,零成交的列头部也要采样
                seen.add(addr)
                out.append({"address": addr, "numeraire": num, "symbol": sym,
                            "name": name, "image": img,
                            "market_cap": mcap, "volume_24h": vol})

        with self.owner.db_lock:
            hot = self.owner.con.execute(
                """SELECT address,numeraire,symbol,name,image,created_at,mcap,volume24
                   FROM assets WHERE mcap>=? AND volume24>0
                   ORDER BY volume24 DESC LIMIT 600""", (MIN_MCAP,)).fetchall()
            # 每底池市值前10:看板每列头部不一定有成交,只按量采样会漏掉整列
            tops = self.owner.con.execute(
                """SELECT address,numeraire,symbol,name,image,created_at,mcap,volume24 FROM (
                     SELECT *, row_number() OVER (PARTITION BY numeraire ORDER BY mcap DESC) rn
                     FROM assets WHERE mcap>=?) WHERE rn<=10""", (MIN_MCAP,)).fetchall()
            fresh = self.owner.con.execute(
                """SELECT address,numeraire,symbol,name,image,created_at,mcap,volume24
                   FROM assets ORDER BY created_at DESC LIMIT 300""").fetchall()
        # 近 60 分钟发过信号的地址钉在 watchlist 最前:signal_followup 跟价不断档
        # (哪怕已跌出量/市值/新币三个来源;数量有限,优先占位不受上限挤占)
        pin_from = time.time() - FOLLOWUP_PIN
        try:
            with self.owner.db_lock:
                pinned = self.owner.con.execute(
                    "SELECT DISTINCT address FROM rising_hist WHERE ts>=?",
                    (pin_from,)).fetchall()
            if pinned:
                marks = ",".join("?" * len(pinned))
                with self.owner.db_lock:
                    prows = self.owner.con.execute(
                        f"""SELECT address,numeraire,symbol,name,image,created_at,
                           mcap,volume24 FROM assets WHERE address IN ({marks})""",
                        [r[0] for r in pinned]).fetchall()
                take(prows, "pin")             # gate 未匹配任何分支 → 只做去重和收录
        except Exception:
            traceback.print_exc()
        take(hot, "vol")
        take(tops, "mcap")
        take(fresh, "fresh")
        return out

    def fetch_prices(self, addrs):
        out = {}
        for i in range(0, len(addrs), PAGE):
            try:                               # 单页失败只丢该页,不废整轮采样/回写
                r = http().post(GRAPHQL, json={
                    "query": RISE_QUERY,
                    "variables": {"addrs": addrs[i:i + PAGE]}}, timeout=10)
                for a in r.json()["data"]["assets"]:
                    pool = a["auction_pool"] if a["asset_current_pool"] == "auction" \
                        else a["graduation_pool"]
                    pool = pool or a["graduation_pool"] or a["auction_pool"]
                    md = (pool or {}).get("pool_market_data") or {}
                    out[a["asset_address"]] = (
                        fnum(md.get("price")), fnum(md.get("marketCap")),
                        fnum(md.get("volumeUSD24")))
            except Exception:
                traceback.print_exc()
            time.sleep(0.2)
        return out

    def fetch_klines(self, addr):
        """最近 RISE_KLINE_FETCH 根已收盘的 1 分钟K线 [(ts, open, high, close)…],失败返回空。"""
        now = int(time.time())
        try:
            d = http().post(GRAPHQL, json={
                "query": BARS_QUERY,
                "variables": {"symbol": f"{addr}:{CHAIN_ID}",
                              "from": now - RISE_KLINE_FETCH * 60 - 5, "to": now}},
                timeout=15).json()["data"]["getBars"]
        except Exception:
            return []
        if not d or d.get("s") != "ok":
            return []
        return [(d["t"][i], d["o"][i], d["h"][i], d["c"][i]) for i in range(len(d["t"]))
                if d["c"][i] is not None and now - d["t"][i] >= 60]  # 排除未收盘K线

    def _day_first_price(self, addr):
        """当天(北京时间)该地址首个有效信号的触发价;落在 rising_hist,重启不丢。"""
        today0 = datetime.now(CST).replace(
            hour=0, minute=0, second=0, microsecond=0).timestamp()
        try:
            with self.owner.db_lock:
                row = self.owner.con.execute(
                    """SELECT price FROM rising_hist
                       WHERE address=? AND ts>=? AND price IS NOT NULL
                       ORDER BY ts ASC LIMIT 1""", (addr, today0)).fetchone()
        except Exception:
            traceback.print_exc()
            return None                      # 基线查不到时宁可放行,不误杀
        return row[0] if row else None

    def _day_count(self, addr):
        """北京时间当天该地址已归档的信号条数(调用方自行 +1 得本次序号)。"""
        today0 = datetime.now(CST).replace(
            hour=0, minute=0, second=0, microsecond=0).timestamp()
        try:
            with self.owner.db_lock:
                return self.owner.con.execute(
                    "SELECT count(*) FROM rising_hist WHERE address=? AND ts>=?",
                    (addr, today0)).fetchone()[0]
        except Exception:
            traceback.print_exc()
            return 0

    def fire(self, addr, meta, run, gain, span, price, mcap, vol, now, brk=0.0):
        first = self._day_first_price(addr)  # 当日基线:首个信号价
        if first is not None and price is not None and price <= first:
            # 无效信号:触发价不高于当日首个信号(区间内反复拉回),不发送不落库;
            # 不计冷却,一旦真正突破基线可立即再触发
            self.filtered += 1
            self.state["rise_filtered"] = self.filtered
            return
        self.fired_at[addr] = now
        if not meta:
            return
        mcap_v = mcap if mcap is not None else meta["market_cap"]
        vol_v = vol if vol is not None else meta["volume_24h"]
        nth = self._day_count(addr) + 1        # 本次为当日第 nth 次信号(落库前计数)
        churn = bool(mcap_v and vol_v and vol_v / mcap_v >= RISE_CHURN_RATIO)
        chase = gain >= RISE_CHASE_GAIN
        hi = (mcap_v or 0) >= RISE_HI_MCAP
        tags = ",".join(t for t, on in (("churn", churn), ("repeat", nth >= 2),
                                        ("chase", chase), ("hi", hi)) if on)
        ev = {
            "address": addr, "symbol": meta["symbol"], "name": meta["name"],
            "image": meta["image"], "stock": self.owner.official_sym.get(
                meta["numeraire"], meta["numeraire"][:8] + "…"),
            "url": TOKEN_URL + addr, "price": price, "gain": gain,
            "span": round(span), "samples": len(run), "brk": brk, "nth": nth,
            "tags": tags, "pushed": 0 if (churn or nth >= 2) else 1,
            "market_cap": mcap_v, "volume_24h": vol_v,
            "fired_at": datetime.fromtimestamp(now, timezone.utc).isoformat(),
            "ts": now,
        }
        self._save_hist(ev, now)               # 全部归档(含被拦截的,复盘可核验过滤效果)
        self._track_followup(addr, now)
        if churn:                              # 高换手博弈盘:只归档,不上看板不推TG
            self.churned += 1
            self.state["rise_churn"] = self.churned
            return
        self.events.append(ev)
        self.events = [e for e in self.events if now - e["ts"] <= RISE_KEEP]
        self.events.sort(key=lambda e: e["ts"], reverse=True)
        if ev["pushed"]:                       # 仅当日首次推送 TG(复盘:重复信号胜率19% vs 首次28%)
            try:                                # TG 频道推送(tg_pusher 在模块后部实例化)
                tg_pusher.rise(ev)
            except NameError:
                pass                            # 模块加载早期,下一事件再推
            except Exception:
                traceback.print_exc()           # 推送异常不影响采样主流程

    def _save_hist(self, ev, now):
        """通知历史落库,跨刷新/跨设备;永久存档,不再清理(复盘/回测数据源)。"""
        try:
            with self.owner.db_lock:
                self.owner.con.execute(
                    """INSERT INTO rising_hist(ts,address,symbol,name,image,stock,url,
                       price,gain,span,market_cap,volume_24h,fired_at,brk,pushed,tags)
                       VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                       ON CONFLICT(address,ts) DO NOTHING""",
                    (ev["ts"], ev["address"], ev["symbol"], ev["name"], ev["image"],
                     ev["stock"], ev["url"], ev["price"], ev["gain"], ev["span"],
                     ev["market_cap"], ev["volume_24h"], ev["fired_at"],
                     ev.get("brk"), ev["pushed"], ev["tags"]))
                self.owner.con.commit()
        except Exception:
            traceback.print_exc()           # 历史落库失败不影响通知主流程

    def _track_followup(self, addr, now):
        """登记信号后跟踪:cycle 采样时回填 +1/3/5/15/30/60 分钟价格。"""
        try:
            with self.owner.db_lock:
                self.owner.con.execute(
                    "INSERT OR IGNORE INTO signal_followup(address,ts) VALUES(?,?)",
                    (addr, now))
                self.owner.con.commit()
        except Exception:
            traceback.print_exc()

    def _fill_followup(self, prices, now):
        """用本轮采样价回填信号后轨迹;窗口未到的列保持 NULL,过 2 小时未跟到的收尾。"""
        try:
            with self.owner.db_lock:
                pend = self.owner.con.execute(
                    """SELECT address,ts,p1,p3,p5,p15,p30,p60 FROM signal_followup
                       WHERE done=0""").fetchall()
        except Exception:
            traceback.print_exc()
            return
        for addr, ts, *ps in pend:
            if now - ts > 7200:              # 跟踪窗口已过(如中途停服),收尾防死挂
                try:
                    with self.owner.db_lock:
                        self.owner.con.execute(
                            "UPDATE signal_followup SET done=1 WHERE address=? AND ts=?",
                            (addr, ts))
                        self.owner.con.commit()
                except Exception:
                    pass
                continue
            price = prices.get(addr, (None,))[0]
            if price is None:
                continue
            sets, last = [], False
            for (m_, _sec), cur in zip(FOLLOWUP_STEPS, ps):
                if cur is None and now - ts >= _sec:
                    sets.append(f"p{m_}=?")
                    last = m_ == FOLLOWUP_STEPS[-1][0]
            if not sets:
                continue
            sql = (f"UPDATE signal_followup SET {', '.join(sets)}"
                   + (", done=1" if last else "")
                   + " WHERE address=? AND ts=?")
            try:
                with self.owner.db_lock:
                    self.owner.con.execute(sql, [price] * len(sets) + [addr, ts])
                    self.owner.con.commit()
            except Exception:
                traceback.print_exc()

    def history(self):
        """当天的通知历史,新→旧,随 /api/board 下发给前端历史面板(churn 拦截的不展示)。"""
        today0 = datetime.now().replace(
            hour=0, minute=0, second=0, microsecond=0).timestamp()
        with self.owner.db_lock:
            rows = self.owner.con.execute(
                """SELECT ts,address,symbol,name,image,stock,url,price,gain,span,
                   market_cap,volume_24h,fired_at,tags FROM rising_hist
                   WHERE ts>=? AND (tags IS NULL OR tags NOT LIKE '%churn%')
                   ORDER BY ts DESC LIMIT 1000""", (today0,)).fetchall()
        return [{"ts": r[0], "address": r[1], "symbol": r[2], "name": r[3], "image": r[4],
                 "stock": r[5], "url": r[6], "price": r[7], "gain": r[8], "span": r[9],
                 "market_cap": r[10], "volume_24h": r[11], "fired_at": r[12],
                 "tags": r[13] or ""} for r in rows]

    def cycle(self):
        now = time.time()
        wl = self.watchlist()
        meta = {m["address"]: m for m in wl}
        updates, candidates = [], []
        prices = self.fetch_prices([m["address"] for m in wl])
        self._fill_followup(prices, now)       # 信号后轨迹回填(+1/3/5/15/30/60分钟)
        for addr, (price, mcap, vol) in prices.items():
            if price is not None:
                pts = self.hist.setdefault(addr, [])
                pts.append((now, price))
                if now - pts[0][0] > 600:           # 粗筛只要近10分钟的两三个点
                    pts[:] = [p for p in pts if now - p[0] <= 600]
                # 粗筛候选:最近一步在涨(便宜);是否真连涨3分钟交给K线判定
                if (len(pts) >= 2 and pts[-1][1] > pts[-2][1]
                        and now - self.fired_at.get(addr, 0) >= RISE_COOLDOWN
                        and now - self.kline_at.get(addr, 0) >= RISE_KLINE_GAP):
                    candidates.append(addr)
            updates.append((price, mcap, vol, time.time(), addr))
        for addr in candidates[:RISE_CAND_MAX]:     # K线右侧趋势确认(连涨+破前高)
            self.kline_at[addr] = now
            res = is_right_side(self.fetch_klines(addr))
            if not res:
                continue
            run, brk = res
            o0, c_last = run[0][1], run[-1][3]
            gain = (c_last - o0) / o0
            span = run[-1][0] + 60 - run[0][0]      # 段首开盘 → 段尾收盘
            price, mcap, vol = prices.get(addr, (None, None, None))
            self.fire(addr, meta.get(addr), run, gain, span,
                      price if price is not None else c_last, mcap, vol, now, brk)
        if updates:                                 # 采样结果写回,看板价格更实时
            with self.owner.db_lock:
                self.owner.con.executemany(
                    """UPDATE assets SET price=COALESCE(?,price), mcap=COALESCE(?,mcap),
                       volume24=COALESCE(?,volume24), updated_at=? WHERE address=?""",
                    updates)
                self.owner.con.commit()
        for addr in list(self.hist):                # 清理移出 watch list 的历史
            if addr not in meta and now - self.hist[addr][-1][0] > 600:
                del self.hist[addr]
        self.state.update(rise_at=time.strftime("%Y-%m-%d %H:%M:%S"),
                          rise_watch=len(wl), rise_error=None)

    def _heartbeat(self):
        """每小时 TG 心跳:监控活着 + 今日信号/拦截计数(哑巴失效立即可见)。"""
        today0 = datetime.now(CST).replace(
            hour=0, minute=0, second=0, microsecond=0).timestamp()
        tot = pushed = 0
        try:
            with self.owner.db_lock:
                tot, pushed = self.owner.con.execute(
                    "SELECT count(*), COALESCE(sum(pushed),0) FROM rising_hist "
                    "WHERE ts>=?", (today0,)).fetchone()
        except Exception:
            pass
        txt = (f"💓 监控心跳 {time.strftime('%H:%M')} · 今日信号 {tot}"
               f"(推送 {pushed}/重复 {tot - pushed - self.churned}≤/高换手拦截 {self.churned})"
               f" · 监视 {self.state.get('rise_watch') or 0} 币"
               f" · 上轮 {self.state.get('rise_at') or '—'}")
        try:
            tg_pusher.push(txt)                # 模块加载早期 NameError 时静默跳过
        except Exception:
            pass

    def run(self):
        last_beat = time.time()
        last_alert = 0.0
        while True:
            try:
                self.cycle()
                self.state["rise_error"] = None
            except Exception as e:
                self.state["rise_error"] = str(e)
                traceback.print_exc()
                if time.time() - last_alert >= 600:   # 告警节流:同种异常 10 分钟最多一条
                    last_alert = time.time()
                    try:
                        tg_pusher.push(f"🔴 连涨监控异常已自动重试: {str(e)[:150]}")
                    except Exception:
                        pass
            if time.time() - last_beat >= 3600:
                last_beat = time.time()
                self._heartbeat()
            time.sleep(RISE_SAMPLE)

    def recent(self):
        now = time.time()
        return [e for e in self.events if now - e["ts"] <= RISE_KEEP]


rising_watcher = RisingWatcher(poller)
rising_watcher.start()


# ---- TG 频道推送:连涨事件 → Telegram(tg.json 配置,缺省关闭) ----
class TgError(Exception):
    def __init__(self, msg, wait=0):
        super().__init__(msg)
        self.wait = wait    # 秒,429 时为官方 retry_after


def fmt_usd(v):
    if not v:
        return "-"
    for div, suf in ((1e9, "B"), (1e6, "M"), (1e3, "K")):
        if v >= div:
            return f"${v / div:,.2f}{suf}"
    return f"${v:,.0f}"


def rise_text(e, stock_zh="", day_count=0):
    """频道第一条:右侧信号行情,四行排版;追高⚠️/高置信🔴按 tags 附加。"""
    mins = max(1, round(e["span"] / 60))
    gain = f"+{e['gain'] * 100:.1f}%"
    cnt = f"今日第{day_count}次信号" if day_count else ""
    brk = e.get("brk")
    brk_txt = f"超前高{brk * 100:.1f}%" if brk is not None else ""
    line2 = "  ".join(x for x in (brk_txt, cnt) if x)
    parts = [html.escape(e["stock"])]
    if stock_zh:
        parts.append(html.escape(stock_zh))
    line3 = "底池 " + "·".join(parts)
    if e.get("name"):
        line3 += "· " + html.escape(e["name"])
    tags = e.get("tags") or ""
    warns = []
    if "chase" in tags:
        warns.append(f"⚠️ <b>追高极顶警示</b>:连涨窗口已达 +{e['gain'] * 100:.0f}%,"
                     f"此档复盘中位 -19.6%,谨防接顶")
    if "hi" in tags:
        warns.append(f"🔴 高置信:市值 {fmt_usd(e['market_cap'])} ≥ $2M,"
                     f"此档复盘中位 -3.5%,远稳于小市值")
    return (
        f"📈 <b>连续{mins}分钟上涨 {html.escape(e['symbol'])} {gain}</b>\n"
        + (f"{line2}\n" if line2 else "")
        + "".join(w + "\n" for w in warns)
        + f"{line3}\n"
        f"市值 {fmt_usd(e['market_cap'])} · 24h量 {fmt_usd(e['volume_24h'])}")


WEEK_ZH = "一二三四五六日"


def digest_text(day, per_coin, n_signals, n_stocks):
    """每日信号复盘:衡量 bot 整体胜率。

    每个币取「首发信号时的市值 vs 当日结束时的现价市值」算涨跌幅——
    (不能用首次 vs 末次信号:基线过滤已保证末次信号价必然高于首发,恒 100% 无意义)。
    per_coin=[(symbol, first_mcap, latest_mcap, 信号数, 首发ts)]。
    """
    d = datetime.strptime(day, "%Y-%m-%d")
    L = [f"📊 每日信号复盘 · {d.month}月{d.day}日 周{WEEK_ZH[d.weekday()]}",
         f"信号 {n_signals} 个 · {len(per_coin)} 个 meme · {n_stocks} 个底池", ""]
    scored = [(last / first - 1, sym, cnt)
              for sym, first, last, cnt, _ts in per_coin if first and last]
    if not scored:
        L.append("今日无有效信号")
        return "\n".join(L)
    wins = [s for s in scored if s[0] > 0]
    skipped = len(per_coin) - len(scored)
    L.append(f"🏆 胜率 {len(wins)}/{len(scored)} = {len(wins) / len(scored):.0%}"
             f" · 平均 {sum(s[0] for s in scored) / len(scored):+.1%}"
             + (f"(另 {skipped} 个无现价)" if skipped else ""))
    L.append("🚀 最佳: " + " · ".join(
        f"{html.escape(s)} {p:+.1%}" for p, s, _ in sorted(scored, reverse=True)[:3]))
    if len(scored) > 3:
        L.append("🥀 最差: " + " · ".join(
            f"{html.escape(s)} {p:+.1%}" for p, s, _ in sorted(scored)[:3]))
    L.append("🔥 高频: " + " · ".join(
        f"{html.escape(s)}×{c}" for s, _, _, c, _ in
        sorted(per_coin, key=lambda x: -x[3])[:3]))
    return "\n".join(L)


class TelegramPusher(threading.Thread):
    """fire() 入队 → 单发送线程消费;429 按 retry_after 退避,失败重试 3 次后丢弃。

    配置 tg.json:{"bot_token","chat_id","proxy"?}(bot 需为频道管理员),
    或环境变量 TG_BOT_TOKEN / TG_CHAT_ID / TG_PROXY,缺任一项整体关闭。
    """

    def __init__(self, owner):
        super().__init__(daemon=True, name="tg")
        self.owner = owner                    # 共用 Poller 的 con/db_lock/zh
        cfg = {}
        if TG_CONF.exists():
            try:
                cfg = json.loads(TG_CONF.read_text())
            except (OSError, ValueError):
                traceback.print_exc()
        self.token = cfg.get("bot_token") or os.environ.get("TG_BOT_TOKEN") or ""
        self.chat = cfg.get("chat_id") or os.environ.get("TG_CHAT_ID") or ""
        self.proxy = cfg.get("proxy") or os.environ.get("TG_PROXY") or ""
        self.q = queue.Queue()
        self.state = {"tg_ok": bool(self.token and self.chat), "tg_error": None}
        if not self.state["tg_ok"]:
            print("[tg] 未配置 bot_token/chat_id(tg.json),频道推送关闭")

    def push(self, text):
        if self.token and self.chat:
            self.q.put(text)

    def rise(self, e):
        zh = self.owner.zh.get(e["stock"], {}).get("zh") or ""
        self.push(rise_text(e, zh, e.get("nth") or self.day_count(e["address"])))
        self.push(e["address"])     # CA 单独一条纯文本,长按复制即整条地址

    def day_count(self, addr):
        """北京时间当天该币的信号次数(含本次:fire() 先落库后推送)。"""
        today0 = datetime.now(CST).replace(
            hour=0, minute=0, second=0, microsecond=0).timestamp()
        try:
            with self.owner.db_lock:
                return self.owner.con.execute(
                    "SELECT count(*) FROM rising_hist WHERE ts >= ? AND address = ?",
                    (today0, addr)).fetchone()[0]
        except Exception:
            traceback.print_exc()           # 计数失败只影响展示,不影响推送
            return 0

    def _send(self, text):
        proxies = {"http": self.proxy, "https": self.proxy} if self.proxy else None
        r = http().post(
            TG_API + f"/bot{self.token}/sendMessage",
            json={"chat_id": self.chat, "text": text, "parse_mode": "HTML",
                  "disable_web_page_preview": True},
            proxies=proxies, timeout=15)
        data = {}
        try:
            data = r.json()
        except ValueError:
            pass
        if r.status_code != 200:
            wait = int((data.get("parameters") or {}).get("retry_after") or 0)
            raise TgError(f"TG {r.status_code}: {str(data)[:200]}", wait)

    def run(self):
        if not (self.token and self.chat):
            return
        self.push(f"🟢 LONG 榜单监控已启动,右侧信号通知"
                  f"(连涨{RISE_KLINE}根1分钟K线 + 突破{RIGHT_LOOKBACK}分钟前高)"
                  f"将推送到本频道")
        while True:
            text = self.q.get()
            try:
                for attempt in range(3):
                    try:
                        self._send(text)
                        self.state["tg_error"] = None
                        break
                    except Exception as e:
                        self.state["tg_error"] = str(e)
                        if attempt < 2:
                            time.sleep(getattr(e, "wait", 0) or 10)
                else:
                    print(f"[tg] 重试 3 次仍失败,放弃: {text[:60]}…")
            finally:
                self.q.task_done()
            time.sleep(1.5)                      # 频道限流 ~20条/分钟,保底间隔


tg_pusher = TelegramPusher(poller)
tg_pusher.start()


# ---- 每日总结:零点(北京)把前一天的信号留存汇总推到 TG 频道,重启自动补发漏发 ----
class DailyDigest(threading.Thread):
    """rising_hist 按天留存,每天 0 点后总结前一天;digests 表防止重发。"""

    def __init__(self, owner):
        super().__init__(daemon=True, name="daily-digest")
        self.owner = owner                    # 共用 Poller 的 con/db_lock
        self.state = {"digest_at": None, "digest_error": None,
                      "snap_at": None, "snap_n": 0}

    @staticmethod
    def _day_bounds(day):
        day0 = datetime.strptime(day, "%Y-%m-%d").replace(tzinfo=CST).timestamp()
        return day0, day0 + 86400

    def _rows(self, day):
        t0, t1 = self._day_bounds(day)
        with self.owner.db_lock:
            return self.owner.con.execute(
                """SELECT ts,address,symbol,stock,market_cap FROM rising_hist
                   WHERE ts>=? AND ts<? AND (tags IS NULL OR tags NOT LIKE '%churn%')
                   ORDER BY ts""", (t0, t1)).fetchall()

    def _latest_mcaps(self, addrs):
        """各币当前(≈当日结束)市值,取自 assets 表实时行情。"""
        out = {}
        with self.owner.db_lock:
            for i in range(0, len(addrs), 500):
                chunk = addrs[i:i + 500]
                out.update(dict(self.owner.con.execute(
                    "SELECT address, mcap FROM assets WHERE address IN (%s)"
                    % ",".join("?" * len(chunk)), chunk)))
        return out

    def _snapshot(self, day):
        """零点快照:当天有信号的币过零点立即拉实时价存档,每日复盘的终点从此精确
        (此前用"最后信号观测价"近似,掉出 watchlist 的币可能滞后数小时)。
        仅零点后 10 分钟内执行——过时再拉到的是当前价而非午夜价,宁缺毋滥。"""
        _, t1 = self._day_bounds(day)
        if not (0 <= time.time() - t1 <= 600):
            return
        t0, t1b = self._day_bounds(day)
        with self.owner.db_lock:
            if self.owner.con.execute(
                    "SELECT 1 FROM day_snapshot WHERE day=? LIMIT 1",
                    (day,)).fetchone():
                return                          # 当天已快照,幂等
            addrs = [r[0] for r in self.owner.con.execute(
                "SELECT DISTINCT address FROM rising_hist WHERE ts>=? AND ts<?",
                (t0, t1b)).fetchall()]
        if not addrs:
            return
        prices = {}
        for _attempt in range(3):               # 零点偶发网络失败重试
            prices = rising_watcher.fetch_prices(addrs)
            if prices:
                break
            time.sleep(20)
        now = time.time()
        rows = [(day, a, p[0], p[1], now)
                for a, p in prices.items() if p[0] is not None]
        if rows:
            with self.owner.db_lock:
                self.owner.con.executemany(
                    "INSERT OR REPLACE INTO day_snapshot(day,address,price,mcap,ts)"
                    " VALUES(?,?,?,?,?)", rows)
                self.owner.con.commit()
            self.state.update(snap_at=time.strftime("%Y-%m-%d %H:%M:%S"),
                              snap_n=len(rows))

    def _sent(self, day):
        with self.owner.db_lock:
            return bool(self.owner.con.execute(
                "SELECT 1 FROM digests WHERE day=?", (day,)).fetchone())

    def _send_daily(self, day):
        if self._sent(day) or not (tg_pusher.token and tg_pusher.chat):
            return
        try:
            rows = self._rows(day)
            # 每币:(symbol, 首发市值, 首发ts, 信号次数),rows 已按时间升序
            first_by = {}
            for ts, addr, sym, stock, mc in rows:
                hit = first_by.setdefault(addr, [sym, mc, ts, 0])
                hit[3] += 1
            latest = self._latest_mcaps(list(first_by))
            # 零点快照优先:assets 对掉出 watchlist 的币可能滞后数小时,快照才是真午夜市值
            with self.owner.db_lock:
                for addr, mc in self.owner.con.execute(
                        "SELECT address, mcap FROM day_snapshot WHERE day=?",
                        (day,)).fetchall():
                    if mc is not None:
                        latest[addr] = mc
            per_coin = [(sym, first, latest.get(addr), cnt, ts)
                        for addr, (sym, first, ts, cnt) in first_by.items()]
            n_stocks = len({r[3] for r in rows})
            tg_pusher.push(digest_text(day, per_coin, len(rows), n_stocks))
            with self.owner.db_lock:
                self.owner.con.execute(
                    "INSERT OR IGNORE INTO digests(day,sent_at) VALUES(?,?)",
                    (day, time.time()))
                self.owner.con.commit()
            self.state.update(digest_at=time.strftime("%Y-%m-%d %H:%M:%S"),
                              digest_error=None)
        except Exception as e:
            self.state["digest_error"] = str(e)
            traceback.print_exc()

    def run(self):
        # 补发:最近7天里有数据但未发过总结的往日(服务跨零点重启的场景)
        for i in range(7, 0, -1):
            day = (datetime.now(CST) - timedelta(days=i)).strftime("%Y-%m-%d")
            self._snapshot(day)                 # 非零点时刻直接跳过(窗口守卫)
            if self._rows(day):
                self._send_daily(day)
        while True:
            now = datetime.now(CST)
            nxt = (now + timedelta(days=1)).replace(
                hour=0, minute=0, second=0, microsecond=0)
            time.sleep((nxt - now).total_seconds() + 5)   # 过零点5秒,避开整点写入竞态
            day = (datetime.now(CST) - timedelta(days=1)).strftime("%Y-%m-%d")
            self._snapshot(day)
            self._send_daily(day)


daily_digest = DailyDigest(poller)
daily_digest.start()

# ---- 本地图标缓存:后台预取,磁盘留存 ----
ICONS = BASE / "icons"
ICONS.mkdir(exist_ok=True)
ICON_HOSTS = (".long.xyz",)      # 只代理白名单域名
_icon_q: "queue.Queue" = queue.Queue()
_icon_seen: set = set()
_icon_lock = threading.Lock()


def _icon_cached(addr: str):
    """返回已缓存的图标文件,或 None(负缓存未过期时也返回 None)。"""
    for p in ICONS.glob(addr + ".*"):
        if p.suffix == ".empty":
            if time.time() - p.stat().st_mtime < 3600:
                return None
            p.unlink()
            continue
        return p
    return None


def _download_icon(addr: str, u: str):
    if _icon_cached(addr) is not None:      # 已缓存则不重复下载
        return True
    if (ICONS / (addr + ".empty")).exists():
        return False
    candidates = []
    if u.startswith("https://") and any(h in u for h in ICON_HOSTS):
        candidates.append(u)
    candidates.append("https://storage.long.xyz/tokens/" + addr + ".jpg")
    for url in candidates:
        try:
            r = http().get(url, timeout=15)
            ctype = r.headers.get("content-type", "")
            if r.status_code == 200 and ctype.startswith("image/") and len(r.content) > 100:
                data = r.content
                try:                        # 缩到 96px,原图可达数 MB
                    import io
                    from PIL import Image
                    im = Image.open(io.BytesIO(data)).convert("RGB")
                    im.thumbnail((96, 96))
                    buf = io.BytesIO()
                    im.save(buf, "JPEG", quality=85)
                    data = buf.getvalue()
                    ext = "jpg"
                except Exception:
                    ext = "png" if "png" in ctype else ("webp" if "webp" in ctype else "jpg")
                tmp = ICONS / (addr + ".tmp")
                tmp.write_bytes(data)
                tmp.rename(ICONS / (addr + "." + ext))
                return True
        except Exception:
            continue
    (ICONS / (addr + ".empty")).touch()
    return False


def queue_icon(addr: str, u: str = ""):
    if not (addr and addr.startswith("0x") and len(addr) == 42):
        return
    with _icon_lock:
        if addr in _icon_seen:
            return
        _icon_seen.add(addr)
    _icon_q.put((addr.lower(), u))


def _icon_worker():
    while True:
        addr, u = _icon_q.get()
        try:
            ok = _download_icon(addr, u)
            if not ok:                     # 失败的允许 1 小时后(负缓存过期)重试
                with _icon_lock:
                    _icon_seen.discard(addr)
        finally:
            _icon_q.task_done()


for _ in range(6):                       # 并行预取,加快首次回填
    threading.Thread(target=_icon_worker, daemon=True, name="icon-worker").start()

app = FastAPI(title="LONG 股票 meme 榜单")


@app.get("/icon")
def icon(addr: str, u: str = ""):
    addr = addr.lower()
    if not (addr.startswith("0x") and len(addr) == 42 and all(c in "0123456789abcdef" for c in addr[2:])):
        return Response(status_code=400)
    p = _icon_cached(addr)
    if p is not None:
        return Response(p.read_bytes(), media_type="image/" + p.suffix.lstrip("."),
                        headers={"Cache-Control": "public, max-age=86400"})
    queue_icon(addr, u)                    # 未缓存则排入预取队列,前端稍后重试即可
    return Response(status_code=404)


@app.get("/api/iconstats")
def iconstats():
    import threading as _t
    return {
        "queued": _icon_q.qsize(),
        "seen": len(_icon_seen),
        "workers": [t.name for t in _t.enumerate() if t.name == "icon-worker"],
        "alive": sum(t.is_alive() for t in _t.enumerate() if t.name == "icon-worker"),
    }


@app.get("/api/board")
def board():
    return JSONResponse(poller.board())


@app.get("/api/memes")
def memes(n: str):
    return JSONResponse(poller.memes_for(n))


@app.get("/api/longx")
def longx():
    return JSONResponse(longx_poller.snapshot())


app.mount("/", StaticFiles(directory=str(ROOT / "web" / "static"), html=True), name="static")
