import requests
import re
import json
import math
import os
import html
import xml.etree.ElementTree as ET
from datetime import datetime, timezone

ET.register_namespace("content", "http://purl.org/rss/1.0/modules/content/")

# ----------------------------
# Constants
# ----------------------------

FMP_API_KEY = os.getenv("FMP_API_KEY")
BOARD = "biz"

# Feeds
FEED_ACTIVE = "feed-biz.xml"
FEED_FAST = "feed-biz-fast.xml"
FEED_TICKERS = "feed-biz-tickers.xml"
FEED_HISIGNAL = "feed-biz-alpha.xml"
FEED_MICROCAP = "feed-microcap.xml"
FEED_DASH = "feed-alpha-dashboard.xml"
FEED_ELITE = "feed-alpha-elite.xml"
FEED_ASYM = "feed-alpha-asymmetric.xml"
FEED_CRYPTO = "feed-crypto-100x.xml"  # NEW: breakout meme / low-cap crypto alpha

# History / cache
MICROCAP_HISTORY_FILE = "microcap_history.json"
ELITE_HISTORY_FILE = "elite_history.json"
COINGECKO_CACHE_FILE = "coingecko_cache.json"

# Ticker extraction
TICKER_REGEX = r"\b[A-Z]{2,5}\b"

# /biz/ thread rendering
THREAD_LIMIT = 12
LAST_REPLIES = 30

# Stocks: US exchange, microcap, optionable
VALID_EXCHANGES = {"NASDAQ", "NYSE", "AMEX"}
MAX_MARKET_CAP_STOCK = 2_500_000_000  # <= $2.5B
REQUIRE_OPTIONABLE = True

# Elite analytics windows
SNAPSHOT_KEEP_HOURS = 48
NEW_TICKER_LOOKBACK_HOURS = 24

# Mention thresholds
MIN_MENTIONS_TO_SURFACE = 2
SPIKE_ABS_DELTA = 4
SPIKE_MULTIPLIER = 2.5

# Sources
REDDIT_SUBS = ["pennystocks", "wallstreetbets"]

# Noise blacklist
TICKER_BLACKLIST = {
    "USD", "USDT", "USDC",
    "CEO", "CFO", "SEC", "FED", "FOMC", "USA", "EU", "UK",
    "NYSE", "NASDAQ", "AMEX", "ETF", "IPO",
    "AI", "DD", "IMO", "LOL", "YOLO", "FOMO", "HODL", "ATH", "TLDR"
}

# Crypto "100x" tuning
CRYPTO_MAX_MCAP = 300_000_000        # focus on low cap
CRYPTO_MIN_MCAP = 10_000_000         # avoid dust / broken markets
CRYPTO_MIN_VOLUME = 3_000_000        # avoid illiquid junk
CRYPTO_BREAKOUT_PCT_24H = 15.0       # breakout bias
CRYPTO_PAGES = 4                     # 4 * 250 = top 1000 by cap daily cache
MEME_KEYWORDS = [
    "INU", "DOGE", "SHIB", "PEPE", "BONK", "FLOKI", "WIF", "BRETT",
    "CAT", "FROG", "MONKEY", "TRUMP", "MAGA", "BODEN", "HARRIS",
    "WOJAK", "MEME", "BABY", "KID", "ELON"
]


# ----------------------------
# Helpers
# ----------------------------

def now_ts() -> int:
    return int(datetime.now(timezone.utc).timestamp())


def rfc822(ts: int) -> str:
    return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%a, %d %b %Y %H:%M:%S %z")


def strip_html(s: str) -> str:
    if not s:
        return ""
    s = s.replace("<br>", "\n")
    s = re.sub(r"<[^>]+>", "", s)
    return html.unescape(s).strip()


def safe_get(url: str, headers=None, timeout=12, params=None):
    try:
        return requests.get(url, headers=headers or {}, timeout=timeout, params=params)
    except:
        return None


def fetch_json(url: str, headers=None, timeout=12, params=None):
    r = safe_get(url, headers=headers, timeout=timeout, params=params)
    if not r or r.status_code != 200:
        return None
    try:
        return r.json()
    except:
        return None


def extract_tickers(text: str):
    return re.findall(TICKER_REGEX, text or "")


def plausible_ticker(tk: str) -> bool:
    if tk in TICKER_BLACKLIST:
        return False
    if len(tk) < 2 or len(tk) > 5:
        return False
    return True


def fmt_money(x):
    if x is None or not isinstance(x, (int, float)):
        return "Unknown"
    return f"${int(x):,}"


# ----------------------------
# Sources
# ----------------------------

def fetch_catalog():
    return fetch_json(f"https://a.4cdn.org/{BOARD}/catalog.json")


def fetch_thread(thread_no: int):
    data = fetch_json(f"https://a.4cdn.org/{BOARD}/thread/{thread_no}.json")
    return data.get("posts", []) if data else None


def fetch_reddit(sub: str):
    headers = {"User-Agent": "Mozilla/5.0 (alpha-rss)"}
    return fetch_json(f"https://www.reddit.com/r/{sub}/new.json?limit=75", headers=headers)


def thread_velocity(t: dict, now: int) -> float:
    replies = t.get("replies", 0)
    last = t.get("last_modified", t.get("time", now))
    hours = max((now - last) / 3600.0, 0.25)
    return replies / hours


# ----------------------------
# JSON file utils
# ----------------------------

def load_json_file(path: str, default):
    if not os.path.exists(path):
        return default
    try:
        with open(path, "r") as f:
            return json.load(f)
    except:
        return default


def save_json_file(path: str, data):
    with open(path, "w") as f:
        json.dump(data, f)


# ----------------------------
# RSS writer (oldest -> newest)
# ----------------------------

def write_rss(title: str, link: str, desc: str, items: list, filename: str):
    # Sort items oldest -> newest using pub_ts
    items_sorted = sorted(items, key=lambda it: it.get("pub_ts", 0))

    rss = ET.Element("rss", version="2.0")
    channel = ET.SubElement(rss, "channel")

    ET.SubElement(channel, "title").text = title
    ET.SubElement(channel, "link").text = link
    ET.SubElement(channel, "description").text = desc
    ET.SubElement(channel, "lastBuildDate").text = rfc822(now_ts())

    for it in items_sorted:
        item = ET.SubElement(channel, "item")
        ET.SubElement(item, "title").text = it["title"]
        ET.SubElement(item, "link").text = it["link"]
        ET.SubElement(item, "guid").text = it["guid"]
        ET.SubElement(item, "pubDate").text = it["pubDate"]
        ET.SubElement(item, "description").text = it.get("description", "Open article")

        content = ET.SubElement(item, "{http://purl.org/rss/1.0/modules/content/}encoded")
        content.text = it.get("content_html", "")

        if it.get("enclosure_url"):
            enc = ET.SubElement(item, "enclosure")
            enc.set("url", it["enclosure_url"])
            enc.set("type", it.get("enclosure_type", "image/jpeg"))
            enc.set("length", "0")

    ET.ElementTree(rss).write(filename, encoding="utf-8", xml_declaration=True)


# ----------------------------
# Thread item builder
# - OP stays on top
# - Replies are oldest -> newest (scroll down for newer)
# - GUID changes with last_modified so Reeder notices updates
# ----------------------------

def build_thread_item(t: dict, posts: list, prefix: str = "") -> dict:
    n = now_ts()
    no = t["no"]
    url = f"https://boards.4chan.org/{BOARD}/thread/{no}"
    subject = strip_html(t.get("sub")) or f"Thread {no}"
    replies = t.get("replies", 0)

    last_mod = t.get("last_modified", t.get("time", n))
    pub_ts = last_mod
    guid = f"{url}?lm={last_mod}"  # force update recognition

    op_text = strip_html(posts[0].get("com"))

    # Take the last N replies but keep chronological order (oldest -> newest)
    reply_posts = posts[1:]
    reply_posts = reply_posts[-LAST_REPLIES:]  # keep last N, still chronological

    body = []
    body.append(f"<h2>{html.escape(prefix + subject)}</h2>")
    body.append(
        f"<p><a href='{url}'>Open thread</a> • Replies: {replies} • "
        f"Updated: {datetime.fromtimestamp(last_mod, tz=timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}</p>"
    )

    body.append("<hr><h3>OP</h3>")
    body.append(f"<p>{html.escape(op_text).replace(chr(10), '<br>')}</p>")

    body.append("<hr><h3>Replies (oldest → newest)</h3>")
    for p in reply_posts:
        txt = strip_html(p.get("com"))
        if not txt:
            continue
        body.append(
            f"<p><b>{p.get('no')}</b><br>"
            f"{html.escape(txt).replace(chr(10), '<br>')}</p><hr>"
        )

    thumb = None
    if "tim" in t:
        thumb = f"https://i.4cdn.org/{BOARD}/{t['tim']}s.jpg"

    return {
        "title": f"{prefix}{subject} — {replies} replies",
        "link": url,
        "guid": guid,
        "pub_ts": pub_ts,
        "pubDate": rfc822(pub_ts),
        "description": "Open article for full thread",
        "content_html": "".join(body),
        "enclosure_url": thumb,
        "enclosure_type": "image/jpeg",
    }


# ----------------------------
# Stock + options validation
# ----------------------------

def fmp_stock_profile(ticker: str):
    if not FMP_API_KEY:
        return None
    return fetch_json(f"https://financialmodelingprep.com/api/v3/profile/{ticker}?apikey={FMP_API_KEY}")


def yahoo_optionable(ticker: str) -> bool:
    data = fetch_json(f"https://query2.finance.yahoo.com/v7/finance/options/{ticker}")
    result = (data or {}).get("optionChain", {}).get("result")
    if not result:
        return False
    return bool(result[0].get("expirationDates"))


def validate_stock_us_microcap_optionable(ticker: str):
    prof = fmp_stock_profile(ticker)
    if not prof:
        return None
    p = prof[0]

    exch = p.get("exchangeShortName")
    if exch not in VALID_EXCHANGES:
        return None

    cap = p.get("mktCap")
    if not cap or cap > MAX_MARKET_CAP_STOCK:
        return None

    if REQUIRE_OPTIONABLE and not yahoo_optionable(ticker):
        return None

    return {
        "type": "Stock",
        "ticker": ticker,
        "name": p.get("companyName") or ticker,
        "cap": cap,
        "desc": (p.get("description") or "")[:240]
    }


# ----------------------------
# CoinGecko cache (top 1000 daily)
# ----------------------------

def get_coingecko_symbol_map():
    cache = load_json_file(COINGECKO_CACHE_FILE, {})
    n = now_ts()
    last = cache.get("ts", 0)

    # refresh daily
    if (n - last) < 24 * 3600 and "coins" in cache:
        coins = cache["coins"]
    else:
        coins = []
        url = "https://api.coingecko.com/api/v3/coins/markets"
        for page in range(1, CRYPTO_PAGES + 1):
            params = {
                "vs_currency": "usd",
                "order": "market_cap_desc",
                "per_page": 250,
                "page": page,
                "sparkline": "false",
                "price_change_percentage": "24h"
            }
            r = safe_get(url, timeout=18, params=params)
            page_data = r.json() if (r and r.status_code == 200) else []
            if not page_data:
                break
            coins.extend(page_data)

        save_json_file(COINGECKO_CACHE_FILE, {"ts": n, "coins": coins})

    sym_map = {}
    for c in coins or []:
        sym = (c.get("symbol") or "").upper()
        if not sym:
            continue
        if sym not in sym_map:
            sym_map[sym] = {
                "type": "Crypto",
                "ticker": sym,
                "name": c.get("name") or sym,
                "cap": c.get("market_cap"),
                "volume": c.get("total_volume"),
                "chg24": c.get("price_change_percentage_24h"),
                "desc": ""
            }
    return sym_map


# ----------------------------
# Elite history & mention gathering
# ----------------------------

def load_elite_history():
    data = load_json_file(ELITE_HISTORY_FILE, {"snapshots": []})
    if "snapshots" not in data or not isinstance(data["snapshots"], list):
        data = {"snapshots": []}
    return data


def save_elite_history(data):
    save_json_file(ELITE_HISTORY_FILE, data)


def trim_snapshots(data):
    keep_seconds = SNAPSHOT_KEEP_HOURS * 3600
    n = now_ts()
    data["snapshots"] = [s for s in data["snapshots"] if (n - s.get("ts", 0)) <= keep_seconds]


def get_recent_snapshots(history, lookback_hours):
    n = now_ts()
    lb = lookback_hours * 3600
    return [s for s in history.get("snapshots", []) if (n - s.get("ts", 0)) <= lb]


def compute_momentum(history, ticker, points=6):
    snaps = history.get("snapshots", [])
    if len(snaps) < 2:
        return 0.0
    last = snaps[-points:]
    series = [s.get("counts", {}).get(ticker, 0) for s in last]
    if len(series) < 2:
        return 0.0
    return (series[-1] - series[0]) / max(len(series) - 1, 1)


def is_new_ticker(history, ticker):
    recent = get_recent_snapshots(history, NEW_TICKER_LOOKBACK_HOURS)
    for s in recent:
        if s.get("counts", {}).get(ticker, 0) > 0:
            return False
    return True


def gather_mentions():
    catalog = fetch_catalog()
    c_biz = {}
    c_red = {}

    # /biz/ catalog mentions
    if catalog:
        for page in catalog:
            for t in page.get("threads", []):
                text = (t.get("sub", "") or "") + " " + (t.get("com", "") or "")
                for tk in extract_tickers(text):
                    if plausible_ticker(tk):
                        c_biz[tk] = c_biz.get(tk, 0) + 1

    # reddit mentions (titles)
    for sub in REDDIT_SUBS:
        data = fetch_reddit(sub)
        if not data:
            continue
        for post in data.get("data", {}).get("children", []):
            title = post.get("data", {}).get("title", "")
            for tk in extract_tickers(title):
                if plausible_ticker(tk):
                    c_red[tk] = c_red.get(tk, 0) + 1

    return c_biz, c_red


def classify_and_enrich(ticker, cg_map):
    # Prefer stock validation (quality)
    s = validate_stock_us_microcap_optionable(ticker)
    if s:
        return s
    # Crypto: from cached top1000 list (quality)
    if ticker in cg_map:
        return cg_map[ticker]
    return None


# ----------------------------
# Scoring (risk-on bias)
# ----------------------------

def cap_factor_for_asym(asset_type, cap):
    """
    Smaller cap => larger factor.
    Bounded so it doesn't explode.
    """
    if not isinstance(cap, (int, float)) or not cap or cap <= 0:
        return 1.0

    if asset_type == "Stock":
        # Reference: 2.5B -> ~1.0, 500M -> ~2.2, 200M -> ~3.0, 50M -> ~4.0
        return min(4.5, max(1.0, (2_500_000_000 / cap) ** 0.35))

    # Crypto: similar, but a touch softer
    return min(4.0, max(1.0, (2_000_000_000 / cap) ** 0.30))


def elite_score(curr, prev, momentum, is_new, biz_count, red_count, asset_type, cap):
    delta = curr - prev
    cross = 1.6 if (biz_count > 0 and red_count > 0) else 1.0
    new_bonus = 2.0 if is_new else 0.0

    base = (delta * 2.2) + (curr * 0.35) + (momentum * 1.6) + new_bonus

    # risk-on microcap boost
    cf = cap_factor_for_asym(asset_type, cap)
    if asset_type == "Stock":
        cf = min(2.2, cf)  # elite feed: cap helps, but not as aggressive as asym feed
    else:
        cf = min(1.8, cf)

    return base * cross * cf


def asymmetry_score(curr, prev, momentum, is_new, biz_count, red_count, asset_type, cap):
    delta = curr - prev
    cross = 1.7 if (biz_count > 0 and red_count > 0) else 1.0
    new_bonus = 2.5 if is_new else 0.0

    wake = (delta * 2.6) + (momentum * 2.0) + (curr * 0.25) + new_bonus

    cf = cap_factor_for_asym(asset_type, cap)

    type_bias = 1.15 if asset_type == "Stock" else 1.0
    return wake * cf * cross * type_bias


def build_why_asymmetric(asset_type, cap, new_flag, spike_flag, cross_flag, delta, momentum, curr):
    bits = []
    bits.append(f"{asset_type}")
    bits.append(f"mcap {fmt_money(cap)}")
    if new_flag:
        bits.append("NEW")
    if spike_flag:
        bits.append("SPIKE")
    if cross_flag:
        bits.append("CROSS")
    bits.append(f"Δ{delta}")
    bits.append(f"mom {momentum:.2f}")
    bits.append(f"mentions {curr}")
    return " • ".join(bits)


# ----------------------------
# Feeds: /biz/ thread feeds
# ----------------------------

def generate_biz_active():
    catalog = fetch_catalog()
    if not catalog:
        return

    n = now_ts()
    threads = [t for p in catalog for t in p.get("threads", [])]
    threads.sort(key=lambda x: thread_velocity(x, n), reverse=True)

    items = []
    for t in threads[:THREAD_LIMIT]:
        posts = fetch_thread(t["no"])
        if posts:
            items.append(build_thread_item(t, posts))

    write_rss("/biz/ Active Threads",
              f"https://boards.4chan.org/{BOARD}/",
              "Active threads with OP + last 30 replies (oldest→newest)",
              items,
              FEED_ACTIVE)


def generate_biz_fast():
    catalog = fetch_catalog()
    if not catalog:
        return

    n = now_ts()
    threads = [t for p in catalog for t in p.get("threads", [])]
    threads.sort(key=lambda x: thread_velocity(x, n), reverse=True)

    items = []
    for t in threads[:THREAD_LIMIT]:
        if t.get("replies", 0) < 25:
            continue
        posts = fetch_thread(t["no"])
        if posts:
            vel = thread_velocity(t, n)
            items.append(build_thread_item(t, posts, f"[FAST {vel:.1f}/hr] "))

    write_rss("/biz/ FAST Threads",
              f"https://boards.4chan.org/{BOARD}/",
              "Rapidly moving threads (oldest→newest within thread)",
              items,
              FEED_FAST)


def generate_biz_tickers():
    catalog = fetch_catalog()
    if not catalog:
        return

    threads = [t for p in catalog for t in p.get("threads", [])]
    items = []

    for t in threads:
        text = (t.get("sub", "") or "") + " " + (t.get("com", "") or "")
        tks = [x for x in extract_tickers(text) if plausible_ticker(x)]
        if not tks:
            continue
        posts = fetch_thread(t["no"])
        if posts:
            prefix = "[" + " ".join(sorted(set(tks))[:5]) + "] "
            items.append(build_thread_item(t, posts, prefix))
        if len(items) >= THREAD_LIMIT:
            break

    write_rss("/biz/ Ticker Threads",
              f"https://boards.4chan.org/{BOARD}/",
              "Ticker mention threads with inline replies",
              items,
              FEED_TICKERS)


def generate_biz_hisignal():
    catalog = fetch_catalog()
    if not catalog:
        return

    n = now_ts()
    threads = [t for p in catalog for t in p.get("threads", [])]

    candidates = []
    for t in threads:
        replies = t.get("replies", 0)
        last = t.get("last_modified", t.get("time", n))
        hours = max((n - last) / 3600.0, 0.25)
        vel = replies / hours

        if replies < 40 or vel < 8 or (n - last) > 7200:
            continue

        text = (t.get("sub", "") or "") + " " + (t.get("com", "") or "")
        tks = [x for x in extract_tickers(text) if plausible_ticker(x)]
        if not tks:
            continue

        score = vel * math.log(replies + 1) * len(set(tks))
        candidates.append((score, vel, len(set(tks)), t))

    candidates.sort(reverse=True)

    items = []
    for score, vel, tc, t in candidates[:THREAD_LIMIT]:
        posts = fetch_thread(t["no"])
        if posts:
            items.append(build_thread_item(t, posts, f"[ALPHA v={vel:.1f}/hr t={tc}] "))

    write_rss("/biz/ HIGH-SIGNAL",
              f"https://boards.4chan.org/{BOARD}/",
              "High-signal threads (fast + tickers + fresh)",
              items,
              FEED_HISIGNAL)


# ----------------------------
# Microcap feed (stocks only)
# ----------------------------

def generate_microcap_feed():
    catalog = fetch_catalog()
    if not catalog:
        return

    mentions = {}
    for page in catalog:
        for t in page.get("threads", []):
            text = (t.get("sub", "") or "") + " " + (t.get("com", "") or "")
            for tk in extract_tickers(text):
                if plausible_ticker(tk):
                    mentions[tk] = mentions.get(tk, 0) + 1

    prev = load_json_file(MICROCAP_HISTORY_FILE, {})
    accel = {k: v for k, v in mentions.items() if prev.get(k, 0) < v}
    save_json_file(MICROCAP_HISTORY_FILE, mentions)

    validated = []
    for tk, count in accel.items():
        info = validate_stock_us_microcap_optionable(tk)
        if not info:
            continue
        score = count / math.log(info["cap"])
        validated.append((score, tk, info, count))

    validated.sort(reverse=True)

    n = now_ts()
    items = []
    for i, (score, tk, info, count) in enumerate(validated[:12]):
        cap_str = fmt_money(info["cap"])
        body = (
            f"<h2>${tk} — {html.escape(info['name'])}</h2>"
            f"<p><b>Type:</b> Stock</p>"
            f"<p><b>Market Cap:</b> {cap_str}</p>"
            f"<p><b>Mentions (accelerating):</b> {count}</p>"
            f"<p>{html.escape(info['desc'])}</p>"
            f"<p><a href='https://finance.yahoo.com/quote/{tk}'>Yahoo Finance</a></p>"
        )
        pub_ts = n - (len(validated[:12]) - i)  # preserve list order when sorted oldest->newest
        items.append({
            "title": f"{tk} — {cap_str} — accel {count}",
            "link": f"https://finance.yahoo.com/quote/{tk}",
            "guid": f"{tk}-microcap-{n}",
            "pub_ts": pub_ts,
            "pubDate": rfc822(pub_ts),
            "description": "Open for details",
            "content_html": body
        })

    write_rss("Microcap Equities Alpha",
              "https://boards.4chan.org/biz/",
              "Accelerating mentions (US exchange, <2.5B, optionable)",
              items,
              FEED_MICROCAP)


# ----------------------------
# Elite + Asymmetric + Crypto 100x feeds
# ----------------------------

def generate_elite_and_asym_and_crypto():
    n = now_ts()
    cg_map = get_coingecko_symbol_map()

    c_biz, c_red = gather_mentions()
    merged = {}
    for k, v in c_biz.items():
        merged[k] = merged.get(k, 0) + v
    for k, v in c_red.items():
        merged[k] = merged.get(k, 0) + v

    # history
    history = load_elite_history()
    trim_snapshots(history)

    prev_counts = {}
    if history["snapshots"]:
        prev_counts = history["snapshots"][-1].get("counts", {}) or {}

    # append snapshot
    history["snapshots"].append({
        "ts": n,
        "counts": merged,
        "biz": c_biz,
        "reddit": c_red
    })
    trim_snapshots(history)
    save_elite_history(history)

    rows = []
    for tk, curr in merged.items():
        if curr < MIN_MENTIONS_TO_SURFACE:
            continue

        prev = prev_counts.get(tk, 0)
        delta = curr - prev
        mom = compute_momentum(history, tk, points=6)
        new_flag = is_new_ticker(history, tk)
        biz_ct = c_biz.get(tk, 0)
        red_ct = c_red.get(tk, 0)

        spike = False
        if delta >= SPIKE_ABS_DELTA:
            spike = True
        if prev > 0 and curr >= prev * SPIKE_MULTIPLIER:
            spike = True

        info = classify_and_enrich(tk, cg_map)
        if not info:
            continue

        asset_type = info.get("type") or "Unknown"
        cap = info.get("cap")

        rows.append({
            "ticker": tk,
            "asset_type": asset_type,
            "name": info.get("name", tk),
            "cap": cap,
            "desc": info.get("desc", ""),
            "curr": curr,
            "prev": prev,
            "delta": delta,
            "momentum": mom,
            "new": new_flag,
            "spike": spike,
            "biz": biz_ct,
            "reddit": red_ct,
            "chg24": info.get("chg24"),
            "volume": info.get("volume"),
        })

    # ---------- ELITE ----------
    elite_ranked = []
    for r in rows:
        sc = elite_score(r["curr"], r["prev"], r["momentum"], r["new"], r["biz"], r["reddit"], r["asset_type"], r["cap"])
        elite_ranked.append((sc, r))
    elite_ranked.sort(key=lambda x: x[0], reverse=True)

    elite_items = []
    top_elite = elite_ranked[:25]
    for idx, (sc, r) in enumerate(top_elite):
        cap_str = fmt_money(r["cap"])
        cross = (r["biz"] > 0 and r["reddit"] > 0)

        flags = []
        if r["new"]:
            flags.append("NEW")
        if r["spike"]:
            flags.append("SPIKE")
        if cross:
            flags.append("CROSS")
        flag_txt = ("[" + " ".join(flags) + "] ") if flags else ""

        if r["asset_type"] == "Stock":
            link = f"https://finance.yahoo.com/quote/{r['ticker']}"
        else:
            link = f"https://www.coingecko.com/en/search?query={r['ticker']}"

        body = []
        body.append(f"<h2>{flag_txt}${html.escape(r['ticker'])} — {html.escape(r['name'])}</h2>")
        body.append(f"<p><b>Type:</b> {html.escape(r['asset_type'])}</p>")
        body.append(f"<p><b>Market Cap:</b> {html.escape(cap_str)}</p>")
        body.append(f"<p><b>Mentions:</b> {r['curr']} (prev {r['prev']}, Δ {r['delta']}, momentum {r['momentum']:.2f})</p>")
        body.append(f"<p><b>Sources:</b> /biz/ {r['biz']} • reddit {r['reddit']}</p>")
        body.append(f"<p><b>Elite score:</b> {sc:.2f}</p>")
        if r.get("desc"):
            body.append(f"<p>{html.escape(r['desc'])}</p>")
        body.append(f"<p><a href='{link}'>Open</a></p>")

        pub_ts = n - (len(top_elite) - idx)  # preserve ranked order when sorted oldest->newest
        elite_items.append({
            "title": f"{flag_txt}{r['ticker']} — {r['asset_type']} — {cap_str} — Δ{r['delta']} (m{r['momentum']:.1f})",
            "link": link,
            "guid": f"elite-{r['asset_type']}-{r['ticker']}-{n}",
            "pub_ts": pub_ts,
            "pubDate": rfc822(pub_ts),
            "description": "Open for details",
            "content_html": "".join(body),
        })

    write_rss("Alpha Dashboard — ELITE (quality, risk-on)",
              "https://boards.4chan.org/biz/",
              "Ranked signal with type + market cap (oldest→newest list order)",
              elite_items,
              FEED_ELITE)

    # ---------- ASYMMETRIC ----------
    asym_ranked = []
    for r in rows:
        sc = asymmetry_score(r["curr"], r["prev"], r["momentum"], r["new"], r["biz"], r["reddit"], r["asset_type"], r["cap"])
        asym_ranked.append((sc, r))
    asym_ranked.sort(key=lambda x: x[0], reverse=True)

    # Bias selection: mostly stocks, some crypto
    stocks = [(sc, r) for sc, r in asym_ranked if r["asset_type"] == "Stock"]
    cryptos = [(sc, r) for sc, r in asym_ranked if r["asset_type"] == "Crypto"]

    chosen = stocks[:14] + cryptos[:6]
    chosen = chosen[:20]

    asym_items = []
    for idx, (sc, r) in enumerate(chosen):
        cap_str = fmt_money(r["cap"])
        cross = (r["biz"] > 0 and r["reddit"] > 0)
        why = build_why_asymmetric(
            r["asset_type"],
            r["cap"],
            r["new"],
            r["spike"],
            cross,
            r["delta"],
            r["momentum"],
            r["curr"]
        )

        flags = []
        if r["new"]:
            flags.append("NEW")
        if r["spike"]:
            flags.append("SPIKE")
        if cross:
            flags.append("CROSS")
        flag_txt = ("[" + " ".join(flags) + "] ") if flags else ""

        if r["asset_type"] == "Stock":
            link = f"https://finance.yahoo.com/quote/{r['ticker']}"
        else:
            link = f"https://www.coingecko.com/en/search?query={r['ticker']}"

        body = []
        body.append(f"<h2>{flag_txt}MOST ASYMMETRIC — ${html.escape(r['ticker'])} — {html.escape(r['name'])}</h2>")
        body.append(f"<p><b>Why it’s asymmetric:</b> {html.escape(why)}</p>")
        body.append(f"<p><b>Asymmetry score:</b> {sc:.2f}</p>")
        body.append(f"<p><b>Sources:</b> /biz/ {r['biz']} • reddit {r['reddit']}</p>")
        body.append(f"<p><a href='{link}'>Open</a></p>")
        if r.get("desc"):
            body.append(f"<hr><p>{html.escape(r['desc'])}</p>")

        pub_ts = n - (len(chosen) - idx)
        asym_items.append({
            "title": f"{flag_txt}{r['ticker']} — WHY: {why}",
            "link": link,
            "guid": f"asym-{r['asset_type']}-{r['ticker']}-{n}",
            "pub_ts": pub_ts,
            "pubDate": rfc822(pub_ts),
            "description": "Why it’s asymmetric inside",
            "content_html": "".join(body),
        })

    write_rss("Most Asymmetric Plays (risk-on)",
              "https://boards.4chan.org/biz/",
              "Why it’s asymmetric included (oldest→newest list order)",
              asym_items,
              FEED_ASYM)

    # ---------- CRYPTO 100x / BREAKOUT MEMES ----------
    crypto_candidates = []
    for r in rows:
        if r["asset_type"] != "Crypto":
            continue

        cap = r.get("cap")
        vol = r.get("volume")
        chg24 = r.get("chg24")

        if not isinstance(cap, (int, float)) or cap is None:
            continue
        if cap < CRYPTO_MIN_MCAP or cap > CRYPTO_MAX_MCAP:
            continue
        if not isinstance(vol, (int, float)) or vol < CRYPTO_MIN_VOLUME:
            continue

        name_u = (r.get("name") or "").upper()
        sym_u = (r.get("ticker") or "").upper()
        meme_flag = any(k in name_u or k in sym_u for k in MEME_KEYWORDS)

        # breakout bias: 24h change and/or volume/cap ratio
        chg = float(chg24) if isinstance(chg24, (int, float)) else 0.0
        vcr = (vol / cap) if cap > 0 and isinstance(vol, (int, float)) else 0.0

        # incorporate mention wakeup too
        wake = (r["delta"] * 2.2) + (r["momentum"] * 1.6) + (r["curr"] * 0.25)

        # scoring: meme + low cap + breakout/volume + waking up
        score = wake
        score += (2.0 if meme_flag else 0.0)
        score += (max(0.0, chg - CRYPTO_BREAKOUT_PCT_24H) / 10.0)  # breakout boost
        score += min(2.0, vcr)  # liquidity/attention proxy
        score *= cap_factor_for_asym("Crypto", cap)

        crypto_candidates.append((score, meme_flag, chg, vcr, r))

    crypto_candidates.sort(key=lambda x: x[0], reverse=True)
    top_crypto = crypto_candidates[:25]

    crypto_items = []
    for idx, (sc, meme_flag, chg, vcr, r) in enumerate(top_crypto):
        cap_str = fmt_money(r["cap"])
        cross = (r["biz"] > 0 and r["reddit"] > 0)

        flags = []
        if meme_flag:
            flags.append("MEME")
        if chg >= CRYPTO_BREAKOUT_PCT_24H:
            flags.append(f"BREAKOUT {chg:.0f}%")
        if cross:
            flags.append("CROSS")
        flag_txt = ("[" + " | ".join(flags) + "] ") if flags else ""

        link = f"https://www.coingecko.com/en/search?query={r['ticker']}"

        why = f"mcap {cap_str} • vol/cap {vcr:.2f} • 24h {chg:.1f}% • Δ{r['delta']} • mom {r['momentum']:.2f} • mentions {r['curr']}"

        body = []
        body.append(f"<h2>{flag_txt}{html.escape(r['ticker'])} — {html.escape(r['name'])}</h2>")
        body.append(f"<p><b>Why it’s in Crypto 100x:</b> {html.escape(why)}</p>")
        body.append(f"<p><b>Crypto score:</b> {sc:.2f}</p>")
        body.append(f"<p><b>Sources:</b> /biz/ {r['biz']} • reddit {r['reddit']}</p>")
        body.append(f"<p><a href='{link}'>Open</a></p>")

        pub_ts = n - (len(top_crypto) - idx)
        crypto_items.append({
            "title": f"{flag_txt}{r['ticker']} — WHY: {why}",
            "link": link,
            "guid": f"crypto100x-{r['ticker']}-{n}",
            "pub_ts": pub_ts,
            "pubDate": rfc822(pub_ts),
            "description": "Open for breakout/meme rationale",
            "content_html": "".join(body),
        })

    write_rss("Crypto Alpha — Breakout Memes & Low Cap 100x",
              "https://boards.4chan.org/biz/",
              "Low-cap + meme/breakout bias, with 'why' (oldest→newest list order)",
              crypto_items,
              FEED_CRYPTO)


# ----------------------------
# Dashboard feed (simple pointers)
# ----------------------------

def generate_alpha_dashboard_feed():
    n = now_ts()
    items = []

    # Keep two pointers, ordered oldest->newest by pub_ts
    items.append({
        "title": "[ELITE] feed-alpha-elite.xml",
        "link": f"https://donicusmcginty.github.io/biz-active-rss/{FEED_ELITE}",
        "guid": f"dash-elite-{n}",
        "pub_ts": n - 2,
        "pubDate": rfc822(n - 2),
        "description": "Open article",
        "content_html": "<p>Your main enriched signal feed: <b>feed-alpha-elite.xml</b></p>",
    })
    items.append({
        "title": "[ASYM] feed-alpha-asymmetric.xml",
        "link": f"https://donicusmcginty.github.io/biz-active-rss/{FEED_ASYM}",
        "guid": f"dash-asym-{n}",
        "pub_ts": n - 1,
        "pubDate": rfc822(n - 1),
        "description": "Open article",
        "content_html": "<p>Ranked asymmetric plays with <b>why</b>: <b>feed-alpha-asymmetric.xml</b></p>",
    })
    items.append({
        "title": "[CRYPTO 100x] feed-crypto-100x.xml",
        "link": f"https://donicusmcginty.github.io/biz-active-rss/{FEED_CRYPTO}",
        "guid": f"dash-crypto-{n}",
        "pub_ts": n,
        "pubDate": rfc822(n),
        "description": "Open article",
        "content_html": "<p>Breakout meme / low-cap crypto feed with <b>why</b>: <b>feed-crypto-100x.xml</b></p>",
    })

    write_rss("Alpha Dashboard",
              "https://boards.4chan.org/biz/",
              "Pointers to Elite, Asymmetric, Crypto 100x",
              items,
              FEED_DASH)


# ----------------------------
# Main
# ----------------------------

def main():
    # Thread feeds
    generate_biz_active()
    generate_biz_fast()
    generate_biz_tickers()
    generate_biz_hisignal()

    # Equity feed
    generate_microcap_feed()

    # Signal feeds (elite + asymmetric + crypto100x)
    generate_elite_and_asym_and_crypto()

    # Dashboard pointers
    generate_alpha_dashboard_feed()


if __name__ == "__main__":
    main()