import requests
import re
import json
import math
import os
import html
import time
import xml.etree.ElementTree as ET
from datetime import datetime, timezone

ET.register_namespace("content", "http://purl.org/rss/1.0/modules/content/")

# ----------------------------
# Constants
# ----------------------------

FMP_API_KEY = os.getenv("FMP_API_KEY")
BOARD = "biz"

# Output feeds
FEED_ACTIVE = "feed-biz.xml"
FEED_FAST = "feed-biz-fast.xml"
FEED_TICKERS = "feed-biz-tickers.xml"
FEED_HISIGNAL = "feed-biz-alpha.xml"
FEED_MICROCAP = "feed-microcap.xml"
FEED_DASH = "feed-alpha-dashboard.xml"
FEED_ELITE = "feed-alpha-elite.xml"

# History / cache
MICROCAP_HISTORY_FILE = "microcap_history.json"      # existing microcap accel history
ELITE_HISTORY_FILE = "elite_history.json"            # new snapshot history for elite scoring
COINGECKO_CACHE_FILE = "coingecko_cache.json"        # daily cache of top coins by market cap

# Ticker extraction
TICKER_REGEX = r"\b[A-Z]{2,5}\b"

# /biz/ inline replies behavior (fixed)
THREAD_LIMIT = 12
LAST_REPLIES = 30

# Quality filters for stocks (your spec)
VALID_EXCHANGES = {"NASDAQ", "NYSE", "AMEX"}
MAX_MARKET_CAP_STOCK = 2_500_000_000  # <= $2.5B
REQUIRE_OPTIONABLE = True

# Elite analytics windows
SNAPSHOT_INTERVAL_SECONDS = 15 * 60  # workflow runs every 15m
SNAPSHOT_KEEP_HOURS = 48            # keep last 48h of snapshots
NEW_TICKER_LOOKBACK_HOURS = 24      # "new" if not seen in last 24h

# Elite thresholds (kept conservative for quality)
MIN_MENTIONS_TO_SURFACE = 2
SPIKE_ABS_DELTA = 4           # absolute delta vs previous snapshot
SPIKE_MULTIPLIER = 2.5        # current >= prev * multiplier (when prev > 0)

# Basic noise blacklist
TICKER_BLACKLIST = {
    "USD", "USDT", "USDC", "BTC", "ETH", "SOL", "XRP", "BNB", "DOGE",  # handled separately as crypto
    "CEO", "CFO", "SEC", "FED", "FOMC", "USA", "EU", "UK",
    "NYSE", "NASDAQ", "AMEX", "ETF", "IPO", "AI", "DD",
    "IMO", "LOL", "YOLO", "FOMO", "HODL", "ATH", "TLDR"
}

# Reddit sources for signal confirmation
REDDIT_SUBS = ["pennystocks", "wallstreetbets"]


# ----------------------------
# Small helpers
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


def safe_get(url: str, headers=None, timeout=12):
    try:
        r = requests.get(url, headers=headers or {}, timeout=timeout)
        return r
    except:
        return None


def fetch_json(url: str, headers=None, timeout=12):
    r = safe_get(url, headers=headers, timeout=timeout)
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
    # Avoid 2-letter noise a bit, but keep it permissive
    if len(tk) < 2 or len(tk) > 5:
        return False
    return True


# ----------------------------
# 4chan + reddit fetchers
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
# RSS writer
# ----------------------------

def write_rss(title: str, link: str, desc: str, items: list, filename: str):
    rss = ET.Element("rss", version="2.0")
    channel = ET.SubElement(rss, "channel")

    ET.SubElement(channel, "title").text = title
    ET.SubElement(channel, "link").text = link
    ET.SubElement(channel, "description").text = desc

    for it in items:
        item = ET.SubElement(channel, "item")

        ET.SubElement(item, "title").text = it["title"]
        ET.SubElement(item, "link").text = it["link"]
        ET.SubElement(item, "guid").text = it["guid"]
        ET.SubElement(item, "pubDate").text = it["pubDate"]

        # Keep description minimal so Reeder uses content:encoded
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
# /biz/ inline thread item builder
# ----------------------------

def build_thread_item(t: dict, posts: list, prefix: str = "") -> dict:
    n = now_ts()
    no = t["no"]
    url = f"https://boards.4chan.org/{BOARD}/thread/{no}"
    subject = strip_html(t.get("sub")) or f"Thread {no}"
    replies = t.get("replies", 0)
    created = t.get("time", n)

    op_text = strip_html(posts[0].get("com"))

    reply_posts = posts[1:]
    reply_posts = reply_posts[-LAST_REPLIES:]
    reply_posts.reverse()  # newest first

    body = []
    body.append(f"<h2>{html.escape(prefix + subject)}</h2>")
    body.append(f"<p><a href='{url}'>Open thread</a> • Replies: {replies}</p>")
    body.append("<hr><h3>OP</h3>")
    body.append(f"<p>{html.escape(op_text).replace(chr(10), '<br>')}</p>")
    body.append("<hr><h3>Latest replies</h3>")

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
        "guid": url,
        "pubDate": rfc822(created),
        "description": "Open article for full thread",
        "content_html": "".join(body),
        "enclosure_url": thumb,
        "enclosure_type": "image/jpeg",
    }


# ----------------------------
# Existing feeds (kept)
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
              "Active threads with inline OP + last 30 replies",
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
              "Rapidly moving threads (inline replies)",
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
              "Threads mentioning tickers (inline replies)",
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
# Microcap feed (acceleration) - kept (stocks only)
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


def generate_microcap_feed():
    catalog = fetch_catalog()
    if not catalog:
        return

    mentions = {}

    # /biz/ mentions
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
    for score, tk, info, count in validated[:12]:
        cap_str = f"${info['cap']:,}"
        body = (
            f"<h2>${tk} — {html.escape(info['name'])}</h2>"
            f"<p><b>Type:</b> Stock</p>"
            f"<p><b>Market Cap:</b> {cap_str}</p>"
            f"<p><b>Mentions (accelerating):</b> {count}</p>"
            f"<p>{html.escape(info['desc'])}</p>"
            f"<p><a href='https://finance.yahoo.com/quote/{tk}'>Yahoo Finance</a></p>"
        )
        items.append({
            "title": f"{tk} — {cap_str} — accel {count}",
            "link": f"https://finance.yahoo.com/quote/{tk}",
            "guid": f"{tk}-microcap",
            "pubDate": rfc822(n),
            "description": "Open article",
            "content_html": body
        })

    write_rss("Microcap Equities Alpha",
              "https://boards.4chan.org/biz/",
              "Accelerating mentions (US exchange, <2.5B, optionable)",
              items,
              FEED_MICROCAP)


# ----------------------------
# CoinGecko cache (top coins only, refreshed daily)
# ----------------------------

def get_coingecko_symbol_map():
    cache = load_json_file(COINGECKO_CACHE_FILE, {})
    n = now_ts()
    last = cache.get("ts", 0)

    # refresh daily
    if (n - last) < 24 * 3600 and "coins" in cache:
        coins = cache["coins"]
    else:
        # pull top 250 coins by market cap
        url = "https://api.coingecko.com/api/v3/coins/markets"
        params = {
            "vs_currency": "usd",
            "order": "market_cap_desc",
            "per_page": 250,
            "page": 1,
            "sparkline": "false"
        }
        try:
            r = requests.get(url, params=params, timeout=15)
            coins = r.json() if r.status_code == 200 else []
        except:
            coins = []

        save_json_file(COINGECKO_CACHE_FILE, {"ts": n, "coins": coins})

    sym_map = {}
    for c in coins or []:
        sym = (c.get("symbol") or "").upper()
        if not sym:
            continue
        # prefer first seen (top cap)
        if sym not in sym_map:
            sym_map[sym] = {
                "type": "Crypto",
                "ticker": sym,
                "name": c.get("name") or sym,
                "cap": c.get("market_cap"),
            }
    return sym_map


# ----------------------------
# Elite history & scoring
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


def snapshot_counts(c_biz, c_red):
    # Merge counts, but keep per-source for explanation
    merged = {}
    for k, v in c_biz.items():
        merged[k] = merged.get(k, 0) + v
    for k, v in c_red.items():
        merged[k] = merged.get(k, 0) + v
    return merged


def get_recent_snapshots(history, lookback_hours):
    n = now_ts()
    lb = lookback_hours * 3600
    return [s for s in history.get("snapshots", []) if (n - s.get("ts", 0)) <= lb]


def compute_momentum(history, ticker, points=4):
    snaps = history.get("snapshots", [])
    if len(snaps) < 2:
        return 0.0
    # take last 'points' snapshots with counts
    last = snaps[-points:]
    series = []
    for s in last:
        c = s.get("counts", {}).get(ticker, 0)
        series.append(c)
    if len(series) < 2:
        return 0.0
    # momentum = (latest - oldest) / (len-1)
    return (series[-1] - series[0]) / max(len(series) - 1, 1)


def is_new_ticker(history, ticker):
    recent = get_recent_snapshots(history, NEW_TICKER_LOOKBACK_HOURS)
    for s in recent:
        if s.get("counts", {}).get(ticker, 0) > 0:
            return False
    return True


def elite_score(curr, prev, momentum, is_new, biz_count, red_count):
    delta = curr - prev
    # quality bias: cross-source > single-source
    cross = 1.25 if (biz_count > 0 and red_count > 0) else 1.0
    new_bonus = 2.0 if is_new else 0.0
    # delta and momentum do the heavy lifting
    return cross * (delta * 2.0 + curr * 0.5 + momentum * 1.5 + new_bonus)


# ----------------------------
# Elite Alpha Feed
# ----------------------------

def gather_mentions():
    catalog = fetch_catalog()
    c_biz = {}
    c_red = {}

    # /biz/ catalog mentions (fast)
    if catalog:
        for page in catalog:
            for t in page.get("threads", []):
                text = (t.get("sub", "") or "") + " " + (t.get("com", "") or "")
                for tk in extract_tickers(text):
                    if plausible_ticker(tk):
                        c_biz[tk] = c_biz.get(tk, 0) + 1

    # reddit mentions
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
    # Prefer stocks first (quality)
    s = validate_stock_us_microcap_optionable(ticker)
    if s:
        return s

    # Crypto only from top CG set (quality)
    if ticker in cg_map:
        return cg_map[ticker]

    return None


def generate_elite_alpha_feed():
    n = now_ts()
    cg_map = get_coingecko_symbol_map()

    c_biz, c_red = gather_mentions()
    merged = snapshot_counts(c_biz, c_red)

    history = load_elite_history()
    trim_snapshots(history)

    # determine prev snapshot counts
    prev_counts = {}
    if history["snapshots"]:
        prev_counts = history["snapshots"][-1].get("counts", {}) or {}

    # append snapshot first (so momentum uses consistent series next run)
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
        mom = compute_momentum(history, tk, points=5)
        new_flag = is_new_ticker(history, tk)
        biz_ct = c_biz.get(tk, 0)
        red_ct = c_red.get(tk, 0)

        # spike logic
        spike = False
        if delta >= SPIKE_ABS_DELTA:
            spike = True
        if prev > 0 and curr >= prev * SPIKE_MULTIPLIER and curr >= MIN_MENTIONS_TO_SURFACE:
            spike = True

        info = classify_and_enrich(tk, cg_map)
        if not info:
            continue  # quality: ignore unverified tickers

        sc = elite_score(curr, prev, mom, new_flag, biz_ct, red_ct)

        rows.append({
            "ticker": tk,
            "asset_type": info.get("type"),
            "name": info.get("name", tk),
            "cap": info.get("cap"),
            "curr": curr,
            "prev": prev,
            "delta": delta,
            "momentum": mom,
            "new": new_flag,
            "spike": spike,
            "biz": biz_ct,
            "reddit": red_ct,
            "score": sc,
            "desc": info.get("desc", "")
        })

    rows.sort(key=lambda r: r["score"], reverse=True)
    top = rows[:25]

    items = []
    for r in top:
        cap = r["cap"]
        cap_str = f"${cap:,}" if isinstance(cap, (int, float)) and cap is not None else "Unknown"

        flags = []
        if r["new"]:
            flags.append("NEW")
        if r["spike"]:
            flags.append("SPIKE")
        if r["biz"] > 0 and r["reddit"] > 0:
            flags.append("CROSS")

        flag_txt = ("[" + " ".join(flags) + "] ") if flags else ""

        # link choice
        if r["asset_type"] == "Stock":
            link = f"https://finance.yahoo.com/quote/{r['ticker']}"
        else:
            # generic coingecko search
            link = f"https://www.coingecko.com/en/search?query={r['ticker']}"

        body = []
        body.append(f"<h2>{flag_txt}${html.escape(r['ticker'])} — {html.escape(r['name'])}</h2>")
        body.append(f"<p><b>Type:</b> {html.escape(r['asset_type'] or 'Unknown')}</p>")
        body.append(f"<p><b>Market Cap:</b> {html.escape(cap_str)}</p>")
        body.append(f"<p><b>Mentions:</b> {r['curr']} (prev {r['prev']}, Δ {r['delta']}, momentum {r['momentum']:.2f})</p>")
        body.append(f"<p><b>Sources:</b> /biz/ {r['biz']} • reddit {r['reddit']}</p>")
        if r.get("desc"):
            body.append(f"<p>{html.escape(r['desc'])}</p>")
        body.append(f"<p><a href='{link}'>Open</a></p>")

        items.append({
            "title": f"{flag_txt}{r['ticker']} — {r['asset_type']} — {cap_str} — Δ{r['delta']} (m{r['momentum']:.1f})",
            "link": link,
            "guid": f"elite-{r['asset_type']}-{r['ticker']}",
            "pubDate": rfc822(n),
            "description": "Open for details",
            "content_html": "".join(body),
        })

    write_rss("Alpha Dashboard — ELITE (quality)",
              "https://boards.4chan.org/biz/",
              "New + spiking + momentum tickers, enriched with type + market cap (quality filters)",
              items,
              FEED_ELITE)


# ----------------------------
# Simple dashboard (kept)
# ----------------------------

def generate_alpha_dashboard_feed():
    # Keep a lightweight combined feed (threads + microcap list)
    n = now_ts()
    items = []

    # pull top /biz/ threads
    catalog = fetch_catalog()
    if catalog:
        threads = [t for p in catalog for t in p.get("threads", [])]
        threads.sort(key=lambda x: thread_velocity(x, n), reverse=True)
        for t in threads[:6]:
            posts = fetch_thread(t["no"])
            if posts:
                items.append(build_thread_item(t, posts, "[BIZ] "))

    # microcap headline items (from last written file state isn't accessible; just a placeholder)
    # keeps the feed alive but doesn't duplicate elite logic
    items.append({
        "title": "[ELITE] Use the Elite feed",
        "link": f"https://donicusmcginty.github.io/biz-active-rss/{FEED_ELITE}",
        "guid": f"dash-elite-pointer",
        "pubDate": rfc822(n),
        "description": "Open article",
        "content_html": "<p>Your enriched, high-quality signals are in <b>feed-alpha-elite.xml</b>.</p>",
    })

    write_rss("Alpha Dashboard",
              "https://boards.4chan.org/biz/",
              "Combined chatter + pointers",
              items,
              FEED_DASH)


# ----------------------------
# Main
# ----------------------------

def main():
    # keep your existing feeds
    generate_biz_active()
    generate_biz_fast()
    generate_biz_tickers()
    generate_biz_hisignal()
    generate_microcap_feed()
    generate_alpha_dashboard_feed()

    # elite quality feed
    generate_elite_alpha_feed()


if __name__ == "__main__":
    main()