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

FEED_ACTIVE = "feed-biz.xml"
FEED_FAST = "feed-biz-fast.xml"
FEED_TICKERS = "feed-biz-tickers.xml"
FEED_HISIGNAL = "feed-biz-alpha.xml"
FEED_MICROCAP = "feed-microcap.xml"
FEED_DASH = "feed-alpha-dashboard.xml"
FEED_ELITE = "feed-alpha-elite.xml"

MICROCAP_HISTORY_FILE = "microcap_history.json"
ELITE_HISTORY_FILE = "elite_history.json"
COINGECKO_CACHE_FILE = "coingecko_cache.json"

TICKER_REGEX = r"\b[A-Z]{2,5}\b"

THREAD_LIMIT = 12
LAST_REPLIES = 30

VALID_EXCHANGES = {"NASDAQ", "NYSE", "AMEX"}
MAX_MARKET_CAP_STOCK = 2_500_000_000
REQUIRE_OPTIONABLE = True

SNAPSHOT_KEEP_HOURS = 48
NEW_TICKER_LOOKBACK_HOURS = 24

MIN_MENTIONS_TO_SURFACE = 2
SPIKE_ABS_DELTA = 4
SPIKE_MULTIPLIER = 2.5

REDDIT_SUBS = ["pennystocks", "wallstreetbets"]

TICKER_BLACKLIST = {
    "USD", "USDT", "USDC", "CEO", "CFO", "SEC", "FED", "FOMC", "USA", "EU", "UK",
    "NYSE", "NASDAQ", "AMEX", "ETF", "IPO", "AI", "DD", "IMO", "LOL", "YOLO",
    "FOMO", "HODL", "ATH", "TLDR"
}


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
# RSS writer (adds lastBuildDate)
# ----------------------------

def write_rss(title: str, link: str, desc: str, items: list, filename: str):
    rss = ET.Element("rss", version="2.0")
    channel = ET.SubElement(rss, "channel")

    ET.SubElement(channel, "title").text = title
    ET.SubElement(channel, "link").text = link
    ET.SubElement(channel, "description").text = desc
    ET.SubElement(channel, "lastBuildDate").text = rfc822(now_ts())

    for it in items:
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
# Thread item builder (pubDate + guid use last_modified)
# ----------------------------

def build_thread_item(t: dict, posts: list, prefix: str = "") -> dict:
    n = now_ts()
    no = t["no"]
    url = f"https://boards.4chan.org/{BOARD}/thread/{no}"
    subject = strip_html(t.get("sub")) or f"Thread {no}"
    replies = t.get("replies", 0)

    last_mod = t.get("last_modified", t.get("time", n))
    pub = last_mod  # IMPORTANT: makes feed feel "live"
    guid = f"{url}?lm={last_mod}"  # IMPORTANT: forces Reeder to recognize updates

    op_text = strip_html(posts[0].get("com"))

    reply_posts = posts[1:]
    reply_posts = reply_posts[-LAST_REPLIES:]
    reply_posts.reverse()

    body = []
    body.append(f"<h2>{html.escape(prefix + subject)}</h2>")
    body.append(f"<p><a href='{url}'>Open thread</a> • Replies: {replies} • Updated: {datetime.fromtimestamp(last_mod, tz=timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}</p>")
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
        "guid": guid,
        "pubDate": rfc822(pub),
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
# CoinGecko cache (top 250)
# ----------------------------

def get_coingecko_symbol_map():
    cache = load_json_file(COINGECKO_CACHE_FILE, {})
    n = now_ts()
    last = cache.get("ts", 0)

    if (n - last) < 24 * 3600 and "coins" in cache:
        coins = cache["coins"]
    else:
        url = "https://api.coingecko.com/api/v3/coins/markets"
        params = {
            "vs_currency": "usd",
            "order": "market_cap_desc",
            "per_page": 250,
            "page": 1,
            "sparkline": "false"
        }
        r = safe_get(url, timeout=15, params=params)
        coins = r.json() if (r and r.status_code == 200) else []
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
                "desc": ""
            }
    return sym_map


# ----------------------------
# Elite history
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


def compute_momentum(history, ticker, points=5):
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

    if catalog:
        for page in catalog:
            for t in page.get("threads", []):
                text = (t.get("sub", "") or "") + " " + (t.get("com", "") or "")
                for tk in extract_tickers(text):
                    if plausible_ticker(tk):
                        c_biz[tk] = c_biz.get(tk, 0) + 1

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
    s = validate_stock_us_microcap_optionable(ticker)
    if s:
        return s
    if ticker in cg_map:
        return cg_map[ticker]
    return None


# ----------------------------
# Elite scoring (risk-on bias)
# ----------------------------

def small_cap_boost(asset_type, cap):
    # Risk-on microcap bias:
    # Stocks: boost under $500M, extra under $200M
    if asset_type != "Stock":
        # crypto: mild boost for < $2B, extra < $500M
        if isinstance(cap, (int, float)) and cap:
            if cap < 500_000_000:
                return 1.35
            if cap < 2_000_000_000:
                return 1.15
        return 1.0

    if not isinstance(cap, (int, float)) or not cap:
        return 1.0
    if cap < 200_000_000:
        return 1.55
    if cap < 500_000_000:
        return 1.35
    if cap < 1_000_000_000:
        return 1.15
    return 1.0


def elite_score(curr, prev, momentum, is_new, biz_count, red_count, asset_type, cap):
    delta = curr - prev

    cross = 1.6 if (biz_count > 0 and red_count > 0) else 1.0
    new_bonus = 2.0 if is_new else 0.0

    # Core: delta + momentum matter most for “it’s waking up”
    base = (delta * 2.2) + (curr * 0.35) + (momentum * 1.6) + new_bonus

    return base * cross * small_cap_boost(asset_type, cap)


# ----------------------------
# Elite Alpha feed
# ----------------------------

def generate_elite_alpha_feed():
    n = now_ts()
    cg_map = get_coingecko_symbol_map()

    c_biz, c_red = gather_mentions()
    merged = {}
    for k, v in c_biz.items():
        merged[k] = merged.get(k, 0) + v
    for k, v in c_red.items():
        merged[k] = merged.get(k, 0) + v

    history = load_elite_history()
    trim_snapshots(history)

    prev_counts = {}
    if history["snapshots"]:
        prev_counts = history["snapshots"][-1].get("counts", {}) or {}

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

        sc = elite_score(curr, prev, mom, new_flag, biz_ct, red_ct, asset_type, cap)

        rows.append({
            "ticker": tk,
            "asset_type": asset_type,
            "name": info.get("name", tk),
            "cap": cap,
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
        if r.get("desc"):
            body.append(f"<p>{html.escape(r['desc'])}</p>")
        body.append(f"<p><a href='{link}'>Open</a></p>")

        items.append({
            "title": f"{flag_txt}{r['ticker']} — {r['asset_type']} — {cap_str} — Δ{r['delta']} (m{r['momentum']:.1f})",
            "link": link,
            "guid": f"elite-{r['asset_type']}-{r['ticker']}-{n}",  # updates every run
            "pubDate": rfc822(n),
            "description": "Open for details",
            "content_html": "".join(body),
        })

    write_rss("Alpha Dashboard — ELITE (quality, risk-on)",
              "https://boards.4chan.org/biz/",
              "New + spiking + momentum tickers, enriched with type + market cap (risk-on microcap bias)",
              items,
              FEED_ELITE)


# ----------------------------
# /biz/ feeds
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
# Microcap feed (kept)
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
            "guid": f"{tk}-microcap-{n}",
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
# Simple dashboard (kept)
# ----------------------------

def generate_alpha_dashboard_feed():
    n = now_ts()
    items = []

    catalog = fetch_catalog()
    if catalog:
        threads = [t for p in catalog for t in p.get("threads", [])]
        threads.sort(key=lambda x: thread_velocity(x, n), reverse=True)
        for t in threads[:6]:
            posts = fetch_thread(t["no"])
            if posts:
                items.append(build_thread_item(t, posts, "[BIZ] "))

    items.append({
        "title": "[ELITE] Use the Elite feed",
        "link": f"https://donicusmcginty.github.io/biz-active-rss/{FEED_ELITE}",
        "guid": f"dash-elite-pointer-{n}",
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
    generate_biz_active()
    generate_biz_fast()
    generate_biz_tickers()
    generate_biz_hisignal()
    generate_microcap_feed()
    generate_alpha_dashboard_feed()
    generate_elite_alpha_feed()


if __name__ == "__main__":
    main()
