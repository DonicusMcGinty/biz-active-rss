import requests
import re
import math
import os
import html
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
import json

ET.register_namespace("content", "http://purl.org/rss/1.0/modules/content/")

FMP_API_KEY = os.getenv("FMP_API_KEY")

# ===== OUTPUT FILES =====

FEED_ACTIVE = "feed-biz-active.xml"
FEED_ASYM   = "feed-alpha-asymmetric.xml"
FEED_PRE    = "feed-prebreakout.xml"
FEED_TOP    = "feed-top-opportunities.xml"

HISTORY_FILE = "mention_history.json"

# ===== MARKET CAP WINDOW =====

MIN_CAP = 50_000_000
MAX_CAP = 2_500_000_000

# ===== SETTINGS =====

MAX_TICKERS_TO_VALIDATE = 80

TICKER_REGEX = r"\b[A-Z]{2,5}\b"
VALID_EXCHANGES = {"NASDAQ", "NYSE", "AMEX"}

BLACKLIST = {
    "USD","USDT","USDC","CEO","CFO","SEC","FED","FOMC",
    "NYSE","NASDAQ","AMEX","ETF","IPO","AI","DD",
    "IMO","LOL","YOLO","FOMO","HODL","ATH","TLDR"
}

# -------------------------------------------------
# TIME / HTTP
# -------------------------------------------------

def now():
    return int(datetime.now(timezone.utc).timestamp())

def rfc(ts):
    return datetime.fromtimestamp(ts, tz=timezone.utc)\
        .strftime("%a, %d %b %Y %H:%M:%S %z")

def fetch_json(url):
    try:
        r = requests.get(url, timeout=12)
        if r.status_code == 200:
            return r.json()
    except:
        pass
    return None

# -------------------------------------------------
# ACTIVE /biz/ FEED (no generals)
# -------------------------------------------------

def thread_velocity(t, now_ts):
    replies = t.get("replies", 0)
    last = t.get("last_modified", t.get("time", now_ts))
    hours = max((now_ts - last) / 3600.0, 0.25)
    return replies / hours

def build_active_feed():

    catalog = fetch_json("https://a.4cdn.org/biz/catalog.json")
    if not catalog:
        return

    n = now()

    threads = []
    for page in catalog:
        threads.extend(page.get("threads", []))

    filtered = []
    for t in threads:
        title = (t.get("sub") or "").lower()
        if "general" in title:
            continue
        filtered.append(t)

    filtered.sort(key=lambda x: thread_velocity(x, n), reverse=True)

    items = []

    for t in filtered[:15]:

        no = t["no"]
        url = f"https://boards.4chan.org/biz/thread/{no}"
        subject = html.escape(t.get("sub") or f"Thread {no}")
        replies = t.get("replies", 0)

        body = (
            f"<h2>{subject}</h2>"
            f"<p><b>Replies:</b> {replies}</p>"
            f"<p><a href='{url}'>Open thread</a></p>"
        )

        items.append({
            "title": f"{subject} — {replies} replies",
            "link": url,
            "guid": f"{no}-{n}",
            "date": rfc(n),
            "body": body
        })

    write_rss("/biz/ Active (no generals)", items, FEED_ACTIVE)

# -------------------------------------------------
# STOCK VALIDATION
# -------------------------------------------------

def fmp_profile(tk):
    if not FMP_API_KEY:
        return None
    return fetch_json(
        f"https://financialmodelingprep.com/api/v3/profile/{tk}?apikey={FMP_API_KEY}"
    )

def optionable(tk):
    data = fetch_json(
        f"https://query2.finance.yahoo.com/v7/finance/options/{tk}"
    )
    res = (data or {}).get("optionChain", {}).get("result")
    return bool(res and res[0].get("expirationDates"))

def validate_stock(tk):

    prof = fmp_profile(tk)
    if not prof:
        return None

    p = prof[0]

    exch = p.get("exchangeShortName")
    if exch not in VALID_EXCHANGES:
        return None

    cap = p.get("mktCap")
    if not cap or cap < MIN_CAP or cap > MAX_CAP:
        return None

    if not optionable(tk):
        return None

    return {
        "ticker": tk,
        "name": p.get("companyName") or tk,
        "cap": cap,
        "desc": (p.get("description") or "")[:240]
    }

# -------------------------------------------------
# /biz/ MENTIONS
# -------------------------------------------------

def gather_mentions():

    catalog = fetch_json("https://a.4cdn.org/biz/catalog.json")
    counts = {}

    if not catalog:
        return counts

    for page in catalog:
        for t in page.get("threads", []):
            text = (t.get("sub","") or "") + " " + (t.get("com","") or "")
            for tk in re.findall(TICKER_REGEX, text):
                if 2 <= len(tk) <= 5 and tk not in BLACKLIST:
                    counts[tk] = counts.get(tk, 0) + 1

    return counts

# -------------------------------------------------
# HISTORY
# -------------------------------------------------

def load_history():
    if not os.path.exists(HISTORY_FILE):
        return {}
    try:
        return json.load(open(HISTORY_FILE))
    except:
        return {}

def save_history(data):
    json.dump(data, open(HISTORY_FILE,"w"))

# -------------------------------------------------
# RSS WRITER
# -------------------------------------------------

def write_rss(title, items, file):

    rss = ET.Element("rss", version="2.0")
    ch = ET.SubElement(rss, "channel")

    ET.SubElement(ch, "title").text = title
    ET.SubElement(ch, "link").text = "https://boards.4chan.org/biz/"
    ET.SubElement(ch, "description").text = title
    ET.SubElement(ch, "lastBuildDate").text = rfc(now())

    for it in items:
        item = ET.SubElement(ch, "item")

        ET.SubElement(item, "title").text = it["title"]
        ET.SubElement(item, "link").text = it["link"]
        ET.SubElement(item, "guid").text = it["guid"]
        ET.SubElement(item, "pubDate").text = it["date"]
        ET.SubElement(item, "description").text = "Open"

        c = ET.SubElement(
            item,
            "{http://purl.org/rss/1.0/modules/content/}encoded"
        )
        c.text = it["body"]

    ET.ElementTree(rss).write(file, encoding="utf-8", xml_declaration=True)

# -------------------------------------------------
# OPPORTUNITY FEEDS
# -------------------------------------------------

def build_opportunity_feeds():

    curr = gather_mentions()
    prev = load_history()

    ranked = sorted(curr.items(), key=lambda x: x[1], reverse=True)
    ranked = ranked[:MAX_TICKERS_TO_VALIDATE]

    validated = {}

    for tk, _ in ranked:
        info = validate_stock(tk)
        if info:
            validated[tk] = info

    n = now()

    asym_rows = []
    pre_rows = []

    for tk, info in validated.items():

        m = curr.get(tk, 0)
        p = prev.get(tk, 0)
        cap = info["cap"]

        asym_score = m * (MAX_CAP / cap)
        asym_rows.append((asym_score, tk, info, m))

        if 2 <= m <= 15:
            delta = m - p
            if delta > 0:
                pre_score = delta * (MAX_CAP / cap) / math.log(m + 1)
                pre_rows.append((pre_score, tk, info, m, delta))

    asym_rows.sort(reverse=True)
    pre_rows.sort(reverse=True)

    asym_items = []
    for sc, tk, info, m in asym_rows[:25]:
        cap = f"${info['cap']:,}"
        why = f"ASYM • cap {cap} • mentions {m} • score {sc:.2f}"

        body = (
            f"<h2>${tk} — {html.escape(info['name'])}</h2>"
            f"<p><b>Why it’s asymmetric:</b> {why}</p>"
            f"<p>{html.escape(info['desc'])}</p>"
        )

        asym_items.append({
            "title": f"{tk} — WHY: {why}",
            "link": f"https://finance.yahoo.com/quote/{tk}",
            "guid": f"{tk}-asym-{n}",
            "date": rfc(n),
            "body": body
        })

    write_rss("Asymmetric Plays (50M–2.5B)", asym_items, FEED_ASYM)

    pre_items = []
    for sc, tk, info, m, d in pre_rows[:20]:
        cap = f"${info['cap']:,}"
        why = f"PRE • mentions {m} (+{d}) • cap {cap}"

        body = (
            f"<h2>${tk} — {html.escape(info['name'])}</h2>"
            f"<p><b>Pre-breakout signal:</b> {why}</p>"
            f"<p>{html.escape(info['desc'])}</p>"
        )

        pre_items.append({
            "title": f"{tk} — PRE-BREAKOUT — {why}",
            "link": f"https://finance.yahoo.com/quote/{tk}",
            "guid": f"{tk}-pre-{n}",
            "date": rfc(n),
            "body": body
        })

    write_rss("Pre-Breakout Detector (50M–2.5B)", pre_items, FEED_PRE)

    combined = asym_rows[:15] + pre_rows[:15]

    top_items = []
    for row in combined[:25]:

        if len(row) == 4:
            sc, tk, info, m = row
            d = 0
            tag = "ASYM"
        else:
            sc, tk, info, m, d = row
            tag = "PRE"

        cap = f"${info['cap']:,}"
        why = f"{tag} • cap {cap} • mentions {m}" + (f" (+{d})" if d else "")

        body = (
            f"<h2>${tk} — {html.escape(info['name'])}</h2>"
            f"<p><b>Why it’s top:</b> {why}</p>"
            f"<p>{html.escape(info['desc'])}</p>"
        )

        top_items.append({
            "title": f"{tk} — WHY: {why}",
            "link": f"https://finance.yahoo.com/quote/{tk}",
            "guid": f"{tk}-top-{n}",
            "date": rfc(n),
            "body": body
        })

    write_rss("Top Opportunities Now (50M–2.5B)", top_items, FEED_TOP)

    save_history(curr)

# -------------------------------------------------
# MAIN
# -------------------------------------------------

def main():

    build_active_feed()
    build_opportunity_feeds()

if __name__ == "__main__":
    main()