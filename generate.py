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

# ===== Feeds =====
FEED_ASYM = "feed-alpha-asymmetric.xml"
FEED_PRE = "feed-prebreakout.xml"

# ===== Market cap window =====
MIN_CAP = 10_000_000
MAX_CAP = 250_000_000

# ===== Files =====
HISTORY_FILE = "mention_history.json"

# ===== Regex =====
TICKER_REGEX = r"\b[A-Z]{2,5}\b"

VALID_EXCHANGES = {"NASDAQ", "NYSE", "AMEX"}
REQUIRE_OPTIONABLE = True

BLACKLIST = {
    "USD","USDT","USDC","CEO","CFO","SEC","FED",
    "NYSE","NASDAQ","ETF","IPO","AI","DD","IMO",
    "LOL","YOLO","FOMO","HODL","ATH","TLDR"
}

# -----------------------------------------------------

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

def extract(text):
    return re.findall(TICKER_REGEX, text or "")

def ok_ticker(t):
    return 2 <= len(t) <= 5 and t not in BLACKLIST

# -----------------------------------------------------
# STOCK VALIDATION
# -----------------------------------------------------

def fmp_profile(tk):
    if not FMP_API_KEY:
        return None
    return fetch_json(
        f"https://financialmodelingprep.com/api/v3/profile/{tk}?apikey={FMP_API_KEY}"
    )

def yahoo_optionable(tk):
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

    if REQUIRE_OPTIONABLE and not yahoo_optionable(tk):
        return None

    return {
        "ticker": tk,
        "name": p.get("companyName") or tk,
        "cap": cap,
        "desc": (p.get("description") or "")[:240]
    }

# -----------------------------------------------------
# MENTIONS
# -----------------------------------------------------

def gather_mentions():
    catalog = fetch_json("https://a.4cdn.org/biz/catalog.json")
    counts = {}

    if not catalog:
        return counts

    for page in catalog:
        for t in page.get("threads", []):
            text = (t.get("sub","") or "") + " " + (t.get("com","") or "")
            for tk in extract(text):
                if ok_ticker(tk):
                    counts[tk] = counts.get(tk, 0) + 1

    return counts

# -----------------------------------------------------
# HISTORY
# -----------------------------------------------------

def load_history():
    if not os.path.exists(HISTORY_FILE):
        return {}
    try:
        return json.load(open(HISTORY_FILE))
    except:
        return {}

def save_history(data):
    json.dump(data, open(HISTORY_FILE,"w"))

# -----------------------------------------------------
# RSS
# -----------------------------------------------------

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

# -----------------------------------------------------
# ASYMMETRIC FEED
# -----------------------------------------------------

def gen_asymmetric(curr):
    rows = []

    for tk, count in curr.items():
        info = validate_stock(tk)
        if not info:
            continue

        cap = info["cap"]
        score = count * (MAX_CAP / cap)

        rows.append((score, tk, info, count))

    rows.sort(reverse=True)

    n = now()
    items = []

    for sc, tk, info, c in rows[:25]:
        cap = f"${info['cap']:,}"
        why = f"small cap {cap} • mentions {c} • score {sc:.2f}"

        body = (
            f"<h2>${tk} — {html.escape(info['name'])}</h2>"
            f"<p><b>Why it’s asymmetric:</b> {why}</p>"
            f"<p>{html.escape(info['desc'])}</p>"
        )

        items.append({
            "title": f"{tk} — WHY: {why}",
            "link": f"https://finance.yahoo.com/quote/{tk}",
            "guid": f"{tk}-asym-{n}",
            "date": rfc(n),
            "body": body
        })

    write_rss("Asymmetric Plays (10–250M)", items, FEED_ASYM)

# -----------------------------------------------------
# PRE-BREAKOUT FEED
# -----------------------------------------------------

def gen_prebreakout(curr, prev):
    rows = []

    for tk, c in curr.items():
        p = prev.get(tk, 0)

        if c < 2 or c > 15:
            continue  # ignore zero & already popular

        delta = c - p
        if delta <= 0:
            continue

        info = validate_stock(tk)
        if not info:
            continue

        cap = info["cap"]

        # early momentum score
        score = delta * (MAX_CAP / cap) / math.log(c + 1)

        rows.append((score, tk, info, c, delta))

    rows.sort(reverse=True)

    n = now()
    items = []

    for sc, tk, info, c, d in rows[:20]:
        cap = f"${info['cap']:,}"
        why = f"mentions {c} (+{d}) • cap {cap} • early momentum"

        body = (
            f"<h2>${tk} — {html.escape(info['name'])}</h2>"
            f"<p><b>Pre-breakout signal:</b> {why}</p>"
            f"<p>{html.escape(info['desc'])}</p>"
        )

        items.append({
            "title": f"{tk} — PRE-BREAKOUT — {why}",
            "link": f"https://finance.yahoo.com/quote/{tk}",
            "guid": f"{tk}-pre-{n}",
            "date": rfc(n),
            "body": body
        })

    write_rss("Pre-Breakout Detector (10–250M)", items, FEED_PRE)

# -----------------------------------------------------

def main():
    curr = gather_mentions()
    prev = load_history()

    gen_asymmetric(curr)
    gen_prebreakout(curr, prev)

    save_history(curr)

if __name__ == "__main__":
    main()