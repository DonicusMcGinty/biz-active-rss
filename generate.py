# === ONLY SHOWING CHANGED / NEW CONSTANTS + FUNCTIONS ===
# Replace your existing file with this whole version

import requests
import re
import json
import math
import os
import html
import xml.etree.ElementTree as ET
from datetime import datetime, timezone

ET.register_namespace("content", "http://purl.org/rss/1.0/modules/content/")

FMP_API_KEY = os.getenv("FMP_API_KEY")
BOARD = "biz"

FEED_ASYM = "feed-alpha-asymmetric.xml"

TICKER_REGEX = r"\b[A-Z]{2,5}\b"

# --- NEW: nano-cap window ---
ASYM_MIN_CAP = 5_000_000
ASYM_MAX_CAP = 25_000_000

LAST_REPLIES = 30
THREAD_LIMIT = 12

VALID_EXCHANGES = {"NASDAQ", "NYSE", "AMEX"}
REQUIRE_OPTIONABLE = True

TICKER_BLACKLIST = {
    "USD","USDT","USDC","CEO","CFO","SEC","FED","FOMC",
    "NYSE","NASDAQ","ETF","IPO","AI","DD","IMO","LOL",
    "YOLO","FOMO","HODL","ATH","TLDR"
}

# ----------------------------
# Utilities
# ----------------------------

def now_ts():
    return int(datetime.now(timezone.utc).timestamp())

def rfc822(ts):
    return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%a, %d %b %Y %H:%M:%S %z")

def strip_html(s):
    if not s: return ""
    s = s.replace("<br>", "\n")
    s = re.sub(r"<[^>]+>", "", s)
    return html.unescape(s).strip()

def fetch_json(url):
    try:
        r = requests.get(url, timeout=12)
        if r.status_code == 200:
            return r.json()
    except:
        pass
    return None

def extract_tickers(text):
    return re.findall(TICKER_REGEX, text or "")

def plausible_ticker(tk):
    if tk in TICKER_BLACKLIST:
        return False
    return 2 <= len(tk) <= 5

# ----------------------------
# Stock validation
# ----------------------------

def fmp_profile(tk):
    if not FMP_API_KEY:
        return None
    return fetch_json(
        f"https://financialmodelingprep.com/api/v3/profile/{tk}?apikey={FMP_API_KEY}"
    )

def yahoo_optionable(tk):
    data = fetch_json(f"https://query2.finance.yahoo.com/v7/finance/options/{tk}")
    res = (data or {}).get("optionChain", {}).get("result")
    if not res:
        return False
    return bool(res[0].get("expirationDates"))

def validate_nano_stock(tk):
    prof = fmp_profile(tk)
    if not prof:
        return None
    p = prof[0]

    exch = p.get("exchangeShortName")
    if exch not in VALID_EXCHANGES:
        return None

    cap = p.get("mktCap")
    if not cap or cap < ASYM_MIN_CAP or cap > ASYM_MAX_CAP:
        return None

    if REQUIRE_OPTIONABLE and not yahoo_optionable(tk):
        return None

    return {
        "ticker": tk,
        "name": p.get("companyName") or tk,
        "cap": cap,
        "desc": (p.get("description") or "")[:240]
    }

# ----------------------------
# Gather mentions from /biz/
# ----------------------------

def gather_mentions():
    catalog = fetch_json(f"https://a.4cdn.org/{BOARD}/catalog.json")
    mentions = {}

    if not catalog:
        return mentions

    for page in catalog:
        for t in page.get("threads", []):
            text = (t.get("sub","") or "") + " " + (t.get("com","") or "")
            for tk in extract_tickers(text):
                if plausible_ticker(tk):
                    mentions[tk] = mentions.get(tk, 0) + 1

    return mentions, catalog

# ----------------------------
# Build asymmetric feed
# ----------------------------

def generate_asymmetric():
    mentions, catalog = gather_mentions()

    rows = []

    for tk, count in mentions.items():
        info = validate_nano_stock(tk)
        if not info:
            continue

        cap = info["cap"]

        # Simple asymmetry score
        score = count * (25_000_000 / cap)

        rows.append((score, tk, info, count))

    rows.sort(reverse=True)

    n = now_ts()
    items = []

    for score, tk, info, count in rows[:25]:
        cap_str = f"${info['cap']:,}"

        why = f"nano cap {cap_str} • mentions {count} • score {score:.2f}"

        body = (
            f"<h2>${tk} — {html.escape(info['name'])}</h2>"
            f"<p><b>Why it’s asymmetric:</b> {html.escape(why)}</p>"
            f"<p>{html.escape(info['desc'])}</p>"
            f"<p><a href='https://finance.yahoo.com/quote/{tk}'>Open</a></p>"
        )

        items.append({
            "title": f"{tk} — WHY: {why}",
            "link": f"https://finance.yahoo.com/quote/{tk}",
            "guid": f"asym-nano-{tk}-{n}",
            "pubDate": rfc822(n),
            "description": "Open for details",
            "content_html": body,
        })

    write_rss(
        "Nano-cap Asymmetric Plays ($5M–$25M)",
        "https://boards.4chan.org/biz/",
        "Nano-cap asymmetric plays from /biz/ mentions",
        items,
        FEED_ASYM
    )

# ----------------------------
# RSS writer
# ----------------------------

def write_rss(title, link, desc, items, filename):
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
        ET.SubElement(item, "description").text = it["description"]

        content = ET.SubElement(
            item,
            "{http://purl.org/rss/1.0/modules/content/}encoded"
        )
        content.text = it["content_html"]

    ET.ElementTree(rss).write(filename, encoding="utf-8", xml_declaration=True)

# ----------------------------
# Main
# ----------------------------

def main():
    generate_asymmetric()

if __name__ == "__main__":
    main()