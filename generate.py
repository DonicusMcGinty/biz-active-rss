import requests
import re
import json
import math
import os
import html
import xml.etree.ElementTree as ET
from datetime import datetime, timezone

# Make Reeder respect full article content
ET.register_namespace("content", "http://purl.org/rss/1.0/modules/content/")

FMP_API_KEY = os.getenv("FMP_API_KEY")
BOARD = "biz"

HISTORY_FILE = "microcap_history.json"

MAX_MARKET_CAP = 2_500_000_000
TOP_COUNT = 12
VALID_EXCHANGES = {"NASDAQ", "NYSE", "AMEX"}

# Keep this conservative or you’ll get loads of false positives
TICKER_REGEX = r"\b[A-Z]{2,5}\b"

# How many replies to embed in the /biz/ item body (Reeder article view)
BIZ_REPLIES_TO_INCLUDE = 40
# How many /biz/ threads to include (we fetch each thread JSON, so keep it modest)
BIZ_THREADS_TO_INCLUDE = 12


# ----------------------------
# Helpers
# ----------------------------

def rfc822(ts: int) -> str:
    return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%a, %d %b %Y %H:%M:%S %z")


def strip_4chan_html(s: str) -> str:
    """Turn 4chan's HTML-ish comment body into readable plain text."""
    if not s:
        return ""
    s = s.replace("<br>", "\n").replace("<br/>", "\n").replace("<br />", "\n")
    s = re.sub(r"<[^>]+>", "", s)
    s = html.unescape(s)
    s = re.sub(r"\n{3,}", "\n\n", s).strip()
    return s


def fetch_json(url: str, headers=None, timeout=10):
    r = requests.get(url, headers=headers or {}, timeout=timeout)
    if r.status_code != 200:
        return None
    try:
        return r.json()
    except:
        return None


def fetch_biz_catalog():
    return fetch_json(f"https://a.4cdn.org/{BOARD}/catalog.json")


def fetch_thread_posts(thread_no: int):
    data = fetch_json(f"https://a.4cdn.org/{BOARD}/thread/{thread_no}.json")
    if not data:
        return None
    return data.get("posts", [])


def fetch_reddit(subreddit: str):
    headers = {"User-Agent": "Mozilla/5.0 (MicrocapScanner)"}
    return fetch_json(f"https://www.reddit.com/r/{subreddit}/new.json?limit=100", headers=headers)


def extract_tickers(text: str):
    if not text:
        return []
    return re.findall(TICKER_REGEX, text)


def load_history():
    if not os.path.exists(HISTORY_FILE):
        return {}
    with open(HISTORY_FILE, "r") as f:
        return json.load(f)


def save_history(data):
    with open(HISTORY_FILE, "w") as f:
        json.dump(data, f)


def validate_ticker(ticker: str):
    """US exchange-listed, <2.5B market cap, and optionable."""
    try:
        # FMP profile
        prof = fetch_json(f"https://financialmodelingprep.com/api/v3/profile/{ticker}?apikey={FMP_API_KEY}")
        if not prof:
            return None
        p = prof[0]

        if p.get("exchangeShortName") not in VALID_EXCHANGES:
            return None

        mkt_cap = p.get("mktCap")
        if not mkt_cap or mkt_cap > MAX_MARKET_CAP:
            return None

        # Yahoo options existence check
        opt = fetch_json(f"https://query2.finance.yahoo.com/v7/finance/options/{ticker}")
        result = (opt or {}).get("optionChain", {}).get("result")
        if not result or not result[0].get("expirationDates"):
            return None

        return {
            "ticker": ticker,
            "name": p.get("companyName") or ticker,
            "mktCap": mkt_cap,
            "description": (p.get("description") or "")[:220],
        }
    except:
        return None


# ----------------------------
# RSS builders
# ----------------------------

def write_rss(channel_title: str, channel_link: str, channel_desc: str, items: list, filename: str):
    rss = ET.Element("rss", version="2.0")
    channel = ET.SubElement(rss, "channel")

    ET.SubElement(channel, "title").text = channel_title
    ET.SubElement(channel, "link").text = channel_link
    ET.SubElement(channel, "description").text = channel_desc

    for it in items:
        item = ET.SubElement(channel, "item")
        ET.SubElement(item, "title").text = it["title"]
        ET.SubElement(item, "link").text = it["link"]
        ET.SubElement(item, "guid").text = it["guid"]
        if it.get("pubDate"):
            ET.SubElement(item, "pubDate").text = it["pubDate"]

        # Short summary
        ET.SubElement(item, "description").text = it.get("description", "")

        # Full content for Reeder
        if it.get("content_html"):
            content_encoded = ET.SubElement(item, "{http://purl.org/rss/1.0/modules/content/}encoded")
            content_encoded.text = it["content_html"]

        # Reeder-friendly preview image
        if it.get("enclosure_url"):
            enc = ET.SubElement(item, "enclosure")
            enc.set("url", it["enclosure_url"])
            enc.set("type", it.get("enclosure_type", "image/jpeg"))
            enc.set("length", "0")

    ET.ElementTree(rss).write(filename, encoding="utf-8", xml_declaration=True)


# ----------------------------
# Feed 1: /biz/ active threads with inline replies
# ----------------------------

def biz_thumb_url(tim: int) -> str:
    # 4chan thumbs are always tim + "s.jpg"
    return f"https://i.4cdn.org/{BOARD}/{tim}s.jpg"


def biz_thread_url(no: int) -> str:
    return f"https://boards.4chan.org/{BOARD}/thread/{no}"


def generate_biz_feed():
    catalog = fetch_biz_catalog()
    if not catalog:
        # still write a valid empty feed
        write_rss(
            "/biz/ Active Threads (inline)",
            f"https://boards.4chan.org/{BOARD}/",
            "Active threads with OP + recent replies embedded.",
            [],
            "feed-biz.xml",
        )
        return

    now = int(datetime.now(tz=timezone.utc).timestamp())
    threads = []
    for page in catalog:
        threads.extend(page.get("threads", []))

    # Rank by "reply velocity" (rough): replies / hours since last bump
    def activity_score(t):
        replies = t.get("replies", 0)
        last = t.get("last_modified", t.get("time", now))
        hours = max((now - last) / 3600.0, 0.25)
        return replies / hours

    top = sorted(threads, key=activity_score, reverse=True)[:BIZ_THREADS_TO_INCLUDE]

    items = []
    for t in top:
        no = t["no"]
        url = biz_thread_url(no)
        created = t.get("time", now)
        replies = t.get("replies", 0)

        subject = strip_4chan_html(t.get("sub", "")) or f"Thread {no}"
        op_snip = strip_4chan_html(t.get("com", ""))[:280]
        is_new = (now - created) <= 3600
        flag = "NEW " if is_new else ""

        # Fetch full thread posts to embed replies
        posts = fetch_thread_posts(no)
        if not posts:
            continue

        # Build HTML body: OP + last N replies
        op = posts[0]
        op_text = strip_4chan_html(op.get("com", ""))

        reply_posts = posts[1:]
        if BIZ_REPLIES_TO_INCLUDE and len(reply_posts) > BIZ_REPLIES_TO_INCLUDE:
            reply_posts = reply_posts[-BIZ_REPLIES_TO_INCLUDE:]

        body_parts = []
        body_parts.append(f"<h3>{html.escape(subject)}</h3>")
        body_parts.append(f"<p><a href=\"{url}\">Open thread</a> • Replies: {replies}</p>")
        body_parts.append(f"<hr><p><b>OP</b><br>{html.escape(op_text).replace('\\n', '<br>')}</p>")

        for p in reply_posts:
            com = strip_4chan_html(p.get("com", ""))
            if not com:
                continue
            pno = p.get("no", "")
            body_parts.append(f"<hr><p><b>{pno}</b><br>{html.escape(com).replace('\\n', '<br>')}</p>")

        content_html = "".join(body_parts)

        # Reeder preview image as enclosure (thumb)
        enclosure_url = None
        if "tim" in t and "ext" in t:
            enclosure_url = biz_thumb_url(t["tim"])

        items.append({
            "title": f"{flag}{subject} — {replies} replies",
            "link": url,
            "guid": url,
            "pubDate": rfc822(created),
            "description": op_snip,
            "content_html": content_html,
            "enclosure_url": enclosure_url,
            "enclosure_type": "image/jpeg",
        })

    write_rss(
        "/biz/ Active Threads (inline replies)",
        f"https://boards.4chan.org/{BOARD}/",
        "Active threads with OP + recent replies embedded for Reeder.",
        items,
        "feed-biz.xml",
    )


# ----------------------------
# Feed 2: Microcap equities alpha (acceleration)
# ----------------------------

def generate_microcap_feed():
    mentions = {}

    # /biz/ mentions
    catalog = fetch_biz_catalog()
    if catalog:
        for page in catalog:
            for thread in page.get("threads", []):
                text = (thread.get("sub", "") or "") + " " + (thread.get("com", "") or "")
                for tk in extract_tickers(text):
                    mentions[tk] = mentions.get(tk, 0) + 1

    # Reddit mentions (best-effort)
    for sub in ["pennystocks", "wallstreetbets"]:
        data = fetch_reddit(sub)
        if not data:
            continue
        for post in data.get("data", {}).get("children", []):
            title = post.get("data", {}).get("title", "")
            for tk in extract_tickers(title):
                mentions[tk] = mentions.get(tk, 0) + 1

    previous = load_history()
    accelerating = {}
    for tk, count in mentions.items():
        prev = previous.get(tk, 0)
        if prev > 0 and count > prev:
            accelerating[tk] = count

    # Save current for next hour comparison
    save_history(mentions)

    validated = []
    for tk, count in accelerating.items():
        info = validate_ticker(tk)
        if not info:
            continue

        # small-cap boost
        cap_factor = 1 / math.log(info["mktCap"])
        score = count * cap_factor

        validated.append({
            "ticker": tk,
            "name": info["name"],
            "mktCap": info["mktCap"],
            "description": info["description"],
            "score": score,
            "mentions": count,
        })

    validated.sort(key=lambda x: x["score"], reverse=True)
    top = validated[:TOP_COUNT]

    items = []
    now = int(datetime.now(tz=timezone.utc).timestamp())
    for x in top:
        items.append({
            "title": f"{x['ticker']} — Score {x['score']:.2f} (mentions {x['mentions']})",
            "link": f"https://finance.yahoo.com/quote/{x['ticker']}",
            "guid": x["ticker"],
            "pubDate": rfc822(now),
            "description": (
                f"Company: {x['name']}\n"
                f"Market Cap: ${x['mktCap']:,}\n"
                f"Business: {x['description']}..."
            ),
            "content_html": (
                f"<h3>{x['ticker']} — {html.escape(x['name'])}</h3>"
                f"<p><b>Mentions (accelerating):</b> {x['mentions']}</p>"
                f"<p><b>Market cap:</b> ${x['mktCap']:,}</p>"
                f"<p><b>Business:</b> {html.escape(x['description'])}</p>"
                f"<p><a href=\"https://finance.yahoo.com/quote/{x['ticker']}\">Open on Yahoo Finance</a></p>"
            ),
        })

    write_rss(
        "Microcap Equities Alpha",
        "https://boards.4chan.org/biz/",
        "Top 12 accelerating tickers (US exchange-listed, <2.5B, optionable).",
        items,
        "feed-microcap.xml",
    )


def main():
    generate_biz_feed()
    generate_microcap_feed()


if __name__ == "__main__":
    main()
