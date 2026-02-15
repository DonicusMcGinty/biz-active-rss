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

HISTORY_FILE = "microcap_history.json"

MAX_MARKET_CAP = 2_500_000_000
TOP_COUNT = 12
VALID_EXCHANGES = {"NASDAQ", "NYSE", "AMEX"}

TICKER_REGEX = r"\b[A-Z]{2,5}\b"

THREAD_LIMIT = 12
LAST_REPLIES = 30


# ----------------------------
# Utilities
# ----------------------------

def rfc822(ts):
    return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%a, %d %b %Y %H:%M:%S %z")


def strip_html(s):
    if not s:
        return ""
    s = s.replace("<br>", "\n")
    s = re.sub(r"<[^>]+>", "", s)
    return html.unescape(s).strip()


def fetch_json(url, headers=None):
    r = requests.get(url, headers=headers or {}, timeout=12)
    if r.status_code != 200:
        return None
    try:
        return r.json()
    except:
        return None


def fetch_catalog():
    return fetch_json(f"https://a.4cdn.org/{BOARD}/catalog.json")


def fetch_thread(no):
    data = fetch_json(f"https://a.4cdn.org/{BOARD}/thread/{no}.json")
    return data.get("posts", []) if data else None


def extract_tickers(text):
    return re.findall(TICKER_REGEX, text or "")


def thread_velocity(t, now):
    replies = t.get("replies", 0)
    last = t.get("last_modified", t.get("time", now))
    hours = max((now - last) / 3600, 0.25)
    return replies / hours


# ----------------------------
# RSS writer
# ----------------------------

def write_rss(title, link, desc, items, filename):
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
        ET.SubElement(item, "description").text = it["description"]

        content = ET.SubElement(item, "{http://purl.org/rss/1.0/modules/content/}encoded")
        content.text = it["content"]

        if it.get("image"):
            enc = ET.SubElement(item, "enclosure")
            enc.set("url", it["image"])
            enc.set("type", "image/jpeg")
            enc.set("length", "0")

    ET.ElementTree(rss).write(filename, encoding="utf-8", xml_declaration=True)


# ----------------------------
# Build inline thread item
# ----------------------------

def build_thread_item(t, posts, prefix=""):
    now = int(datetime.now(timezone.utc).timestamp())
    no = t["no"]
    url = f"https://boards.4chan.org/{BOARD}/thread/{no}"
    subject = strip_html(t.get("sub")) or f"Thread {no}"
    replies = t.get("replies", 0)
    created = t.get("time", now)

    op_text = strip_html(posts[0].get("com"))

    reply_posts = posts[1:]
    reply_posts = reply_posts[-LAST_REPLIES:]
    reply_posts.reverse()

    body = []
    body.append("<h2>" + html.escape(prefix + subject) + "</h2>")
    body.append("<p><a href='" + url + "'>Open thread</a> • Replies: " + str(replies) + "</p>")
    body.append("<hr><h3>OP</h3>")
    body.append("<p>" + html.escape(op_text).replace("\n", "<br>") + "</p>")
    body.append("<hr><h3>Latest replies</h3>")

    for p in reply_posts:
        txt = strip_html(p.get("com"))
        if not txt:
            continue
        body.append(
            "<p><b>" + str(p.get("no")) + "</b><br>" +
            html.escape(txt).replace("\n", "<br>") +
            "</p><hr>"
        )

    image = None
    if "tim" in t:
        image = f"https://i.4cdn.org/{BOARD}/{t['tim']}s.jpg"

    return {
        "title": prefix + subject + " — " + str(replies) + " replies",
        "link": url,
        "guid": url,
        "pubDate": rfc822(created),
        "description": op_text[:280],
        "content": "".join(body),
        "image": image,
    }


# ----------------------------
# Active feed
# ----------------------------

def generate_biz_feed():
    catalog = fetch_catalog()
    if not catalog:
        return

    now = int(datetime.now(timezone.utc).timestamp())
    threads = [t for page in catalog for t in page["threads"]]
    threads.sort(key=lambda x: thread_velocity(x, now), reverse=True)

    items = []
    for t in threads[:THREAD_LIMIT]:
        posts = fetch_thread(t["no"])
        if posts:
            items.append(build_thread_item(t, posts))

    write_rss(
        "/biz/ Active Threads",
        f"https://boards.4chan.org/{BOARD}/",
        "Active threads with inline replies",
        items,
        "feed-biz.xml",
    )


# ----------------------------
# FAST feed
# ----------------------------

def generate_biz_fast_feed():
    catalog = fetch_catalog()
    if not catalog:
        return

    now = int(datetime.now(timezone.utc).timestamp())
    threads = [t for page in catalog for t in page["threads"]]
    threads.sort(key=lambda x: thread_velocity(x, now), reverse=True)

    items = []
    for t in threads[:THREAD_LIMIT]:
        if t.get("replies", 0) < 25:
            continue
        posts = fetch_thread(t["no"])
        if posts:
            vel = thread_velocity(t, now)
            prefix = "[FAST " + format(vel, ".1f") + "/hr] "
            items.append(build_thread_item(t, posts, prefix))

    write_rss(
        "/biz/ FAST Threads",
        f"https://boards.4chan.org/{BOARD}/",
        "Threads gaining replies rapidly",
        items,
        "feed-biz-fast.xml",
    )


# ----------------------------
# Ticker feed
# ----------------------------

def generate_biz_ticker_feed():
    catalog = fetch_catalog()
    if not catalog:
        return

    threads = [t for page in catalog for t in page["threads"]]

    items = []
    for t in threads:
        text = (t.get("sub", "") or "") + " " + (t.get("com", "") or "")
        tks = extract_tickers(text)
        if not tks:
            continue

        posts = fetch_thread(t["no"])
        if posts:
            prefix = "[" + " ".join(sorted(set(tks))[:5]) + "] "
            items.append(build_thread_item(t, posts, prefix))

        if len(items) >= THREAD_LIMIT:
            break

    write_rss(
        "/biz/ Ticker Threads",
        f"https://boards.4chan.org/{BOARD}/",
        "Threads mentioning stock tickers",
        items,
        "feed-biz-tickers.xml",
    )


# ----------------------------
# Microcap feed (unchanged)
# ----------------------------

def load_history():
    if not os.path.exists(HISTORY_FILE):
        return {}
    with open(HISTORY_FILE) as f:
        return json.load(f)


def save_history(data):
    with open(HISTORY_FILE, "w") as f:
        json.dump(data, f)


def validate_ticker(ticker):
    try:
        prof = fetch_json(
            f"https://financialmodelingprep.com/api/v3/profile/{ticker}?apikey={FMP_API_KEY}"
        )
        if not prof:
            return None

        p = prof[0]
        if p.get("exchangeShortName") not in VALID_EXCHANGES:
            return None

        cap = p.get("mktCap")
        if not cap or cap > MAX_MARKET_CAP:
            return None

        opt = fetch_json(
            f"https://query2.finance.yahoo.com/v7/finance/options/{ticker}"
        )
        result = (opt or {}).get("optionChain", {}).get("result")
        if not result or not result[0].get("expirationDates"):
            return None

        return {
            "ticker": ticker,
            "name": p.get("companyName"),
            "mktCap": cap,
            "description": (p.get("description") or "")[:240],
        }
    except:
        return None


def generate_microcap_feed():
    mentions = {}

    catalog = fetch_catalog()
    if catalog:
        for page in catalog:
            for t in page["threads"]:
                text = (t.get("sub", "") or "") + " " + (t.get("com", "") or "")
                for tk in extract_tickers(text):
                    mentions[tk] = mentions.get(tk, 0) + 1

    prev = load_history()
    accel = {k: v for k, v in mentions.items() if prev.get(k, 0) < v}
    save_history(mentions)

    validated = []
    for tk, count in accel.items():
        info = validate_ticker(tk)
        if not info:
            continue
        score = count / math.log(info["mktCap"])
        validated.append((score, tk, info))

    validated.sort(reverse=True)

    now = int(datetime.now(timezone.utc).timestamp())
    items = []

    for score, tk, info in validated[:TOP_COUNT]:
        items.append({
            "title": f"{tk} — Score {score:.2f}",
            "link": f"https://finance.yahoo.com/quote/{tk}",
            "guid": tk,
            "pubDate": rfc822(now),
            "description": f"{info['name']} • ${info['mktCap']:,}",
            "content": info["description"],
            "image": None,
        })

    write_rss(
        "Microcap Equities Alpha",
        "https://finance.yahoo.com",
        "Accelerating microcap mentions",
        items,
        "feed-microcap.xml",
    )


def main():
    generate_biz_feed()
    generate_biz_fast_feed()
    generate_biz_ticker_feed()
    generate_microcap_feed()


if __name__ == "__main__":
    main()
