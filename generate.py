import requests
import re
import json
import math
import os
import xml.etree.ElementTree as ET

from datetime import datetime, timezone
import html

def strip_html(s: str) -> str:
    if not s:
        return ""
    # 4chan uses <br> for newlines
    s = s.replace("<br>", "\n").replace("<br/>", "\n").replace("<br />", "\n")
    # remove tags
    s = re.sub(r"<[^>]+>", "", s)
    # unescape entities
    s = html.unescape(s)
    # tidy whitespace
    s = re.sub(r"\n{3,}", "\n\n", s).strip()
    return s

def rfc822_from_epoch(ts: int) -> str:
    return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%a, %d %b %Y %H:%M:%S %z")

def biz_thumb_url(tim: int) -> str:
    # 4chan thumbnails are served as JPG and end with 's.jpg'
    return f"https://i.4cdn.org/{BOARD}/{tim}s.jpg"

def biz_image_url(tim: int, ext: str) -> str:
    return f"https://i.4cdn.org/{BOARD}/{tim}{ext}"

FMP_API_KEY = os.getenv("FMP_API_KEY")
BOARD = "biz"

HISTORY_FILE = "microcap_history.json"

MAX_MARKET_CAP = 2_500_000_000
TOP_COUNT = 12
TICKER_REGEX = r'\b[A-Z]{2,5}\b'
VALID_EXCHANGES = {"NASDAQ", "NYSE", "AMEX"}


def fetch_biz_catalog():
    r = requests.get(f"https://a.4cdn.org/{BOARD}/catalog.json", timeout=10)
    return r.json()


def fetch_reddit(subreddit):
    headers = {"User-Agent": "Mozilla/5.0 (MicrocapScanner)"}
    url = f"https://www.reddit.com/r/{subreddit}/new.json?limit=100"
    try:
        r = requests.get(url, headers=headers, timeout=10)
        if r.status_code != 200:
            return None
        return r.json()
    except:
        return None


def extract_tickers(text):
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


def validate_ticker(ticker):
    try:
        url = f"https://financialmodelingprep.com/api/v3/profile/{ticker}?apikey={FMP_API_KEY}"
        data = requests.get(url, timeout=10).json()
        if not data:
            return None

        p = data[0]

        if p.get("exchangeShortName") not in VALID_EXCHANGES:
            return None

        mkt_cap = p.get("mktCap")
        if not mkt_cap or mkt_cap > MAX_MARKET_CAP:
            return None

        opt_url = f"https://query2.finance.yahoo.com/v7/finance/options/{ticker}"
        opt_data = requests.get(opt_url, timeout=10).json()

        result = opt_data.get("optionChain", {}).get("result")
        if not result or not result[0].get("expirationDates"):
            return None

        return {
            "ticker": ticker,
            "name": p.get("companyName"),
            "mktCap": mkt_cap,
            "description": p.get("description", "")[:200]
        }

    except:
        return None


def build_rss(items, filename, title):
    rss = ET.Element("rss", version="2.0")
    channel = ET.SubElement(rss, "channel")

    ET.SubElement(channel, "title").text = title
    ET.SubElement(channel, "link").text = "https://boards.4chan.org/biz/"
    ET.SubElement(channel, "description").text = title

    for item in items:
        entry = ET.SubElement(channel, "item")
        ET.SubElement(entry, "title").text = f"{item['ticker']} — Score {item['score']:.2f}"
        ET.SubElement(entry, "guid").text = item["ticker"]
        ET.SubElement(entry, "description").text = (
            f"Company: {item['name']}\n"
            f"Market Cap: ${item['mktCap']:,}\n"
            f"Business: {item['description']}..."
        )

    ET.ElementTree(rss).write(filename, encoding="utf-8", xml_declaration=True)


def generate_biz_feed():
    pages = fetch_biz_catalog()
    threads = []
    now = int(datetime.now(tz=timezone.utc).timestamp())

    for page in pages:
        threads.extend(page["threads"])

    # rank by "active": replies per hour since last bump (rough)
    def activity_score(t):
        replies = t.get("replies", 0)
        last = t.get("last_modified", t.get("time", now))
        hours = max((now - last) / 3600.0, 0.25)  # floor to avoid insane spikes
        return replies / hours

    sorted_threads = sorted(threads, key=activity_score, reverse=True)[:25]

    rss = ET.Element("rss", version="2.0")
    channel = ET.SubElement(rss, "channel")

    ET.SubElement(channel, "title").text = "/biz/ Active Threads (with snippets)"
    ET.SubElement(channel, "link").text = f"https://boards.4chan.org/{BOARD}/"
    ET.SubElement(channel, "description").text = "Ranked by rough reply velocity; includes OP snippet + thumbs."

    for t in sorted_threads:
        thread_no = t["no"]
        thread_url = f"https://boards.4chan.org/{BOARD}/thread/{thread_no}"

        sub = strip_html(t.get("sub", "")) or "No subject"
        op_text = strip_html(t.get("com", ""))

        replies = t.get("replies", 0)
        images = t.get("images", 0)

        created = t.get("time", now)
        last_bump = t.get("last_modified", created)

        # Flags / metrics
        is_new = (now - created) <= 3600
        hours_since_bump = max((now - last_bump) / 3600.0, 0.25)
        rph = replies / hours_since_bump

        # Thumb / image
        thumb_html = ""
        if "tim" in t and "ext" in t:
            thumb = biz_thumb_url(t["tim"])
            full_img = biz_image_url(t["tim"], t["ext"])
            thumb_html = f'<p><a href="{full_img}"><img src="{thumb}" alt="thumb"></a></p>'

        # Build description (HTML tends to render fine in Reader)
        snippet = op_text[:500] + ("…" if len(op_text) > 500 else "")
        flags = []
        if is_new:
            flags.append("NEW")
        flags_txt = ("[" + " • ".join(flags) + "] ") if flags else ""

        desc = (
            f"{thumb_html}"
            f"<p><b>{flags_txt}{sub}</b></p>"
            f"<p>Replies: {replies} | Images: {images} | ~{rph:.1f} replies/hr</p>"
            f"<p>{html.escape(snippet).replace('\\n', '<br>')}</p>"
        )

        item = ET.SubElement(channel, "item")
        ET.SubElement(item, "title").text = f"{flags_txt}{sub} — {replies} replies"
        ET.SubElement(item, "link").text = thread_url
        ET.SubElement(item, "guid").text = thread_url
        ET.SubElement(item, "pubDate").text = rfc822_from_epoch(created)
        ET.SubElement(item, "description").text = desc
                # Add enclosure for Reeder (so it shows an image preview)
        if "tim" in t and "ext" in t:
            thumb = biz_thumb_url(t["tim"])
            enclosure = ET.SubElement(item, "enclosure")
            enclosure.set("url", thumb)
            enclosure.set("type", "image/jpeg")
            enclosure.set("length", "0")


    ET.ElementTree(rss).write("feed-biz.xml", encoding="utf-8", xml_declaration=True)


def generate_microcap_feed():
    mentions = {}

    pages = fetch_biz_catalog()
    for page in pages:
        for thread in page["threads"]:
            text = thread.get("sub", "") + " " + thread.get("com", "")
            for t in extract_tickers(text):
                mentions[t] = mentions.get(t, 0) + 1

    for sub in ["pennystocks", "wallstreetbets"]:
        reddit = fetch_reddit(sub)
        if not reddit:
            continue
        for post in reddit["data"]["children"]:
            text = post["data"]["title"]
            for t in extract_tickers(text):
                mentions[t] = mentions.get(t, 0) + 1

    previous = load_history()
    accelerating = {}

    for ticker, count in mentions.items():
        prev = previous.get(ticker, 0)
        if prev > 0 and count > prev:
            accelerating[ticker] = count

    save_history(mentions)

    validated = []

    for ticker, count in accelerating.items():
        data = validate_ticker(ticker)
        if not data:
            continue

        cap_factor = 1 / math.log(data["mktCap"])
        score = count * cap_factor

        data["score"] = score
        validated.append(data)

    ranked = sorted(validated, key=lambda x: x["score"], reverse=True)[:TOP_COUNT]

    build_rss(ranked, "feed-microcap.xml", "Microcap Equities Alpha")


def main():
    generate_biz_feed()
    generate_microcap_feed()


if __name__ == "__main__":
    main()
