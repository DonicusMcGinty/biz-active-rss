import requests
import re
import json
import math
import os
import xml.etree.ElementTree as ET

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

    for page in pages:
        threads.extend(page["threads"])

    sorted_threads = sorted(
        threads,
        key=lambda x: x.get("replies", 0),
        reverse=True
    )[:20]

    rss = ET.Element("rss", version="2.0")
    channel = ET.SubElement(rss, "channel")

    ET.SubElement(channel, "title").text = "/biz/ Active Threads"
    ET.SubElement(channel, "link").text = "https://boards.4chan.org/biz/"
    ET.SubElement(channel, "description").text = "Top active threads"

    for t in sorted_threads:
        thread_url = f"https://boards.4chan.org/biz/thread/{t['no']}"

        item = ET.SubElement(channel, "item")

        ET.SubElement(item, "title").text = f"Thread {t['no']} — {t.get('replies', 0)} replies"
        ET.SubElement(item, "link").text = thread_url
        ET.SubElement(item, "guid").text = thread_url
        ET.SubElement(item, "description").text = t.get("sub", "")

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
