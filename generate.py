import requests
import re
import json
import math
import os
import xml.etree.ElementTree as ET
from datetime import datetime

FMP_API_KEY = os.getenv("FMP_API_KEY")
BOARD = "biz"

HISTORY_FILE = "microcap_history.json"

# ---------- CONFIG ----------
MAX_MARKET_CAP = 2_500_000_000
TOP_COUNT = 12
TICKER_REGEX = r'\b[A-Z]{2,5}\b'
VALID_EXCHANGES = {"NASDAQ", "NYSE", "AMEX"}
# ----------------------------

def fetch_biz_threads():
    data = requests.get(f"https://a.4cdn.org/{BOARD}/catalog.json").json()
    threads = []
    for page in data:
        threads.extend(page["threads"])
    return threads

def fetch_reddit(subreddit):
    headers = {
        "User-Agent": "Mozilla/5.0 (compatible; MicrocapScanner/1.0)"
    }
    url = f"https://www.reddit.com/r/{subreddit}/new.json?limit=100"

    try:
        response = requests.get(url, headers=headers, timeout=10)
        if response.status_code != 200:
            return None
        return response.json()
    except:
        return None

def extract_tickers(text):
    if not text:
        return []
    return re.findall(TICKER_REGEX, text)

def validate_ticker(ticker):
    try:
        url = f"https://financialmodelingprep.com/api/v3/profile/{ticker}?apikey={FMP_API_KEY}"
        data = requests.get(url).json()
        if not data:
            return None

        profile = data[0]
        if profile["exchangeShortName"] not in VALID_EXCHANGES:
            return None
        if profile["mktCap"] is None or profile["mktCap"] > MAX_MARKET_CAP:
            return None

        # Check options chain via Yahoo
        opt_url = f"https://query2.finance.yahoo.com/v7/finance/options/{ticker}"
        opt_data = requests.get(opt_url).json()
        if "expirationDates" not in opt_data["optionChain"]["result"][0]:
            return None

        return {
            "ticker": ticker,
            "name": profile["companyName"],
            "mktCap": profile["mktCap"],
            "description": profile["description"]
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
            f"Business: {item['description'][:200]}..."
        )

    ET.ElementTree(rss).write(filename, encoding="utf-8", xml_declaration=True)

def load_history():
    if not os.path.exists(HISTORY_FILE):
        return {}
    with open(HISTORY_FILE, "r") as f:
        return json.load(f)

def save_history(data):
    with open(HISTORY_FILE, "w") as f:
        json.dump(data, f)

def generate_microcap_feed():
    mentions = {}

    # --- biz ---
    biz_threads = fetch_biz_threads()
    for thread in biz_threads:
        text = thread.get("sub", "") + " " + thread.get("com", "")
        for t in extract_tickers(text):
            mentions[t] = mentions.get(t, 0) + 1

    # --- reddit ---
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

def generate_biz_feed():
    threads = fetch_biz_threads()
    sorted_threads = sorted(threads, key=lambda x: x.get("replies", 0), reverse=True)[:20]

    items = []
    for t in sorted_threads:
        items.append({
            "ticker": f"/biz/ Thread {t['no']}",
            "score": t.get("replies", 0),
            "name": "",
            "mktCap": 0,
            "description": t.get("sub", "")
        })

    build_rss(items, "feed-biz.xml", "/biz/ Active Threads")

def main():
    generate_biz_feed()
    generate_microcap_feed()

if __name__ == "__main__":
    main()
