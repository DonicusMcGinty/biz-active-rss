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

# Fixed behaviour — no configuration
BIZ_THREADS_TO_INCLUDE = 12
LAST_REPLIES = 30


# ----------------------------
# Helpers
# ----------------------------

def rfc822(ts):
    return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%a, %d %b %Y %H:%M:%S %z")


def strip_html(s):
    if not s:
        return ""
    s = s.replace("<br>", "\n")
    s = re.sub(r"<[^>]+>", "", s)
    s = html.unescape(s)
    return s.strip()


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


def fetch_reddit(sub):
    headers = {"User-Agent": "Mozilla/5.0"}
    return fetch_json(f"https://www.reddit.com/r/{sub}/new.json?limit=100", headers=headers)


def extract_tickers(text):
    return re.findall(TICKER_REGEX, text or "")


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
            "
