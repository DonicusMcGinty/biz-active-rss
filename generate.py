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


# ----------------------------
# Helpers
# ----------------------------

def fetch_biz_catalog():
    response = requests.get(f"https://a.4cdn.org/{BOARD}/catalog.json", timeout=10)
    return response.json()


def fetch_reddit(subreddit):
    headers = {
        "User-Agent": "Mozilla/5.0 (compatible; MicrocapScanner/1.0)"
    }
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


def build_rss(_
