import requests
import re
import math
import os
import html
import json
import xml.etree.ElementTree as ET
from datetime import datetime, timezone

ET.register_namespace("content", "http://purl.org/rss/1.0/modules/content/")

# =========================
# CONFIG
# =========================

BOARD = "biz"
FMP_API_KEY = os.getenv("FMP_API_KEY")

# Output feeds
FEED_ACTIVE = "feed-biz-active.xml"
FEED_ASYM   = "feed-alpha-asymmetric.xml"
FEED_PRE    = "feed-prebreakout.xml"
FEED_TOP    = "feed-top-opportunities.xml"

# State files
MENTION_HISTORY_FILE = "mention_history.json"
THREAD_VEL_FILE = "thread_velocity.json"

# Stocks universe
MIN_CAP = 50_000_000
MAX_CAP = 2_500_000_000
VALID_EXCHANGES = {"NASDAQ", "NYSE", "AMEX"}

# Active feed behavior
ACTIVE_THREADS_LIMIT = 15
FIRST_REPLIES = 3
LAST_REPLIES = 30

# Mentions / tickers
TICKER_REGEX = r"\b[A-Z]{2,5}\b"
BLACKLIST = {
    "USD","USDT","USDC","CEO","CFO","SEC","FED","FOMC",
    "NYSE","NASDAQ","AMEX","ETF","IPO","AI","DD","IMO",
    "LOL","YOLO","FOMO","HODL","ATH","TLDR"
}

# Opportunity feed compute limits (avoid too many API calls)
MAX_TICKERS_TO_VALIDATE = 80

# Exploding detector sensitivity
# We boost if velocity increased meaningfully vs last run.
EXPLODE_ABS_DELTA = 6.0     # +6 replies/hr
EXPLODE_MULT = 1.6          # 1.6x increase


# =========================
# UTILS
# =========================

def now_ts() -> int:
    return int(datetime.now(timezone.utc).timestamp())

def rfc822(ts: int) -> str:
    return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%a, %d %b %Y %H:%M:%S %z")

def fetch_json(url: str, timeout: int = 15):
    try:
        r = requests.get(url, timeout=timeout)
        if r.status_code == 200:
            return r.json()
    except:
        pass
    return None

def strip_html(s: str) -> str:
    if not s:
        return ""
    s = s.replace("<br>", "\n")
    s = re.sub(r"<[^>]+>", "", s)
    return html.unescape(s).strip()

def load_json(path: str, default):
    try:
        if os.path.exists(path):
            with open(path, "r") as f:
                return json.load(f)
    except:
        pass
    return default

def save_json(path: str, data):
    with open(path, "w") as f:
        json.dump(data, f)

def fmt_money(x):
    if x is None or not isinstance(x, (int, float)):
        return "Unknown"
    return f"${int(x):,}"

def extract_tickers(text: str):
    return re.findall(TICKER_REGEX, text or "")

def plausible_ticker(tk: str) -> bool:
    if tk in BLACKLIST:
        return False
    return 2 <= len(tk) <= 5


# =========================
# RSS WRITER
# (preserve input order)
# =========================

def write_rss(title: str, link: str, desc: str, items: list, filename: str):
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
        ET.SubElement(item, "description").text = it.get("description", "Open")

        c = ET.SubElement(item, "{http://purl.org/rss/1.0/modules/content/}encoded")
        c.text = it.get("content_html", "")

    ET.ElementTree(rss).write(filename, encoding="utf-8", xml_declaration=True)


# =========================
# 4CHAN FETCH
# =========================

def fetch_catalog():
    return fetch_json(f"https://a.4cdn.org/{BOARD}/catalog.json")

def fetch_thread_posts(thread_no: int):
    data = fetch_json(f"https://a.4cdn.org/{BOARD}/thread/{thread_no}.json")
    return data.get("posts", []) if data else None

def thread_velocity(thread: dict, n: int) -> float:
    replies = thread.get("replies", 0)
    last = thread.get("last_modified", thread.get("time", n))
    hours = max((n - last) / 3600.0, 0.25)
    return replies / hours


# =========================
# ACTIVE FEED (UPGRADED)
# - No "general" titles
# - Ticker threads first
# - Exploding detector (velocity accelerating vs last run)
# - Context window: OP + first 3 replies + last 30 replies
# - Replies are oldest -> newest (OP always on top)
# =========================

def thread_has_general(thread: dict) -> bool:
    sub = (thread.get("sub") or "")
    return "general" in sub.lower()

def thread_has_ticker(thread: dict) -> bool:
    # Use subject + catalog snippet for cheap detection
    text = (thread.get("sub") or "") + " " + (thread.get("com") or "")
    tks = [t for t in extract_tickers(text) if plausible_ticker(t)]
    return len(set(tks)) > 0

def build_thread_context_html(thread_no: int, subject: str, replies: int, last_mod: int, posts: list) -> str:
    url = f"https://boards.4chan.org/{BOARD}/thread/{thread_no}"

    op = posts[0] if posts else {}
    op_text = strip_html(op.get("com", ""))

    # early replies: posts[1:1+FIRST_REPLIES]
    early = posts[1:1 + FIRST_REPLIES] if len(posts) > 1 else []

    # latest replies: last LAST_REPLIES excluding OP
    latest = posts[1:] if len(posts) > 1 else []
    latest = latest[-LAST_REPLIES:] if latest else []

    # Avoid duplicates if thread small (early overlaps with latest)
    early_ids = {p.get("no") for p in early}
    latest = [p for p in latest if p.get("no") not in early_ids]

    body = []
    body.append(f"<h2>{html.escape(subject)}</h2>")
    body.append(
        f"<p><a href='{url}'>Open thread</a> • Replies: {replies} • "
        f"Updated: {datetime.fromtimestamp(last_mod, tz=timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}</p>"
    )

    body.append("<hr><h3>OP</h3>")
    body.append(f"<p>{html.escape(op_text).replace(chr(10), '<br>')}</p>")

    body.append(f"<hr><h3>Early replies (first {FIRST_REPLIES})</h3>")
    if not early:
        body.append("<p><i>No replies yet.</i></p>")
    else:
        # Already oldest->newest by API order
        for p in early:
            txt = strip_html(p.get("com", ""))
            if not txt:
                continue
            body.append(f"<p><b>{p.get('no')}</b><br>{html.escape(txt).replace(chr(10), '<br>')}</p><hr>")

    body.append(f"<hr><h3>Latest replies (last {LAST_REPLIES}, oldest → newest)</h3>")
    if not latest:
        body.append("<p><i>No additional replies in the latest window.</i></p>")
    else:
        # Already oldest->newest by API order for the slice
        for p in latest:
            txt = strip_html(p.get("com", ""))
            if not txt:
                continue
            body.append(f"<p><b>{p.get('no')}</b><br>{html.escape(txt).replace(chr(10), '<br>')}</p><hr>")

    return "".join(body)

def generate_active_feed():
    catalog = fetch_catalog()
    if not catalog:
        return

    n = now_ts()
    threads = [t for page in catalog for t in page.get("threads", [])]

    # Filter out GENERAL threads (display)
    threads = [t for t in threads if not thread_has_general(t)]

    # Compute velocity now
    vel_now = {}
    for t in threads:
        vel_now[str(t["no"])] = thread_velocity(t, n)

    # Load previous velocities
    vel_prev = load_json(THREAD_VEL_FILE, {})

    enriched = []
    for t in threads:
        no = str(t["no"])
        v = vel_now.get(no, 0.0)
        vp = float(vel_prev.get(no, 0.0) or 0.0)

        # exploding?
        explode = False
        if (v - vp) >= EXPLODE_ABS_DELTA:
            explode = True
        if vp > 0 and v >= vp * EXPLODE_MULT:
            explode = True

        has_tk = thread_has_ticker(t)

        # Sort key: ticker first, then exploding, then velocity
        # Use booleans as ints (True=1)
        enriched.append((int(has_tk), int(explode), v, t, vp))

    enriched.sort(key=lambda x: (x[0], x[1], x[2]), reverse=True)

    items = []
    for has_tk_i, explode_i, v, t, vp in enriched[:ACTIVE_THREADS_LIMIT]:
        thread_no = t["no"]
        url = f"https://boards.4chan.org/{BOARD}/thread/{thread_no}"
        subject = strip_html(t.get("sub")) or f"Thread {thread_no}"
        replies = t.get("replies", 0)
        last_mod = t.get("last_modified", t.get("time", n))

        posts = fetch_thread_posts(thread_no)
        if not posts:
            continue

        body = build_thread_context_html(thread_no, subject, replies, last_mod, posts)

        tags = []
        if has_tk_i:
            tags.append("TICKER")
        if explode_i:
            tags.append(f"EXPLODING {v:.1f}/hr")
        else:
            tags.append(f"{v:.1f}/hr")

        title = f"[{' | '.join(tags)}] {subject} — {replies} replies"
        guid = f"{url}?lm={last_mod}"

        items.append({
            "title": title,
            "link": url,
            "guid": guid,
            "pubDate": rfc822(last_mod),
            "description": "Open",
            "content_html": body
        })

    write_rss(
        title="/biz/ Active (no generals, context, ticker-first, exploding)",
        link=f"https://boards.4chan.org/{BOARD}/",
        desc=f"OP + first {FIRST_REPLIES} replies + last {LAST_REPLIES} replies. Ticker threads boosted. Exploding threads flagged.",
        items=items,
        filename=FEED_ACTIVE
    )

    # Save velocities for next run (use full set we computed post-filter)
    save_json(THREAD_VEL_FILE, vel_now)


# =========================
# STOCK VALIDATION (FMP + Yahoo options)
# =========================

def fmp_profile(tk: str):
    if not FMP_API_KEY:
        return None
    return fetch_json(f"https://financialmodelingprep.com/api/v3/profile/{tk}?apikey={FMP_API_KEY}", timeout=18)

def yahoo_optionable(tk: str) -> bool:
    data = fetch_json(f"https://query2.finance.yahoo.com/v7/finance/options/{tk}", timeout=18)
    res = (data or {}).get("optionChain", {}).get("result")
    return bool(res and res[0].get("expirationDates"))

def validate_stock(tk: str):
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

    if not yahoo_optionable(tk):
        return None

    return {
        "ticker": tk,
        "name": p.get("companyName") or tk,
        "cap": cap,
        "desc": (p.get("description") or "")[:240]
    }


# =========================
# MENTIONS (ALL THREADS INCLUDED)
# (You only asked to exclude "general" from display in Active feed.
# Mentions remain based on the full catalog.)
# =========================

def gather_mentions():
    catalog = fetch_catalog()
    counts = {}
    if not catalog:
        return counts

    for page in catalog:
        for t in page.get("threads", []):
            text = (t.get("sub","") or "") + " " + (t.get("com","") or "")
            for tk in extract_tickers(text):
                if plausible_ticker(tk):
                    counts[tk] = counts.get(tk, 0) + 1
    return counts


# =========================
# OPPORTUNITY FEEDS (ASYM / PRE / TOP)
# Universe: 50M–2.5B, optionable, US exchange
# =========================

def load_mentions_history():
    return load_json(MENTION_HISTORY_FILE, {})

def save_mentions_history(data):
    save_json(MENTION_HISTORY_FILE, data)

def generate_opportunity_feeds():
    curr = gather_mentions()
    prev = load_mentions_history()

    ranked = sorted(curr.items(), key=lambda kv: kv[1], reverse=True)[:MAX_TICKERS_TO_VALIDATE]

    validated = {}
    for tk, _ in ranked:
        info = validate_stock(tk)
        if info:
            validated[tk] = info

    n = now_ts()

    asym_rows = []
    pre_rows = []

    for tk, info in validated.items():
        m = curr.get(tk, 0)
        p = prev.get(tk, 0)
        cap = info["cap"]

        # Asymmetry: attention vs size
        asym_score = m * (MAX_CAP / cap)
        asym_rows.append((asym_score, tk, info, m))

        # Pre-breakout: rising mentions but not crowded
        if 2 <= m <= 15:
            delta = m - p
            if delta > 0:
                pre_score = delta * (MAX_CAP / cap) / max(1.0, math.log(m + 1))
                pre_rows.append((pre_score, tk, info, m, delta))

    asym_rows.sort(reverse=True)
    pre_rows.sort(reverse=True)

    # ---- Asymmetric feed
    asym_items = []
    for sc, tk, info, m in asym_rows[:25]:
        cap_s = fmt_money(info["cap"])
        why = f"ASYM • cap {cap_s} • mentions {m} • score {sc:.2f}"
        body = (
            f"<h2>${tk} — {html.escape(info['name'])}</h2>"
            f"<p><b>Why it’s asymmetric:</b> {html.escape(why)}</p>"
            f"<p>{html.escape(info['desc'])}</p>"
            f"<p><a href='https://finance.yahoo.com/quote/{tk}'>Open</a></p>"
        )
        asym_items.append({
            "title": f"{tk} — WHY: {why}",
            "link": f"https://finance.yahoo.com/quote/{tk}",
            "guid": f"asym-{tk}-{n}",
            "pubDate": rfc822(n),
            "description": "Open",
            "content_html": body
        })

    write_rss(
        title="Asymmetric Plays (50M–2.5B)",
        link=f"https://boards.4chan.org/{BOARD}/",
        desc="Attention vs size. US exchange, optionable.",
        items=asym_items,
        filename=FEED_ASYM
    )

    # ---- Pre-breakout feed
    pre_items = []
    for sc, tk, info, m, d in pre_rows[:20]:
        cap_s = fmt_money(info["cap"])
        why = f"PRE • mentions {m} (+{d}) • cap {cap_s} • score {sc:.2f}"
        body = (
            f"<h2>${tk} — {html.escape(info['name'])}</h2>"
            f"<p><b>Pre-breakout signal:</b> {html.escape(why)}</p>"
            f"<p>{html.escape(info['desc'])}</p>"
            f"<p><a href='https://finance.yahoo.com/quote/{tk}'>Open</a></p>"
        )
        pre_items.append({
            "title": f"{tk} — PRE-BREAKOUT — {why}",
            "link": f"https://finance.yahoo.com/quote/{tk}",
            "guid": f"pre-{tk}-{n}",
            "pubDate": rfc822(n),
            "description": "Open",
            "content_html": body
        })

    write_rss(
        title="Pre-Breakout Detector (50M–2.5B)",
        link=f"https://boards.4chan.org/{BOARD}/",
        desc="Rising mentions, still early. US exchange, optionable.",
        items=pre_items,
        filename=FEED_PRE
    )

    # ---- Top opportunities (merge + dedupe; ASYM first then PRE)
    seen = set()
    merged = []

    for sc, tk, info, m in asym_rows[:15]:
        if tk in seen:
            continue
        seen.add(tk)
        merged.append(("ASYM", tk, info, m, 0))

    for sc, tk, info, m, d in pre_rows[:15]:
        if tk in seen:
            continue
        seen.add(tk)
        merged.append(("PRE", tk, info, m, d))

    top_items = []
    for tag, tk, info, m, d in merged[:25]:
        cap_s = fmt_money(info["cap"])
        why = f"{tag} • cap {cap_s} • mentions {m}" + (f" (+{d})" if d else "")
        body = (
            f"<h2>${tk} — {html.escape(info['name'])}</h2>"
            f"<p><b>Why it’s top:</b> {html.escape(why)}</p>"
            f"<p>{html.escape(info['desc'])}</p>"
            f"<p><a href='https://finance.yahoo.com/quote/{tk}'>Open</a></p>"
        )
        top_items.append({
            "title": f"{tk} — WHY: {why}",
            "link": f"https://finance.yahoo.com/quote/{tk}",
            "guid": f"top-{tk}-{n}",
            "pubDate": rfc822(n),
            "description": "Open",
            "content_html": body
        })

    write_rss(
        title="Top Opportunities Now (50M–2.5B)",
        link=f"https://boards.4chan.org/{BOARD}/",
        desc="Combined ASYM + PRE signals. US exchange, optionable.",
        items=top_items,
        filename=FEED_TOP
    )

    # Save mention snapshot
    save_mentions_history(curr)


# =========================
# MAIN
# =========================

def main():
    generate_active_feed()
    generate_opportunity_feeds()

if __name__ == "__main__":
    main()