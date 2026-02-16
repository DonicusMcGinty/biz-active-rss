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

# Stocks universe (as per your latest)
MIN_CAP = 50_000_000
MAX_CAP = 2_500_000_000
VALID_EXCHANGES = {"NASDAQ", "NYSE", "AMEX"}

# Active feed behavior
ACTIVE_THREADS_LIMIT = 15
FIRST_REPLIES = 3
LAST_REPLIES = 30

# “No-title” OP snippet title settings
MAX_OP_TITLE_CHARS = 120  # short enough for Reeder; UI will still truncate
MIN_OP_TITLE_CHARS = 40   # if OP is shorter, fine

# Exclusions for Active feed DISPLAY (subject OR OP snippet)
EXCLUDE_TITLE_KEYWORDS = [
    "general",   # original
    "gme",       # new
    "bbbyq",     # new
]

# Mentions / tickers (for stock feeds)
TICKER_REGEX = r"\b[A-Z]{2,5}\b"
BLACKLIST = {
    "USD","USDT","USDC","CEO","CFO","SEC","FED","FOMC",
    "NYSE","NASDAQ","AMEX","ETF","IPO","AI","DD","IMO",
    "LOL","YOLO","FOMO","HODL","ATH","TLDR"
}

# Opportunity feed compute limits (avoid too many API calls)
MAX_TICKERS_TO_VALIDATE = 80

# Exploding detector sensitivity (used for ordering only; no longer shown in title tags)
EXPLODE_ABS_DELTA = 6.0
EXPLODE_MULT = 1.6

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

def compact_ws(s: str) -> str:
    return re.sub(r"\s+", " ", s or "").strip()

def remove_urls(s: str) -> str:
    return re.sub(r"https?://\S+|www\.\S+", "", s or "", flags=re.IGNORECASE)

def condense_op_for_title(op_text: str) -> str:
    """
    Aggressive, cheap “condense” suitable for RSS title:
    - strip urls
    - compress whitespace
    - drop common filler
    - take first sentence-ish if available
    - hard trim
    """
    s = strip_html(op_text)
    s = remove_urls(s)
    s = compact_ws(s)

    # Drop some common boilerplate/filler that bloats titles
    s = re.sub(r"\b(tl;?dr|tldr|anon here|listen up|serious question|quick question)\b[:\-]?\s*", "", s, flags=re.IGNORECASE)

    # Prefer first sentence if it exists and is meaningful
    parts = re.split(r"(?<=[\.\!\?])\s+", s)
    if parts and len(parts[0]) >= MIN_OP_TITLE_CHARS:
        s = parts[0]

    s = compact_ws(s)

    if len(s) > MAX_OP_TITLE_CHARS:
        s = s[:MAX_OP_TITLE_CHARS].rstrip() + "…"

    return s if s else "Untitled thread"

def contains_excluded_keyword(text: str) -> bool:
    t = (text or "").lower()
    for kw in EXCLUDE_TITLE_KEYWORDS:
        if kw.lower() in t:
            return True
    return False

# =========================
# RSS WRITER (preserve input order)
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

def thread_has_ticker_hint(thread: dict) -> bool:
    # Cheap detection for ordering only (subject + catalog snippet)
    text = (thread.get("sub") or "") + " " + (thread.get("com") or "")
    tks = [t for t in extract_tickers(text) if plausible_ticker(t)]
    return len(set(tks)) > 0

# =========================
# ACTIVE FEED (no generals, no GME/BBBYQ, context window)
# - Titles are clean: "<subject or OP snippet> — <replies> replies — <x.x/hr>"
# - Replies in body: OP, first 3, last 30 (oldest->newest)
# =========================

def build_thread_context_html(thread_no: int, subject: str, replies: int, last_mod: int, posts: list) -> str:
    url = f"https://boards.4chan.org/{BOARD}/thread/{thread_no}"

    op = posts[0] if posts else {}
    op_text = strip_html(op.get("com", ""))

    early = posts[1:1 + FIRST_REPLIES] if len(posts) > 1 else []
    latest = posts[1:] if len(posts) > 1 else []
    latest = latest[-LAST_REPLIES:] if latest else []

    # De-dupe overlap
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
        for p in early:  # already oldest->newest
            txt = strip_html(p.get("com", ""))
            if not txt:
                continue
            body.append(f"<p><b>{p.get('no')}</b><br>{html.escape(txt).replace(chr(10), '<br>')}</p><hr>")

    body.append(f"<hr><h3>Latest replies (last {LAST_REPLIES}, oldest → newest)</h3>")
    if not latest:
        body.append("<p><i>No additional replies in the latest window.</i></p>")
    else:
        for p in latest:  # slice is still oldest->newest
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

    # Compute current velocities
    vel_now = {str(t["no"]): thread_velocity(t, n) for t in threads}

    # Previous velocities (for “exploding” ordering)
    vel_prev = load_json(THREAD_VEL_FILE, {})

    enriched = []
    for t in threads:
        no = str(t["no"])
        v = float(vel_now.get(no, 0.0))
        vp = float(vel_prev.get(no, 0.0) or 0.0)

        explode = False
        if (v - vp) >= EXPLODE_ABS_DELTA:
            explode = True
        if vp > 0 and v >= vp * EXPLODE_MULT:
            explode = True

        has_tk_hint = thread_has_ticker_hint(t)

        enriched.append((int(has_tk_hint), int(explode), v, t))

    # Sort: ticker-hint first, exploding next, then velocity
    enriched.sort(key=lambda x: (x[0], x[1], x[2]), reverse=True)

    items = []
    for has_tk_i, explode_i, v, t in enriched:
        if len(items) >= ACTIVE_THREADS_LIMIT:
            break

        # Need thread JSON to build OP snippet + replies
        thread_no = t["no"]
        posts = fetch_thread_posts(thread_no)
        if not posts:
            continue

        # Determine subject; if missing, use condensed OP snippet
        raw_sub = strip_html(t.get("sub")) or ""
        op_text_raw = (posts[0].get("com") if posts and len(posts) > 0 else "") or ""
        op_snip = condense_op_for_title(op_text_raw)

        # Exclusion check must apply to subject OR OP snippet (for GME/BBBYQ etc.)
        if contains_excluded_keyword(raw_sub) or contains_excluded_keyword(op_snip):
            continue

        subject = raw_sub if raw_sub else op_snip

        replies = t.get("replies", 0)
        last_mod = t.get("last_modified", t.get("time", n))
        url = f"https://boards.4chan.org/{BOARD}/thread/{thread_no}"

        body = build_thread_context_html(thread_no, subject, replies, last_mod, posts)

        # Clean title: subject — replies — x.x/hr
        title = f"{subject} — {replies} replies — {v:.1f}/hr"

        # GUID changes when updated so Reeder refreshes
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
        title="/biz/ Active (filtered, with replies)",
        link=f"https://boards.4chan.org/{BOARD}/",
        desc=f"Filtered out: {', '.join(EXCLUDE_TITLE_KEYWORDS)}. OP + first {FIRST_REPLIES} replies + last {LAST_REPLIES} replies.",
        items=items,
        filename=FEED_ACTIVE
    )

    # Save velocities for next run (full catalog; harmless)
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
# MENTIONS (full catalog; unchanged)
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

def load_mentions_history():
    return load_json(MENTION_HISTORY_FILE, {})

def save_mentions_history(data):
    save_json(MENTION_HISTORY_FILE, data)

# =========================
# OPPORTUNITY FEEDS (ASYM / PRE / TOP)
# =========================

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

        asym_score = m * (MAX_CAP / cap)
        asym_rows.append((asym_score, tk, info, m))

        if 2 <= m <= 15:
            delta = m - p
            if delta > 0:
                pre_score = delta * (MAX_CAP / cap) / max(1.0, math.log(m + 1))
                pre_rows.append((pre_score, tk, info, m, delta))

    asym_rows.sort(reverse=True)
    pre_rows.sort(reverse=True)

    # Asymmetric
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

    # Pre-breakout
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

    # Top opportunities (merge + dedupe; ASYM first then PRE)
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

    save_mentions_history(curr)

# =========================
# MAIN
# =========================

def main():
    generate_active_feed()
    generate_opportunity_feeds()

if __name__ == "__main__":
    main()
