import requests
import xml.etree.ElementTree as ET
from datetime import datetime

CATALOG_URL = "https://a.4cdn.org/biz/catalog.json"
BOARD = "biz"

def build_rss(threads):
    rss = ET.Element("rss", version="2.0")
    channel = ET.SubElement(rss, "channel")

    ET.SubElement(channel, "title").text = "/biz/ Active Threads"
    ET.SubElement(channel, "link").text = "https://boards.4chan.org/biz/"
    ET.SubElement(channel, "description").text = "Top active threads from /biz/"

    for thread in threads:
        item = ET.SubElement(channel, "item")

        title = thread.get("sub") or thread.get("com", "")[:60]
        link = f"https://boards.4chan.org/{BOARD}/thread/{thread['no']}"
        replies = thread.get("replies", 0)

        ET.SubElement(item, "title").text = f"{title} ({replies} replies)"
        ET.SubElement(item, "link").text = link
        ET.SubElement(item, "guid").text = link

        if "tim" in thread and "ext" in thread:
            img_url = f"https://i.4cdn.org/{BOARD}/{thread['tim']}{thread['ext']}"
            description = f'<img src="{img_url}"><br>{thread.get("com","")}'
        else:
            description = thread.get("com", "")

        ET.SubElement(item, "description").text = description

    return ET.tostring(rss, encoding="utf-8")

def main():
    data = requests.get(CATALOG_URL).json()

    threads = []
    for page in data:
        threads.extend(page["threads"])

    # Sort by reply count descending
    threads = sorted(threads, key=lambda x: x.get("replies", 0), reverse=True)

    top_threads = threads[:20]

    rss_content = build_rss(top_threads)

    with open("feed.xml", "wb") as f:
        f.write(rss_content)

if __name__ == "__main__":
    main()
