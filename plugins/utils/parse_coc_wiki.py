import requests
import tempfile
import logging
import re
import os

from bs4 import BeautifulSoup, NavigableString, Tag

WIKI_BASE = "https://clashofclans.fandom.com/wiki/"
WIKI_API = "https://clashofclans.fandom.com/api.php"
CATEGORIES = [
    "Category:Elixir Troops",
    "Category:Dark Elixir Troops",
    "Category:Super Troops",
    "Category:Heroes",
]
BLOCKLIST_WORDS = [
    "Temporary", "Event", "Seasonal", "Obstacle",
    "Party", "Pumpkin", "Royal Ghost",
    "Santa", "Ice Wizard", "Giant Skeleton"
]
LEVEL_RE = re.compile(r"\bLevel\s*\d+(?:\s*[-–&]\s*\d+)?\b", re.IGNORECASE)

def slugify(name: str) -> str:
    return "".join(c.lower() if c.isalnum() else "_" for c in name)

def get_all_troops():
    troops = []
    for category in CATEGORIES:
        params = {
            "action": "query",
            "list": "categorymembers",
            "cmtitle": category,
            "cmlimit": "max",
            "format": "json"
        }
        while True:
            r = requests.get(WIKI_API, params=params)
            data = r.json()
            for m in data["query"]["categorymembers"]:
                title = m["title"]
                if "/" in title or any(w.lower() in title.lower() for w in BLOCKLIST_WORDS):
                    continue
                troops.append(title)
            if "continue" in data:
                params.update(data["continue"])
            else:
                break

    return troops

def get_images_from_troop_page(troop_name: str, scan_limit=10):
    url = WIKI_BASE + troop_name.replace(" ", "_")
    r = requests.get(url, timeout=15)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "html.parser")

    # Find 'SUMMARY' header
    summary_marker = None
    for tag in soup.find_all(["h1", "h2", "h3", "h4"]):
        if "summary" in tag.get_text(strip=True).lower():
            summary_marker = tag
            break

    results = []
    needle = troop_name.replace(" ", "_").lower()

    # Get images before 'SUMMARY'
    for node in soup.descendants:
        if node is summary_marker:
            break

        if not (isinstance(node, Tag) and node.name == "img"):
            continue

        img = node
        src = img.get("data-src") or img.get("src") or ""
        fname = src.lower()
        alt = (img.get("alt") or "").lower()
        if needle not in fname and needle not in alt:
            continue

        title = None
        seen = 0
        for nxt in img.next_elements:
            if nxt is summary_marker or seen >= scan_limit:
                break
            seen += 1

            if isinstance(nxt, NavigableString):
                text = nxt.strip()
            elif isinstance(nxt, Tag) and nxt.name != "img":
                text = nxt.get_text(" ", strip=True)
            else:
                text = ""

            if not text:
                continue

            match = LEVEL_RE.search(text)
            if match:
                title = match.group(0)
                break

        if title is not None:
            results.append({"url": src, "title": title})

    return results

def extract_levels(text: str):
    m = re.match(r"Level\s*(\d+)\s*(?:[-&]\s*(\d+))?$", text)
    if m:
        start = int(m.group(1))
        end = int(m.group(2)) if m.group(2) else start
        return list(range(start, end + 1))
    return []

def scrape_troop_images(**context):
    troops = get_all_troops()
    logging.info(f"found {len(troops)} troops")
    results = []
    for troop in troops:
        logging.info(f"\n=== Processing {troop} ===")
        troop_slug = slugify(troop)
        images = get_images_from_troop_page(troop)
        if not images:
            logging.info("No images found.")
            continue

        for img in images:
            url = img["url"]
            title = img["title"]
            levels = extract_levels(title)
            if not levels:
                continue

            try:
                img_bytes = requests.get(url).content
            except Exception as e:
                logging.info("Failed download:", url, e)
                continue

            # get img name
            filename = os.path.basename(url)
            # save img as tmp file
            tmp = tempfile.NamedTemporaryFile(delete=False)
            tmp.write(img_bytes)
            tmp.close()

            for lvl in levels:
                minio_key = f"{troop_slug}/{lvl}/{filename}"
                results.append({
                    "local_path": tmp.name,
                    "minio_key": minio_key
                })

    return {"data": results}