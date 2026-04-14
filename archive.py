import argparse
from datetime import datetime
import hashlib
import json
import logging
import os
import re
# import requests
import sys
import time
from collections import Counter

from bs4 import BeautifulSoup
from curl_cffi import requests as cffi_requests


INDEX_DELAY=600
ENTRY_DELAY=60
PAGE_DELAY=30
IMAGE_DELAY=10

# HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; personal-archiver-thank-you-JMG/1.0)"}
BASE_URL = "https://ecosophia.dreamwidth.org"
IMAGE_IGNORE = {
    "https://www.dreamwidth.org/img/silk/identity/user.png",
}

CACHE_DIR = "./data/cache"
SKIP_MISSING_IMAGES = False  # set by --skip-missing-images flag

MONTHS = {"Jan": "01", "Feb": "02", "Mar": "03", "Apr": "04",
          "May": "05", "Jun": "06", "Jul": "07", "Aug": "08",
          "Sep": "09", "Oct": "10", "Nov": "11", "Dec": "12"}


log_filename = datetime.now().strftime("./log_%Y-%m-%d_%H-%M.txt")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(log_filename, encoding="utf-8"),
    ],
)
log = logging.getLogger(__name__)


def url_to_cache_path(url):
    """Convert a URL to a cache file path using a hash of the URL."""
    key = hashlib.md5(url.encode()).hexdigest()
    return os.path.join(CACHE_DIR, f"{key}.html")


def fetch(url, retries=10):
    """Returns (soup, from_cache)."""
    cache_path = url_to_cache_path(url)
    if os.path.exists(cache_path):
        log.info(f"  [cache] {url}")
        with open(cache_path, encoding="utf-8") as f:
            return BeautifulSoup(f.read(), "html.parser"), True

    os.makedirs(CACHE_DIR, exist_ok=True)
    for attempt in range(retries):
        try:
            # r = requests.get(url, headers=HEADERS, timeout=(30, 120))
            r = cffi_requests.get(url, impersonate="chrome120", timeout=(30, 120))
            r.raise_for_status()
            with open(cache_path, "w", encoding="utf-8") as f:
                f.write(r.text)
            return BeautifulSoup(r.text, "html.parser"), False
        except Exception as e:
            wait = 30 * (2 ** attempt)  # 30s, 60s, 120s, 240s, 480s
            log.info(f"  [retry {attempt+1}/{retries}] {e} — waiting {wait}s")
            time.sleep(wait)
    raise RuntimeError(f"Failed to fetch {url} after {retries} retries")


def get_entry_links(skip):
    url = f"{BASE_URL}/?tag=magic+monday&skip={skip}"
    log.info(f"Fetching index page: {url}")
    soup, _ = fetch(url)

    entries = []
    for h3 in soup.find_all("h3", class_="entry-title"):
        a = h3.find("a")
        if not (a and a.get("href")):
            continue

        title = a.get_text(strip=True)
        entry_url = a["href"]

        # The <span class="datetime"> is a sibling of the <h3>
        parent = h3.parent
        dt_span = parent.find("span", class_="datetime") if parent else None

        year = month = day = time_str = ""
        if dt_span:
            date_span = dt_span.find("span", class_="date")
            if date_span:
                links = date_span.find_all("a")
                # links[0] = month name, links[1] = day+suffix, links[2] = year
                if len(links) >= 3:
                    month = MONTHS.get(links[0].get_text(strip=True), "00")
                    day   = re.sub(r"\D", "", links[1].get_text(strip=True)).zfill(2)
                    year  = links[2].get_text(strip=True)

            time_tag = dt_span.find("span", class_="time")
            if time_tag:
                # "02:16 pm" -> "02-16PM"
                raw_time = time_tag.get_text(strip=True)
                t_match = re.match(r"(\d+):(\d+)\s*(am|pm)", raw_time, re.I)
                if t_match:
                    time_str = f"{t_match.group(1)}-{t_match.group(2)}{t_match.group(3).upper()}"

        entries.append({
            "title": title,
            "url": entry_url,
            "year": year, "month": month, "day": day, "time": time_str,
        })

    # Check if there are more pages by looking for the page-back link
    has_next = bool(soup.find("li", class_="page-back"))

    return entries, has_next


def extract_cmt_id(url):
    """Pull the comment id from a URL fragment, e.g. #cmt96530 -> 'cmt96530'."""
    match = re.search(r"#(cmt\d+)", url)
    return match.group(1) if match else None


def get_page_count(soup):
    """
    Look for <div class='comment-pages toppages'> and find the highest page number.
    Returns 1 if there's only one page.
    """
    pages_div = soup.find("div", class_="comment-pages")
    if not pages_div:
        return 1

    max_page = 1
    for a in pages_div.find_all("a", class_="comment-page"):
        m = re.search(r"page=(\d+)", a["href"])
        if m:
            max_page = max(max_page, int(m.group(1)))

    return max_page


def download_image(src, images_dir, comment_date, page_cached):
    """
    Download an image from src into images_dir.
    Prepends comment_date to the filename to ensure uniqueness.
    Returns (result_dict, status) where status is 'saved', 'cached', 'skipped', or 'failed'.
    Skips sleep if the image was already downloaded.
    Only skips missing images (when SKIP_MISSING_IMAGES is set) if the page was cached —
    live pages always download images normally.
    """
    try:
        base = re.sub(r"[^\w.\-]", "_", src.split("/")[-1].split("?")[0])
        if not base:
            return {"url": src, "local_path": None}, "failed"
        safe_date = re.sub(r"[^\w\-]", "_", comment_date) if comment_date else "unknown"
        filename = f"{safe_date}_{base}"
        out_path = os.path.join(images_dir, filename)
        os.makedirs(images_dir, exist_ok=True)
        if os.path.exists(out_path):
            # Already downloaded — skip network call and sleep
            log.info(f"    [image] Cached {filename}")
            return {"url": src, "local_path": out_path}, "cached"
        if SKIP_MISSING_IMAGES and page_cached:
            log.info(f"    [image] Skipped {filename}")
            return {"url": src, "local_path": None}, "skipped"
        # r = requests.get(src, headers=HEADERS, timeout=10)
        r = cffi_requests.get(src, impersonate="chrome120", timeout=(30, 120))
        r.raise_for_status()
        with open(out_path, "wb") as f:
            f.write(r.content)
        log.info(f"    [image] Saved {filename}")
        time.sleep(IMAGE_DELAY)
        return {"url": src, "local_path": out_path}, "saved"
    except Exception as e:
        log.warning(f"    [image] Failed to download {src}: {e}")
        return {"url": src, "local_path": None}, "failed"


def process_images(content_tag, images_dir, date, page_cached):
    """
    Download all images in content_tag, replace with [IMAGE:N] placeholders.
    Returns (images list, Counter of statuses).
    """
    images = []
    img_counts = Counter()
    for img in content_tag.find_all("img"):
        src = img.get("src", "")
        if not src or src in IMAGE_IGNORE:
            img.decompose()
            continue
        result, status = download_image(src, images_dir, date, page_cached)
        images.append(result)
        img_counts[status] += 1
        img.replace_with(f'[IMAGE:{len(images) - 1}]')
    return images, img_counts


def parse_comments_from_soup(soup, images_dir, page_cached):
    """
    Extract a flat dict of comments from a single page's soup.
    Images are embedded inline in content as [IMAGE:N] placeholders,
    preserving their original position in the text.
    Returns (flat dict, Counter of image statuses).
    """
    flat = {}
    img_counts = Counter()
    for section in soup.find_all(id=lambda x: x and x.startswith("cmt")):
        comment_id = section["id"]

        title_tag = section.find(class_="comment-title")
        title = title_tag.get_text(strip=True) if title_tag else ""

        date = ""
        datetime_span = section.find(class_="datetime")
        if datetime_span:
            inner = datetime_span.find("span", title=True)
            if inner:
                # e.g. "2018-05-14 04:38 am (UTC)" -> "2018-05-14 04:38 am"
                date = inner.get_text(strip=True).replace("(UTC)", "").strip()

        poster = section.find(class_="comment-poster")
        if poster and poster.find(class_="anonymous"):
            user = "(Anonymous)"
        elif poster:
            lj_span = poster.find("span", attrs={"lj:user": True})
            user = lj_span["lj:user"] if lj_span else "unknown"
        else:
            user = "unknown"

        content_tag = section.find(class_="comment-content")
        images = []
        edit_time = ""
        if content_tag:
            # Remove edittime div before extracting text so it doesn't appear in content
            edittime_div = content_tag.find(class_="edittime")
            if edittime_div:
                inner = edittime_div.find("span", title=True)
                if inner:
                    edit_time = inner.get_text(strip=True)
                edittime_div.decompose()

            # Download images and replace each <img> tag in-place with an
            # [IMAGE:N] index placeholder so position is preserved in content
            images, counts = process_images(content_tag, images_dir, date, page_cached)
            img_counts += counts

            content = content_tag.decode_contents()
        else:
            content = ""

        if not content:
            continue

        parent_id = None
        parent_li = section.find("li", class_="commentparent")
        if parent_li:
            parent_a = parent_li.find("a", href=True)
            if parent_a:
                parent_id = extract_cmt_id(parent_a["href"])

        flat[comment_id] = {
            "parent_id": parent_id,
            "id": comment_id,
            "title": title,
            "date": date,
            "user": user,
            "content": content,
            **({"images": images} if images else {}),
            **({"edit_time": edit_time} if edit_time else {}),
            "replies": [],
        }
    return flat, img_counts


def build_tree(flat):
    """
    Given a flat dict of comments, attach each child to its parent's replies list.
    Returns the list of root (top-level) comments.
    """
    roots = []
    for comment in flat.values():
        parent_id = comment.pop("parent_id")
        if parent_id and parent_id in flat:
            flat[parent_id]["replies"].append(comment)
        else:
            roots.append(comment)

    # Remove empty replies lists to keep JSON clean
    def prune(nodes):
        for node in nodes:
            if node["replies"]:
                prune(node["replies"])
            else:
                del node["replies"]
    prune(roots)

    return roots


def make_filename(entry):
    """Build filename from structured date fields, e.g. '2017-12-25_02-16PM_Magic_Monday.json'"""
    date_str = f"{entry['year']}-{entry['month']}-{entry['day']}"
    time_str = f"_{entry['time']}" if entry['time'] else ""
    safe_title = re.sub(r"[^\w\s-]", "", entry['title']).strip().replace(" ", "_")
    return f"{date_str}{time_str}_{safe_title}.json"


def parse_entry_post(soup, images_dir, page_cached):
    """
    Extract the opening post (class="entry") as a comment-like dict,
    to be prepended as the first item in the comment list.
    Returns (post dict, Counter of image statuses).
    """
    entry_div = soup.find(class_="entry")
    if not entry_div:
        return None, Counter()

    content_tag = entry_div.find(class_="entry-content")
    if not content_tag:
        return None, Counter()

    # Remove edittime if present
    edittime_div = content_tag.find(class_="edittime")
    if edittime_div:
        edittime_div.decompose()

    # Download images and replace with placeholders
    images, img_counts = process_images(content_tag, images_dir, "", page_cached)

    content = content_tag.decode_contents()

    # Get poster username
    poster_span = entry_div.find("span", attrs={"lj:user": True})
    user = poster_span["lj:user"] if poster_span else "ecosophia"

    # Get date
    date = ""
    dt_span = entry_div.find("span", class_="datetime")
    if dt_span:
        inner = dt_span.find("span", title=True)
        if inner:
            date = inner.get_text(strip=True).replace("(UTC)", "").strip()
        else:
            date = dt_span.get_text(" ", strip=True)

    # Build metadata dict from mood, music, and tags
    metadata = {}
    for li in entry_div.find_all("li", id=lambda x: x and x.startswith("metadata-")):
        item = li.find(class_="metadata-item")
        if not item:
            continue
        if "mood" in li["id"]:
            metadata["mood"] = item.get_text(strip=True)
        elif "music" in li["id"]:
            metadata["music"] = item.get_text(strip=True)
    tag_div = entry_div.find("div", class_="tag")
    tags = [a.get_text(strip=True) for a in tag_div.find_all("a", rel="tag")] if tag_div else []
    if tags:
        metadata["tags"] = tags

    post = {
        "id": entry_div.get("id", "entry-post"),
        "title": "",
        "date": date,
        "user": user,
        "content": content,
        **({"images": images} if images else {}),
        **({"metadata": metadata} if metadata else {}),
    }
    return post, img_counts


def scrape_entry(entry_url, images_dir):
    """
    Fetch all pages of an entry (using expand_all=1&page=n),
    merge comments across pages, then build a nested tree.
    Returns (comments, total_pages, pages_cached, all_cached, img_counts).
    """
    page1_url = entry_url.rstrip("/") + "?expand_all=1&page=1#comments"
    log.info(f"  Fetching page 1: {page1_url}")
    soup1, from_cache = fetch(page1_url)
    total_pages = get_page_count(soup1)
    log.info(f"  Total pages: {total_pages}")

    all_cached   = from_cache
    pages_cached = 1 if from_cache else 0
    entry_post, img_counts = parse_entry_post(soup1, images_dir, from_cache)
    flat, counts = parse_comments_from_soup(soup1, images_dir, from_cache)
    img_counts += counts

    for page_num in range(2, total_pages + 1):
        page_url = entry_url.rstrip("/") + f"?expand_all=1&page={page_num}#comments"
        log.info(f"  Fetching page {page_num}: {page_url}")
        soup, from_cache = fetch(page_url)
        page_flat, counts = parse_comments_from_soup(soup, images_dir, from_cache)
        flat.update(page_flat)
        img_counts += counts
        if from_cache:
            pages_cached += 1
        else:
            all_cached = False
            time.sleep(PAGE_DELAY)

    comments = build_tree(flat)
    if entry_post:
        comments = [entry_post] + comments
    return comments, total_pages, pages_cached, all_cached, img_counts


def main():
    out_dir = "./data/archive"
    os.makedirs(out_dir, exist_ok=True)

    num_entries_scraped  = 0
    num_entries_cached   = 0
    num_pages_scraped    = 0
    num_pages_cached     = 0
    num_comments_scraped = 0
    img_counts           = Counter()

    skip = 0
    while True:
        log.info(f"\n=== Index page skip={skip} ===")
        entries, has_next = get_entry_links(skip)
        log.info(f"Found {len(entries)} entries")
        any_entry_live = False

        for entry in entries:
            log.info(f"\n[{entry['year']}-{entry['month']}-{entry['day']} {entry['time'].replace('-', ':')}] {entry['title']} -> {entry['url']}")

            year_dir   = os.path.join(out_dir, entry["year"] or "unknown")
            os.makedirs(year_dir, exist_ok=True)
            filename   = make_filename(entry)
            entry_dir  = os.path.join(year_dir, filename.removesuffix(".json"))
            images_dir = os.path.join(entry_dir, "images")
            os.makedirs(entry_dir, exist_ok=True)

            comments, num_pages, pages_cached, entry_cached, counts = scrape_entry(entry["url"], images_dir)
            def count_comments(nodes):
                return sum(1 + count_comments(c.get("replies", [])) for c in nodes)
            num_comments = count_comments(comments)
            log.info(f"  {len(comments)} top-level comments, {num_comments} total")

            out_path = os.path.join(entry_dir, "entry.json")
            with open(out_path, "w", encoding="utf-8") as f:
                json.dump({**entry, "comments": comments}, f, indent=4, ensure_ascii=False)
            log.info(f"  Saved -> {out_path}")

            num_entries_scraped  += 1
            num_pages_scraped    += num_pages
            num_pages_cached     += pages_cached
            num_comments_scraped += num_comments
            img_counts           += counts
            if entry_cached:
                num_entries_cached += 1
            else:
                any_entry_live = True
                time.sleep(ENTRY_DELAY)

        if not has_next:
            log.info("\nNo more pages (page-back not found). Done!")
            break

        skip += 20
        if any_entry_live:
            time.sleep(INDEX_DELAY)

    num_entries_live = num_entries_scraped - num_entries_cached
    num_pages_live   = num_pages_scraped   - num_pages_cached
    skipped_str = f", {img_counts['skipped']:,} skipped" if SKIP_MISSING_IMAGES else ""
    log.info(f"""
Done!
  Entries  : {num_entries_scraped:>6,}  ({num_entries_live:,} live, {num_entries_cached:,} cached)
  Pages    : {num_pages_scraped:>6,}  ({num_pages_live:,} live, {num_pages_cached:,} cached)
  Comments : {num_comments_scraped:>6,}
  Images   : {img_counts['saved']:,} saved, {img_counts['cached']:,} cached{skipped_str}, {img_counts['failed']:,} failed
""")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--skip-missing-images", action="store_true",
                        help="Skip downloading images that are not already cached locally")
    args = parser.parse_args()
    if args.skip_missing_images:
        SKIP_MISSING_IMAGES = True
    main()

