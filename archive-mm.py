from datetime import datetime
import json
import os
import re
import requests
import sys
import time

from bs4 import BeautifulSoup


INDEX_DELAY=60.0
ENTRY_DELAY=15.0
PAGE_DELAY=1.0

BASE_URL = "https://ecosophia.dreamwidth.org"
HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; personal-archiver-thank-you-JMG/1.0)"}

MONTHS = {"Jan": "01", "Feb": "02", "Mar": "03", "Apr": "04",
          "May": "05", "Jun": "06", "Jul": "07", "Aug": "08",
          "Sep": "09", "Oct": "10", "Nov": "11", "Dec": "12"}


class Tee:
    """Writes all print() output to both the console and a log file."""
    def __init__(self, filepath):
        self.console = sys.__stdout__ or open(os.devnull, "w")
        self.file = open(filepath, "a", encoding="utf-8")

    def write(self, data):
        self.console.write(data)
        self.file.write(data)
        self.file.flush()

    def flush(self):
        self.console.flush()
        self.file.flush()

log_filename = datetime.now().strftime("./log_%Y-%m-%d_%H-%M.txt")
sys.stdout = Tee(log_filename)


def fetch(url):
    r = requests.get(url, headers=HEADERS)
    r.raise_for_status()
    return BeautifulSoup(r.text, "html.parser")


def get_entry_links(skip):
    url = f"{BASE_URL}/?tag=magic+monday&skip={skip}"
    print(f"Fetching index page: {url}")
    soup = fetch(url)

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
                raw_time = time_tag.get_text(strip=True)          # "02:16 pm"
                t_match = re.match(r"(\d+):(\d+)\s*(am|pm)", raw_time, re.I)
                if t_match:
                    time_str = f"{t_match.group(1)}-{t_match.group(2)}{t_match.group(3).upper()}"

        entries.append({
            "title": title,
            "url": entry_url,
            "year": year, "month": month, "day": day, "time": time_str,
        })
    return entries


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

    # All page links have class "comment-page"; find the highest page number
    max_page = 1
    for a in pages_div.find_all("a", class_="comment-page"):
        m = re.search(r"page=(\d+)", a["href"])
        if m:
            max_page = max(max_page, int(m.group(1)))

    return max_page


def parse_comments_from_soup(soup):
    """
    Extract a flat dict of comments from a single page's soup.
    Returns {comment_id: {id, title, user, content, parent_id, replies: []}}
    """
    flat = {}
    for section in soup.find_all(id=lambda x: x and x.startswith("cmt")):
        comment_id = section["id"]

        title_tag = section.find(class_="comment-title")
        title = title_tag.get_text(strip=True) if title_tag else ""

        poster = section.find(class_="comment-poster")
        if poster and poster.find(class_="anonymous"):
            user = "(Anonymous)"
        elif poster:
            lj_span = poster.find("span", attrs={"lj:user": True})
            user = lj_span["lj:user"] if lj_span else "unknown"
        else:
            user = "unknown"

        content_tag = section.find(class_="comment-content")
        content = content_tag.get_text("\n", strip=True) if content_tag else ""

        if not content:
            continue

        parent_id = None
        parent_li = section.find("li", class_="commentparent")
        if parent_li:
            parent_a = parent_li.find("a", href=True)
            if parent_a:
                parent_id = extract_cmt_id(parent_a["href"])

        flat[comment_id] = {
            "id": comment_id,
            "title": title,
            "user": user,
            "content": content,
            "parent_id": parent_id,
            "replies": [],
        }
    return flat


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
    return roots


def make_filename(entry):
    """Build filename from structured date fields, e.g. '2017-12-25_02-16PM_Magic_Monday.json'"""
    date_str = f"{entry['year']}-{entry['month']}-{entry['day']}"
    time_str = f"_{entry['time']}" if entry['time'] else ""
    safe_title = re.sub(r"[^\w\s-]", "", entry['title']).strip().replace(" ", "_")
    return f"{date_str}{time_str}_{safe_title}.json"


def scrape_entry(entry_url):
    """
    Fetch all pages of an entry (using expand_all=1&page=n),
    merge comments across pages, then build a nested tree.
    """
    # Fetch page 1 first to find out the total page count
    page1_url = entry_url.rstrip("/") + "?expand_all=1&page=1#comments"
    print(f"  Fetching page 1: {page1_url}")
    soup1 = fetch(page1_url)
    total_pages = get_page_count(soup1)
    print(f"  Total pages: {total_pages}")

    flat = parse_comments_from_soup(soup1)

    for page_num in range(2, total_pages + 1):
        page_url = entry_url.rstrip("/") + f"?expand_all=1&page={page_num}#comments"
        print(f"  Fetching page {page_num}: {page_url}")
        soup = fetch(page_url)
        flat.update(parse_comments_from_soup(soup))
        time.sleep(PAGE_DELAY)

    return build_tree(flat), total_pages


def main():
    out_dir = "./output"
    num_entries_scraped = 0
    num_pages_scraped = 0
    num_comments_scraped = 0

    for skip in range(400, -1, -20):
        print(f"\n=== Index page skip={skip} ===")
        entries = get_entry_links(skip)
        print(f"Found {len(entries)} entries")

        for entry in reversed(entries):
            print(f"\n[{entry['year']}-{entry['month']}-{entry['day']} {entry['time'].replace('-', ':')}] {entry['title']} -> {entry['url']}")

            comments, num_pages = scrape_entry(entry["url"])
            def count_comments(nodes):
                return sum(1 + count_comments(c["replies"]) for c in nodes)
            num_comments = count_comments(comments)
            print(f"  {len(comments)} top-level comments, {num_comments} total")

            filename = make_filename(entry)
            year_dir = os.path.join(out_dir, entry["year"] or "unknown")
            os.makedirs(year_dir, exist_ok=True)
            out_path = os.path.join(year_dir, filename)
            with open(out_path, "w", encoding="utf-8") as f:
                json.dump({**entry, "comments": comments}, f, indent=4, ensure_ascii=False)
            print(f"  Saved -> {out_path}")

            num_entries_scraped += 1
            num_pages_scraped += num_pages
            num_comments_scraped += num_comments
            time.sleep(ENTRY_DELAY)

        time.sleep(INDEX_DELAY)

    print(f"\nDone! Scraped {num_entries_scraped} entries, {num_pages_scraped} pages, {num_comments_scraped} comments")


if __name__ == "__main__":
    main()

