"""
build.py — run this after scraping to bundle JSON entries into per-year data files.
Usage: python build.py
Output: years.js + data_YYYY.js for each year (load alongside index.html)
"""
import json
import os
import glob
from collections import defaultdict


by_year = defaultdict(list)

for json_path in sorted(glob.glob("./output/**/*.json", recursive=True)):
    with open(json_path, encoding="utf-8") as f:
        try:
            entry = json.load(f)
            year = entry.get("year") or "unknown"
            by_year[year].append(entry)
        except json.JSONDecodeError as e:
            print(f"Skipping {json_path}: {e}")

os.makedirs("./data", exist_ok=True)

for year, entries in by_year.items():
    entries.sort(key=lambda e: (e.get("month",""), e.get("day","")))
    with open(f"./data/data_{year}.js", "w", encoding="utf-8") as f:
        f.write(f"DATA['{year}'] = ")
        json.dump(entries, f, ensure_ascii=False)
        f.write(";")
    print(f"  data/data_{year}.js — {len(entries)} {'entries' if len(entries) > 1 else 'entry'}")

years = sorted(by_year.keys())
counts = {y: len(by_year[y]) for y in years}
year_months = {
    y: sorted(set(e.get("month","") for e in by_year[y] if e.get("month","")))
    for y in years
}
with open("./data/years.js", "w", encoding="utf-8") as f:
    f.write("const YEARS = ")
    json.dump(years, f)
    f.write(";\nconst YEAR_COUNTS = ")
    json.dump(counts, f)
    f.write(";\nconst YEAR_MONTHS = ")
    json.dump(year_months, f)
    f.write(";")

def count_comments(nodes):
    return sum(1 + count_comments(c.get("replies", [])) for c in nodes)

total_entries = sum(len(v) for v in by_year.values())
total_comments = sum(count_comments(e.get("comments", [])) for v in by_year.values() for e in v)
print(f"\nBuilt {len(years)} year files, {total_entries:,} entries, {total_comments:,} comments.")
print(f"Years: {', '.join(years)}")

