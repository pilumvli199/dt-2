#!/usr/bin/env python3
# find_scrips.py
import requests, csv, io, sys
from collections import defaultdict

SCRIP_MASTER_URL = "https://images.dhan.co/api-data/api-scrip-master.csv"

# change this list to whatever you want to search for
QUERIES = ["NIFTY 50","BANKNIFTY","SENSEX","TATAMOTORS","RELIANCE","TCS"]

def download_csv(url):
    print("Downloading scrip master...")
    r = requests.get(url, timeout=30)
    r.raise_for_status()
    text = r.content.decode("utf-8", errors="replace")
    f = io.StringIO(text)
    reader = csv.DictReader(f)
    rows = list(reader)
    print(f"Downloaded {len(rows)} rows")
    return rows, reader.fieldnames

def normalize(s):
    return (s or "").strip().upper()

def search(rows, queries, fieldnames):
    # try to find likely column names for tradingsymbol, name, id, segment
    candidates_trad = [c for c in fieldnames if c.lower() in ("tradingsymbol","trading_symbol","symbol","trade_symbol","trade_symbol")]
    candidates_name = [c for c in fieldnames if c.lower() in ("name","instrumentname","securityname","security_name","securityname")]
    candidates_id = [c for c in fieldnames if c.lower() in ("securityid","security_id","securityid","id")]
    candidates_seg = [c for c in fieldnames if c.lower() in ("exchangesegment","segment","exchange_segment","exchange")]

    trad_col = candidates_trad[0] if candidates_trad else fieldnames[0]
    name_col = candidates_name[0] if candidates_name else fieldnames[1] if len(fieldnames)>1 else fieldnames[0]
    id_col = candidates_id[0] if candidates_id else fieldnames[2] if len(fieldnames)>2 else fieldnames[0]
    seg_col = candidates_seg[0] if candidates_seg else (fieldnames[3] if len(fieldnames)>3 else "")

    print("Using columns:", trad_col, name_col, id_col, seg_col)

    # prepare searchable list
    indexed = []
    for r in rows:
        t = normalize(r.get(trad_col, ""))
        n = normalize(r.get(name_col, ""))
        sid = r.get(id_col, "")
        seg = r.get(seg_col, "") if seg_col else ""
        indexed.append({"trad": t, "name": n, "sid": sid, "seg": seg, "row": r})

    results = {}
    for q in queries:
        qn = normalize(q)
        matches = []
        for item in indexed:
            score = 0
            # exact trading symbol match highest
            if item["trad"] == qn:
                score += 100
            # exact name match
            if item["name"] == qn:
                score += 90
            # prefix match
            if item["trad"].startswith(qn) or item["name"].startswith(qn):
                score += 50
            # substring match
            if qn in item["trad"] or qn in item["name"]:
                score += 20
            if score>0:
                matches.append((score, item))
        matches.sort(key=lambda x: (-x[0], x[1]["trad"], x[1]["name"]))
        results[q] = matches[:15]
    return results

def pretty_print(results):
    for q, matches in results.items():
        print("\n" + "="*60)
        print("Query:", q, "| Matches:", len(matches))
        print("-"*60)
        if not matches:
            print("  >> No matches")
            continue
        for score, it in matches:
            print(f"score={score:3d} | tradingsymbol='{it['trad']}' | name='{it['name'][:60]}' | securityId={it['sid']} | segment={it['seg']}")
    print("\nDone.")

def main():
    rows, fieldnames = download_csv(SCRIP_MASTER_URL)
    results = search(rows, QUERIES, fieldnames)
    pretty_print(results)

if __name__ == "__main__":
    main()
