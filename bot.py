#!/usr/bin/env python3
"""
ltp_once.py
Simple one-shot LTP fetcher using Dhan /marketfeed/ltp.
Reads SECURITY_IDS env var OR SECURITY_IDS directly from command-line.

SECURITY_IDS formats accepted (comma separated):
 - 11536,49081             -> assumes default segment "NSE_EQ"
 - NSE_EQ:11536,NSE_FNO:49081
 - 11536                   -> single id

Environment variables required:
 - DHAN_TOKEN
 - DHAN_CLIENT_ID

Optional (to forward to Telegram):
 - TELEGRAM_BOT_TOKEN
 - TELEGRAM_CHAT_ID

Run:
  pip install requests python-dotenv
  export DHAN_TOKEN=... 
  export DHAN_CLIENT_ID=...
  export SECURITY_IDS="NSE_EQ:11536,NSE_FNO:49081"
  python ltp_once.py
"""
import os, sys, json, time
from urllib.parse import urljoin
import requests
from dotenv import load_dotenv

load_dotenv()

DHAN_TOKEN = os.getenv("DHAN_TOKEN")
DHAN_CLIENT_ID = os.getenv("DHAN_CLIENT_ID")
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
DHAN_API_BASE = os.getenv("DHAN_API_BASE", "https://api.dhan.co/v2")

if not (DHAN_TOKEN and DHAN_CLIENT_ID):
    print("Missing DHAN_TOKEN or DHAN_CLIENT_ID in environment.")
    sys.exit(1)

def parse_security_ids(raw):
    if not raw:
        return []
    parts = [p.strip() for p in raw.split(",") if p.strip()]
    payload = {}
    for p in parts:
        if ":" in p:
            seg, sid = p.split(":", 1)
            seg = seg.strip().upper()
            sid = sid.strip()
        else:
            seg = "NSE_EQ"
            sid = p
        # ensure integer if possible
        try:
            sid_i = int(sid)
        except:
            # keep as string — API probably wants int, but we'll attempt int conversion later
            try:
                sid_i = int(sid.split(".")[0])
            except:
                sid_i = sid
        payload.setdefault(seg, []).append(int(sid_i))
    return payload

def call_ltp(payload):
    url = urljoin(DHAN_API_BASE + "/", "marketfeed/ltp")
    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "access-token": DHAN_TOKEN,
        "client-id": DHAN_CLIENT_ID
    }
    r = requests.post(url, headers=headers, json=payload, timeout=10)
    r.raise_for_status()
    return r.json()

def send_telegram(text):
    if not (TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID):
        return None
    tg_url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    data = {"chat_id": TELEGRAM_CHAT_ID, "text": text, "parse_mode": "HTML"}
    try:
        r = requests.post(tg_url, json=data, timeout=10)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        print("Telegram send failed:", e)
        return None

def main():
    raw = os.getenv("SECURITY_IDS")
    # allow passing as argument too
    if len(sys.argv) > 1:
        raw = sys.argv[1]
    if not raw:
        print("Provide SECURITY_IDS via env or as first arg. Example:")
        print('  export SECURITY_IDS="NSE_EQ:11536,NSE_FNO:49081"')
        print('  python ltp_once.py')
        sys.exit(1)
    payload = parse_security_ids(raw)
    print("Calling LTP for groups:", payload)
    try:
        resp = call_ltp(payload)
    except Exception as e:
        print("LTP call failed:", e)
        sys.exit(1)
    # print nicely
    data = resp.get("data") or resp
    out_parts = []
    if isinstance(data, dict):
        for seg, mapping in data.items():
            if not isinstance(mapping, dict):
                continue
            for secid, info in mapping.items():
                lp = info.get("last_price") or info.get("ltp") or info.get("lastPrice")
                out_parts.append(f"{seg}:{secid} -> {lp}")
    else:
        out_parts.append(str(data)[:1000])
    out_text = "\n".join(out_parts)
    print("=== LTP RESULTS ===")
    print(out_text)
    # send to telegram if creds present
    if TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID:
        send_telegram("<b>LTP Update</b>\n" + out_text)
        print("Sent to Telegram (if credentials valid).")

if __name__ == "__main__":
    main()
