#!/usr/bin/env python3
"""
DhanHQ polling -> Telegram LTP alerts
- Polls instruments list to match symbols
- Polls quote endpoint every 60s to get LTPs
- Sends nicely formatted message to Telegram chat

Environment variables required:
- DHAN_TOKEN             (your Dhan API token)
- TELEGRAM_BOT_TOKEN     (BotFather token)
- TELEGRAM_CHAT_ID       (chat id or group id)
Optional:
- DHAN_API_BASE          (default: https://api.dhan.co/v2)
- POLL_INTERVAL          (seconds, default: 60)
- SYMBOLS_TO_TRACK       (comma separated symbols override default)
"""

import os
import time
import json
import requests
from datetime import datetime

# ---------------- CONFIG ----------------
SYMBOLS_TO_TRACK = os.getenv("SYMBOLS_TO_TRACK", "NIFTY,BANKNIFTY,SENSEX,RELIANCE,TCS,TATAMOTORS").split(",")
POLL_INTERVAL = int(os.getenv("POLL_INTERVAL", "60"))

DHAN_TOKEN = os.getenv("DHAN_TOKEN")
DHAN_API_BASE = os.getenv("DHAN_API_BASE", "https://api.dhan.co/v2")

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

INSTRUMENTS_PATH = "/instruments"      # common Dhan path for instruments list
QUOTE_PATH = "/market/quote"           # template quote path (may need edit for your API)

if not DHAN_TOKEN:
    raise SystemExit("Set DHAN_TOKEN environment variable.")
if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
    raise SystemExit("Set TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID environment variables.")

HEADERS = {"Authorization": f"Bearer {DHAN_TOKEN}", "Content-Type": "application/json"}

# ---------------- Helpers ----------------
def fetch_instruments():
    url = DHAN_API_BASE.rstrip("/") + INSTRUMENTS_PATH
    r = requests.get(url, headers=HEADERS, timeout=30)
    r.raise_for_status()
    return r.json()

def match_symbols(instruments, symbols):
    mapping = {s: [] for s in symbols}
    for instr in instruments:
        # Some Dhan instrument objects use different keys; be flexible
        trad = str(instr.get("tradingsymbol", "") or instr.get("instrument_name", "") or "").upper()
        name = str(instr.get("instrument_name", "") or "").upper()
        exch = str(instr.get("exchange", "") or "").upper()
        for s in symbols:
            if s.upper() in trad or s.upper() in name:
                mapping[s].append(instr)
            # also allow exact trading symbol match
            if trad == s.upper():
                mapping[s].append(instr)
    return mapping

def choose_best_match(matches):
    chosen = {}
    for s, list_matches in matches.items():
        if not list_matches:
            chosen[s] = None
            continue
        # prefer NSE/NFO or INDEX
        pick = None
        for m in list_matches:
            exch = (m.get("exchange") or "").upper()
            itype = (m.get("instrument_type") or m.get("segment") or "").upper()
            if "NSE" in exch or "NFO" in exch or "INDEX" in itype:
                pick = m
                break
        if not pick:
            pick = list_matches[0]
        chosen[s] = pick
    return chosen

def save_json(obj, path):
    with open(path, "w") as f:
        json.dump(obj, f, indent=2)

def fetch_quote_for_token(token):
    url = DHAN_API_BASE.rstrip("/") + QUOTE_PATH
    # Try common param names
    for param_name in ("instrumentToken", "instrument_token", "instrumentToken[]", "token"):
        params = {param_name: token}
        r = requests.get(url, headers=HEADERS, params=params, timeout=12)
        if r.status_code == 200:
            try:
                return r.json()
            except Exception:
                return r.text
    # if none worked, raise
    raise RuntimeError(f"Quote fetch failed for token {token} (checked common param names)")

def extract_ltp(response):
    """
    Try to find LTP in response dict (common keys: last_price, ltp, lastPrice, lastTradedPrice)
    If response is list/dict, search recursively.
    """
    keys = ["last_price", "ltp", "lastPrice", "last_traded_price", "lastTradedPrice", "ltpPrice"]
    def deep_find(obj):
        if isinstance(obj, dict):
            for k in keys:
                if k in obj and (obj[k] is not None):
                    return obj[k]
            for v in obj.values():
                res = deep_find(v)
                if res is not None:
                    return res
        elif isinstance(obj, list):
            for item in obj:
                res = deep_find(item)
                if res is not None:
                    return res
        return None
    return deep_find(response)

def send_telegram(text):
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {"chat_id": TELEGRAM_CHAT_ID, "text": text, "parse_mode": "HTML"}
    r = requests.post(url, json=payload, timeout=12)
    if not r.ok:
        print("Telegram send failed:", r.status_code, r.text)
    return r.ok

def nice_change_symbol(delta):
    try:
        d = float(delta)
        if d > 0: return "🔺"
        if d < 0: return "🔻"
        return "⏺"
    except Exception:
        return ""

# ---------------- Main ----------------
def main():
    print("Fetching instruments and matching symbols...")
    insts = fetch_instruments()
    matches = match_symbols(insts, SYMBOLS_TO_TRACK)
    chosen = choose_best_match(matches)

    # Save for user verification
    save_json(chosen, "instruments.json")
    print("Saved instruments.json — please inspect and correct instrument_token values if needed.")

    # prepare tracked tokens
    tracked = {}
    for s, instr in chosen.items():
        if not instr:
            tracked[s] = None
            continue
        # possible keys for token
        token = instr.get("instrument_token") or instr.get("instrumentToken") or instr.get("token") or instr.get("instrumentId") or instr.get("instrument_id")
        tracked[s] = token

    print("Tracking tokens (check instruments.json to confirm):")
    for s, t in tracked.items():
        print(f" - {s}: {t}")

    prev_prices = {s: None for s in SYMBOLS_TO_TRACK}

    while True:
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        lines = [f"📡 <b>Market update</b> — {now}"]
        for s in SYMBOLS_TO_TRACK:
            token = tracked.get(s)
            if not token:
                lines.append(f"<b>{s}</b>: ❗ instrument token missing — edit instruments.json")
                continue
            try:
                q = fetch_quote_for_token(token)
                ltp = extract_ltp(q)
                if ltp is None:
                    # save debug for inspection
                    fname = f"debug_{token}.json"
                    try:
                        save_json(q, fname)
                        lines.append(f"<b>{s}</b>: ❗ couldn't parse LTP (saved {fname})")
                    except Exception:
                        lines.append(f"<b>{s}</b>: ❗ couldn't parse LTP")
                else:
                    # compute change from prev if available
                    prev = prev_prices.get(s)
                    delta = None
                    pct = None
                    try:
                        cur = float(ltp)
                        if prev is not None:
                            delta = cur - prev
                            pct = (delta / prev) * 100 if prev != 0 else None
                        prev_prices[s] = cur
                    except Exception:
                        cur = ltp
                    # format
                    if delta is not None and pct is not None:
                        sign = nice_change_symbol(delta)
                        lines.append(f"<b>{s}</b>: {cur:.2f} {sign} <small>({delta:+.2f}, {pct:+.2f}%)</small>")
                    else:
                        lines.append(f"<b>{s}</b>: {cur}")
            except Exception as e:
                lines.append(f"<b>{s}</b>: error -> {e}")

        message = "\n".join(lines)
        # send (wrap in try to not crash)
        try:
            send_telegram(message)
            print(f"[{datetime.now()}] Sent update to Telegram.")
        except Exception as e:
            print("Telegram send exception:", e)

        time.sleep(POLL_INTERVAL)

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("Interrupted, exiting.")
