"""
DhanHQ -> Telegram LTP Bot with fallback symbol matching
If exact symbol resolution fails, uses substring match (best score) from scrip master.
"""

import os
import time
import csv
import io
import sys
import json
import logging
from typing import List, Dict
from urllib.parse import urljoin, urlencode

import requests
from dotenv import load_dotenv

try:
    import websocket
except ImportError:
    websocket = None

# --- Logging ---
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("dhan-telegram-bot")

# --- Config ---
load_dotenv()
MODE = os.getenv("MODE", "rest").lower()
DHAN_TOKEN = os.getenv("DHAN_TOKEN")
DHAN_CLIENT_ID = os.getenv("DHAN_CLIENT_ID")
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
POLL_INTERVAL = int(os.getenv("POLL_INTERVAL", "60"))
DHAN_API_BASE = os.getenv("DHAN_API_BASE", "https://api.dhan.co/v2")
SCRIP_MASTER_URL = os.getenv("SCRIP_MASTER_URL", "https://images.dhan.co/api-data/api-scrip-master.csv")
SYMBOLS_ENV = os.getenv("SYMBOLS", "")
WS_URL = os.getenv("WS_URL", "wss://api-feed.dhan.co")

if not (DHAN_TOKEN and DHAN_CLIENT_ID and TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID):
    log.error("Environment variables missing. Please set DHAN_TOKEN, DHAN_CLIENT_ID, TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID.")
    sys.exit(1)

SYMBOLS = [s.strip() for s in SYMBOLS_ENV.split(",") if s.strip()]

# --- Scrip master and resolution ---
def download_scrip_master():
    log.info("Downloading scrip master CSV...")
    r = requests.get(SCRIP_MASTER_URL, timeout=20)
    r.raise_for_status()
    text = r.content.decode("utf-8", errors="replace")
    f = io.StringIO(text)
    reader = csv.DictReader(f)
    rows = list(reader)
    log.info(f"Downloaded {len(rows)} scrip rows.")
    return rows, reader.fieldnames

def detect_columns(fieldnames: List[str]):
    trad_cols = [c for c in fieldnames if c.lower() in ("tradingsymbol","trading_symbol","symbol","trade_symbol")]
    name_cols = [c for c in fieldnames if c.lower() in ("name","instrumentname","securityname")]
    id_cols = [c for c in fieldnames if c.lower() in ("securityid","security_id","securityid","sem_smst_security_id")]
    seg_cols = [c for c in fieldnames if c.lower() in ("exchangesegment","segment","exchange","sem_segment")]

    trad_col = trad_cols[0] if trad_cols else None
    name_col = name_cols[0] if name_cols else None
    id_col = id_cols[0] if id_cols else None
    seg_col = seg_cols[0] if seg_cols else None

    log.info(f"Detected columns → trad: {trad_col}, name: {name_col}, id: {id_col}, seg: {seg_col}")
    return trad_col, name_col, id_col, seg_col

def build_index(rows, trad_col, name_col, id_col, seg_col):
    idx = []
    for r in rows:
        t = (r.get(trad_col) or "").strip().upper() if trad_col else ""
        n = (r.get(name_col) or "").strip().upper() if name_col else ""
        sid = r.get(id_col, "")
        seg = r.get(seg_col, "")
        idx.append({"trad": t, "name": n, "sid": sid, "seg": seg})
    return idx

def fallback_resolve(symbols: List[str], idx):
    """
    Try substring matching fallback: for each symbol find best candidate in idx
    Return list of resolved dicts: {symbol, sid, seg}
    """
    resolved = []
    for s in symbols:
        s_up = s.strip().upper()
        best = None
        best_score = -1
        for it in idx:
            # skip empty sid
            if not it["sid"]:
                continue
            score = 0
            # if symbol substring in trad or name
            if s_up == it["trad"]:
                score += 100
            if s_up == it["name"]:
                score += 90
            if it["trad"].startswith(s_up) or it["name"].startswith(s_up):
                score += 50
            if s_up in it["trad"] or s_up in it["name"]:
                score += 20
            if score > best_score:
                best_score = score
                best = it
        if best and best_score > 0:
            log.info(f"Fallback resolved '{s}' → trad={best['trad']} sid={best['sid']} seg={best['seg']} (score {best_score})")
            resolved.append({"symbol": s, "secid": best["sid"], "segment": best["seg"] or "NSE_EQ"})
        else:
            log.warning(f"Fallback unable to resolve '{s}'")
    return resolved

def resolve_symbols(symbols: List[str]):
    rows, fns = download_scrip_master()
    trad_col, name_col, id_col, seg_col = detect_columns(fns)
    idx = build_index(rows, trad_col, name_col, id_col, seg_col)

    resolved = []
    for s in symbols:
        up = s.strip().upper()
        # try exact trad or name match first
        for it in idx:
            if it["trad"] == up or it["name"] == up:
                if it["sid"]:
                    resolved.append({"symbol": s, "secid": it["sid"], "segment": it["seg"] or "NSE_EQ"})
                    break
        else:
            # fallback substring
            fb = fallback_resolve([s], idx)
            if fb:
                resolved.extend(fb)
    return resolved

# --- REST LTP ---
def build_payload(resolved: List[Dict]):
    payload = {}
    for it in resolved:
        seg = it.get("segment") or "NSE_EQ"
        sid = it.get("secid")
        try:
            sid_int = int(sid)
        except:
            continue
        payload.setdefault(seg, []).append(sid_int)
    return payload

def call_ltp_api(payload):
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

def send_telegram(text: str):
    tg_url = f"https://api.telegram.org/bot{TELEGRAM_Bot_TOKEN}/sendMessage"
    data = {"chat_id": TELEGRAM_CHAT_ID, "text": text, "parse_mode": "HTML"}
    try:
        r = requests.post(tg_url, json=data, timeout=10)
        r.raise_for_status()
        return r.json()
    except Exception:
        log.exception("Telegram send error")
        return None

# --- WebSocket (if needed) omitted for brevity; use REST mode for now ---
def main():
    resolved = resolve_symbols(SYMBOLS)
    if not resolved:
        log.error("No symbols resolved even after fallback. Exiting.")
        sys.exit(1)
    log.info("Resolved symbols list: %s", resolved)
    payload = build_payload(resolved)
    log.info("Payload groups: %s", {k: len(v) for k, v in payload.items()})
    last_prices = {}
    while True:
        try:
            resp = call_ltp_api(payload)
            data = resp.get("data") or resp
            messages = []
            for seg, mapping in data.items():
                if isinstance(mapping, dict):
                    for secid_str, info in mapping.items():
                        lp = info.get("last_price") or info.get("ltp") or info.get("lastPrice")
                        human = next((x["symbol"] for x in resolved if str(x["secid"]) == str(secid_str) and x["segment"] == seg), secid_str)
                        prev = last_prices.get(secid_str)
                        change = ""
                        if prev is not None and lp is not None:
                            try:
                                diff = float(lp) - float(prev)
                                pct = (diff / float(prev)) * 100 if float(prev) != 0 else 0
                                change = f" ({diff:+.2f}, {pct:+.2f}%)"
                            except:
                                change = ""
                        if lp is not None:
                            messages.append(f"<b>{human}</b>: {lp}{change}")
                            last_prices[secid_str] = lp
            if messages:
                text = " | ".join(messages)
                send_telegram(text)
                log.info("Sent to Telegram.")
            time.sleep(POLL_INTERVAL)
        except KeyboardInterrupt:
            log.info("Interrupted by user. Exiting.")
            break
        except Exception as e:
            log.exception("Error in main loop, will retry.")
            time.sleep(5)

if __name__ == "__main__":
    main()
