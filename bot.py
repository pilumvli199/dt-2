"""
DhanHQ -> Telegram LTP Bot (REST or WebSocket mode)

Configure in .env:
- MODE=rest  # or ws
- DHAN_TOKEN
- DHAN_CLIENT_ID
- TELEGRAM_BOT_TOKEN
- TELEGRAM_CHAT_ID
- POLL_INTERVAL=60
- SYMBOLS=NIFTY 50,BANKNIFTY,SENSEX,TATAMOTORS,RELIANCE,TCS
- DHAN_API_BASE=https://api.dhan.co/v2
- WS_URL (optional) e.g. wss://api-feed.dhan.co?version=2
"""

import os
import time
import csv
import io
import sys
import json
import logging
import threading
from typing import List, Dict
from urllib.parse import urljoin, urlencode

import requests
from dotenv import load_dotenv

# Optional websocket-client (only needed for WS mode)
try:
    import websocket
except Exception:
    websocket = None

# --- Logging ---
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("dhan-telegram-bot")

# --- Config ---
load_dotenv()
MODE = os.getenv("MODE", "rest").lower()   # rest or ws
DHAN_TOKEN = os.getenv("DHAN_TOKEN")
DHAN_CLIENT_ID = os.getenv("DHAN_CLIENT_ID")
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
POLL_INTERVAL = int(os.getenv("POLL_INTERVAL", "60"))
DHAN_API_BASE = os.getenv("DHAN_API_BASE", "https://api.dhan.co/v2")
SCRIP_MASTER_URL = os.getenv("SCRIP_MASTER_URL", "https://images.dhan.co/api-data/api-scrip-master.csv")
SYMBOLS_ENV = os.getenv("SYMBOLS", "NIFTY 50,BANKNIFTY,SENSEX,TATAMOTORS,RELIANCE,TCS")
WS_URL = os.getenv("WS_URL", "wss://api-feed.dhan.co")  # base, we'll add query params

if not all([DHAN_TOKEN, DHAN_CLIENT_ID, TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID]):
    log.error("Missing required environment variables. Fill .env (DHAN_TOKEN, DHAN_CLIENT_ID, TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID).")
    sys.exit(1)

SYMBOLS = [s.strip() for s in SYMBOLS_ENV.split(",") if s.strip()]

# --- Helpers for scrip master ---
def download_instrument_master():
    log.info("Downloading scrip master CSV...")
    r = requests.get(SCRIP_MASTER_URL, timeout=15)
    r.raise_for_status()
    text = r.content.decode("utf-8", errors="replace")
    f = io.StringIO(text)
    reader = csv.DictReader(f)
    rows = list(reader)
    log.info(f"Downloaded {len(rows)} instruments.")
    return rows

def build_lookup(rows):
    # detect columns (robust)
    trad_col = next((c for c in ["tradingsymbol","trading_symbol","symbol","TRADE_SYMBOL","scrip"] if c in rows[0]), None)
    name_col = next((c for c in ["name","instrumentName","securityname","Name"] if c in rows[0]), None)
    secid_col = next((c for c in ["securityID","security_id","securityid","id"] if c in rows[0]), None)
    seg_col = next((c for c in ["exchangeSegment","segment","exchange_segment","exchange"] if c in rows[0]), None)

    by_trad = {}
    by_name = {}
    by_full = {}
    for r in rows:
        t = r.get(trad_col,"").strip().upper() if trad_col else ""
        n = r.get(name_col,"").strip().upper() if name_col else ""
        sid = r.get(secid_col,"").strip() if secid_col else ""
        seg = r.get(seg_col,"").strip() if seg_col else "NSE_EQ"
        if t:
            by_trad.setdefault(t, []).append({"secid": sid, "segment": seg})
        if n:
            by_name.setdefault(n, []).append({"secid": sid, "segment": seg})
        full = f"{t} {n}".strip()
        if full:
            by_full.setdefault(full, []).append({"secid": sid, "segment": seg})
    return {"trad": by_trad, "name": by_name, "full": by_full}

def resolve_symbols(symbols: List[str], lookup) -> List[Dict]:
    resolved = []
    for s in symbols:
        s_up = s.upper()
        found = None
        if s_up in lookup["trad"]:
            found = lookup["trad"][s_up][0]
        elif s_up in lookup["name"]:
            found = lookup["name"][s_up][0]
        else:
            # substring search
            for full, arr in lookup["full"].items():
                if s_up in full:
                    found = arr[0]
                    break
        if found and found.get("secid"):
            resolved.append({"symbol": s, "secid": str(found["secid"]), "segment": found.get("segment","NSE_EQ")})
        else:
            log.warning(f"Could not resolve symbol '{s}'.")
    return resolved

# --- Dhan REST LTP ---
def build_payload(resolved):
    payload = {}
    for it in resolved:
        seg = it.get("segment") or "NSE_EQ"
        sid = it.get("secid")
        payload.setdefault(seg, []).append(int(sid))
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

# --- Telegram ---
def send_telegram(text: str):
    tg_url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    data = {"chat_id": TELEGRAM_CHAT_ID, "text": text, "parse_mode": "HTML"}
    try:
        r = requests.post(tg_url, json=data, timeout=10)
        r.raise_for_status()
        return r.json()
    except Exception:
        log.exception("Telegram send failed")
        return None

# --- WebSocket mode ---
class DhanWSClient:
    def __init__(self, resolved):
        if websocket is None:
            raise RuntimeError("websocket-client library not installed. pip install websocket-client")
        self.resolved = resolved
        self.ws = None
        # build instrument list
        self.instruments = [{"ExchangeSegment": it["segment"], "SecurityId": str(it["secid"])} for it in resolved]

    def _build_url(self):
        # add token and clientId and authType=2 per doc
        q = {"version": "2", "token": DHAN_TOKEN, "clientId": DHAN_CLIENT_ID, "authType": 2}
        return WS_URL + "?" + urlencode(q)

    def _on_open(self, ws):
        log.info("WS opened, sending subscribe request...")
        req = {
            "RequestCode": 15,  # Full feed (use 11/12/15 per docs)
            "InstrumentCount": len(self.instruments),
            "InstrumentList": self.instruments
        }
        ws.send(json.dumps(req))

    def _on_message(self, ws, message):
        try:
            obj = json.loads(message)
        except Exception:
            log.debug("Non-JSON WS message received")
            return
        # parse expected payload - this will depend on Dhan's ws format
        # We'll try to extract secid and last_price if present, else forward full message to telegram (compact)
        entries = []
        # common patterns: dict with 'data' or direct updates - be tolerant
        if isinstance(obj, dict):
            # try common keys
            if "last_price" in obj:
                lp = obj.get("last_price")
                secid = obj.get("securityId") or obj.get("securityid") or obj.get("securityID")
                entries.append(f"{secid}: {lp}")
            elif "data" in obj:
                # data can be mapping
                for k, v in obj["data"].items():
                    if isinstance(v, dict):
                        lp = v.get("last_price") or v.get("ltp")
                        if lp is not None:
                            entries.append(f"{k}: {lp}")
            else:
                # fallback - attach short summary
                entries.append(json.dumps(obj)[:800])
        if entries:
            text = " | ".join(entries)
            send_telegram(text)

    def _on_error(self, ws, error):
        log.error("WS error: %s", error)

    def _on_close(self, ws, close_status_code, close_msg):
        log.warning("WS closed: %s %s", close_status_code, close_msg)

    def run_forever(self):
        url = self._build_url()
        # weak reconnect loop
        while True:
            try:
                self.ws = websocket.WebSocketApp(url,
                                                on_open=self._on_open,
                                                on_message=self._on_message,
                                                on_error=self._on_error,
                                                on_close=self._on_close)
                log.info("Connecting to WS: %s", url)
                self.ws.run_forever(ping_interval=30, ping_timeout=10)
            except KeyboardInterrupt:
                log.info("WS interrupted by user")
                break
            except Exception:
                log.exception("WS connection error, will retry in 5s")
                time.sleep(5)

# --- Main ---
def main():
    rows = download_instrument_master()
    lookup = build_lookup(rows)
    resolved = resolve_symbols(SYMBOLS, lookup)
    if not resolved:
        log.error("No symbols resolved. Exiting.")
        sys.exit(1)
    log.info("Resolved symbols: %s", ", ".join([f"{r['symbol']}({r['secid']})" for r in resolved]))

    if MODE == "ws":
        log.info("Starting in WS mode")
        ws_client = DhanWSClient(resolved)
        ws_client.run_forever()
        return

    # REST mode
    payload = build_payload(resolved)
    log.info("Starting REST poll mode, payload groups: %s", {k: len(v) for k, v in payload.items()})
    last_prices = {}
    while True:
        try:
            resp = call_ltp_api(payload)
            if not resp:
                log.warning("Empty response")
                time.sleep(POLL_INTERVAL)
                continue
            # Dhan typical success response has "data" mapping
            data = resp.get("data") or resp
            messages = []
            # handle both nested and flat structures
            if isinstance(data, dict):
                for seg, mapping in data.items():
                    if not isinstance(mapping, dict):
                        continue
                    for secid, info in mapping.items():
                        lp = info.get("last_price") or info.get("ltp") or info.get("lastPrice")
                        human = next((x["symbol"] for x in resolved if str(x["secid"]) == str(secid) and x.get("segment")==seg), None)
                        human = human or str(secid)
                        prev = last_prices.get(secid)
                        change = ""
                        if prev is not None and lp is not None:
                            try:
                                diff = float(lp) - float(prev)
                                pct = (diff/float(prev))*100 if float(prev) != 0 else 0
                                change = f" ({diff:+.2f}, {pct:+.2f}%)"
                            except Exception:
                                change = ""
                        if lp is not None:
                            messages.append(f"<b>{human}</b>: {lp}{change}")
                            last_prices[secid] = lp
            if messages:
                send_telegram(" | ".join(messages))
                log.info("Sent update to Telegram.")
            time.sleep(POLL_INTERVAL)
        except KeyboardInterrupt:
            log.info("Interrupted by user. Exiting.")
            break
        except Exception:
            log.exception("Error in REST loop - will continue after short sleep.")
            time.sleep(5)

if __name__ == "__main__":
    main()
