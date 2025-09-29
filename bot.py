# bot_auto_resolve.py
"""
Updated DhanHQ -> Telegram LTP Bot (robust)
- Auto-resolve symbols via scrip master (supports SEM_* column names)
- Segment normalization (SEG_MAP) to API-expected keys
- Retry/backoff for LTP calls
- Telegram sending guarded to avoid crash
- AUTO_PICK option to auto-choose best guess if unresolved
- Use SECURITY_IDS env to bypass resolution if provided
"""

import os
import time
import csv
import io
import sys
import json
import logging
import difflib
from urllib.parse import urljoin
import requests
from dotenv import load_dotenv

# --- Config & logging ---
load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("dhan-telegram-bot")

MODE = os.getenv("MODE", "rest").lower()
DHAN_TOKEN = os.getenv("DHAN_TOKEN")
DHAN_CLIENT_ID = os.getenv("DHAN_CLIENT_ID")
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
POLL_INTERVAL = int(os.getenv("POLL_INTERVAL", "60"))
DHAN_API_BASE = os.getenv("DHAN_API_BASE", "https://api.dhan.co/v2")
SCRIP_MASTER_URL = os.getenv("SCRIP_MASTER_URL", "https://images.dhan.co/api-data/api-scrip-master.csv")
SYMBOLS_ENV = os.getenv("SYMBOLS", "")
SEC_IDS_ENV = os.getenv("SECURITY_IDS", "")
AUTO_PICK = os.getenv("AUTO_PICK", "false").lower() in ("1", "true", "yes", "y")
MAX_IDS_PER_CALL = 1000  # Dhan LTP supports up to ~1000 instruments per call

# Quick sanity checks
if not (DHAN_TOKEN and DHAN_CLIENT_ID):
    log.error("Missing DHAN_TOKEN or DHAN_CLIENT_ID in environment. Fill .env and restart.")
    sys.exit(1)
if not (TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID):
    log.warning("Telegram credentials missing or incomplete. Bot will run but won't send Telegram messages.")

SYMBOLS = [s.strip() for s in SYMBOLS_ENV.split(",") if s.strip()]

# Segment normalization map (CSV short -> API expected)
SEG_MAP = {
    "E": "NSE_EQ",
    "EQ": "NSE_EQ",
    "NSE_EQ": "NSE_EQ",
    "I": "NSE_INDEX",
    "IND": "NSE_INDEX",
    "INDEX": "NSE_INDEX",
    "FNO": "NSE_FNO",
    "NSE_FNO": "NSE_FNO",
    "BSE_EQ": "BSE_EQ",
    "MCX_COMM": "MCX_COMM"
}

# --- SECURITY_IDS parser (if provided explicitly) ---
def parse_security_ids(raw):
    if not raw:
        return None
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
        try:
            sid_i = int(sid)
            api_seg = SEG_MAP.get(seg, seg)
            payload.setdefault(api_seg, []).append(sid_i)
        except Exception:
            log.warning("Ignoring invalid security id part: %s", p)
    return payload

SECURITY_PAYLOAD = parse_security_ids(SEC_IDS_ENV)

# --- Scrip master download + detection + index build ---
def download_scrip_master(url):
    log.info("Downloading scrip master CSV...")
    r = requests.get(url, timeout=30)
    r.raise_for_status()
    txt = r.content.decode("utf-8", errors="replace")
    f = io.StringIO(txt)
    reader = csv.DictReader(f)
    rows = list(reader)
    log.info("Downloaded %d rows. Fieldnames: %s", len(rows), reader.fieldnames)
    return rows, reader.fieldnames

def detect_columns(fieldnames):
    # candidates include SEM_* names observed in Dhan scrip master
    trad_cands = ["tradingsymbol", "trading_symbol", "sem_trading_symbol", "sm_symbol_name", "symbol", "scrip"]
    name_cands = ["name", "instrumentname", "securityname", "sem_instrument_name", "instrument_name"]
    id_cands = ["securityid", "security_id", "sem_smst_security_id", "smst_security_id", "id"]
    seg_cands = ["exchangesegment", "segment", "sem_segment", "sem_exm_exch_id", "exchange"]

    def find_any(cands):
        for fn in fieldnames:
            for c in cands:
                if fn.strip().lower() == c.strip().lower():
                    return fn
        for fn in fieldnames:
            for c in cands:
                if c.strip().lower() in fn.strip().lower():
                    return fn
        return None

    trad_col = find_any(trad_cands)
    name_col = find_any(name_cands)
    id_col = find_any(id_cands)
    seg_col = find_any(seg_cands)
    log.info("Detected columns -> trad: %s, name: %s, id: %s, seg: %s", trad_col, name_col, id_col, seg_col)
    return trad_col, name_col, id_col, seg_col

def build_index(rows, trad_col, name_col, id_col, seg_col):
    idx = []
    for r in rows:
        trad = (r.get(trad_col) or "").strip().upper() if trad_col else ""
        name = (r.get(name_col) or "").strip().upper() if name_col else ""
        sid = (r.get(id_col) or "").strip() if id_col else ""
        seg = (r.get(seg_col) or "").strip() if seg_col else "NSE_EQ"
        tokens = set((trad + " " + name).replace("/", " ").split())
        idx.append({"trad": trad, "name": name, "sid": sid, "seg": seg, "tokens": tokens})
    return idx

# Multi-step symbol resolver
def resolve_symbol_single(q, idx):
    q_up = q.strip().upper()
    # exact trad
    for it in idx:
        if it["trad"] == q_up and it["sid"]:
            return it
    # exact name
    for it in idx:
        if it["name"] == q_up and it["sid"]:
            return it
    # token subset match
    q_tokens = [t for t in q_up.replace("/", " ").split() if t]
    if q_tokens:
        cand = []
        for it in idx:
            if not it["sid"]:
                continue
            hit = sum(1 for t in q_tokens if t in it["tokens"])
            if hit > 0:
                cand.append((hit, len(it["tokens"]), it))
        if cand:
            cand.sort(key=lambda x: (-x[0], x[1]))
            return cand[0][2]
    # fuzzy match
    names = [(it["trad"] + " " + it["name"]).strip() for it in idx if it["sid"]]
    close = difflib.get_close_matches(q_up, names, n=5, cutoff=0.6)
    if close:
        best_name = close[0]
        for it in idx:
            if (it["trad"] + " " + it["name"]).strip() == best_name and it["sid"]:
                return it
    return None

def resolve_symbols(symbols):
    rows, fns = download_scrip_master(SCRIP_MASTER_URL)
    trad_col, name_col, id_col, seg_col = detect_columns(fns)
    if not id_col:
        log.error("Could not detect security-id column in scrip master. Exiting resolution.")
        return []
    idx = build_index(rows, trad_col, name_col, id_col, seg_col)
    resolved = []
    unresolved = []
    for s in symbols:
        if not s:
            continue
        r = resolve_symbol_single(s, idx)
        if r:
            log.info("Resolved symbol '%s' -> trad=%s sid=%s seg=%s", s, r["trad"], r["sid"], r["seg"])
            resolved.append({"symbol": s, "secid": r["sid"], "segment": r["seg"] or "NSE_EQ"})
        else:
            log.warning("Could NOT resolve '%s' via auto methods.", s)
            unresolved.append(s)
    if unresolved and AUTO_PICK:
        log.info("AUTO_PICK enabled: attempting best-guess for unresolved symbols.")
        for s in unresolved:
            r = resolve_symbol_single(s, idx)
            if r:
                log.info("AUTO_PICK => '%s' -> %s (%s)", s, r["sid"], r["name"] or r["trad"])
                resolved.append({"symbol": s, "secid": r["sid"], "segment": r["seg"] or "NSE_EQ"})
            else:
                log.warning("AUTO_PICK failed for '%s'.", s)
    return resolved

# Build payload with seg normalization
def build_payload_from_resolved(resolved):
    payload = {}
    for it in resolved:
        try:
            sid_i = int(it["secid"])
        except Exception:
            log.warning("Skipping non-int secid: %s", it.get("secid"))
            continue
        seg_raw = str(it.get("segment") or "NSE_EQ").strip().upper()
        seg_api = SEG_MAP.get(seg_raw, seg_raw)
        payload.setdefault(seg_api, []).append(sid_i)
    # safety: warn if too many ids
    total_ids = sum(len(v) for v in payload.values())
    if total_ids > MAX_IDS_PER_CALL:
        log.warning("Total instrument count %d exceeds %d - consider increasing POLL_INTERVAL or grouping.", total_ids, MAX_IDS_PER_CALL)
    return payload

# LTP call with retries and non-fatal behaviour
def call_ltp(payload):
    url = urljoin(DHAN_API_BASE + "/", "marketfeed/ltp")
    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "access-token": DHAN_TOKEN,
        "client-id": DHAN_CLIENT_ID
    }
    tries = 0
    while tries < 4:
        try:
            r = requests.post(url, headers=headers, json=payload, timeout=10)
            r.raise_for_status()
            return r.json()
        except Exception as e:
            tries += 1
            log.warning("LTP call failed (try %d): %s", tries, e)
            time.sleep(1 + tries * 2)
    log.error("LTP call failed after retries; returning empty response.")
    return {}

# Safe telegram send
def send_telegram(text):
    if not (TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID):
        log.debug("Telegram credentials missing - skipping send.")
        return None
    try:
        tg_url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
        data = {"chat_id": TELEGRAM_CHAT_ID, "text": text, "parse_mode": "HTML"}
        r = requests.post(tg_url, json=data, timeout=8)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        log.exception("Telegram send failed: %s", e)
        return None

# Main loop
def main():
    if SECURITY_PAYLOAD:
        payload = SECURITY_PAYLOAD
        log.info("Using SECURITY_IDS from env directly: %s", payload)
        resolved = None
    else:
        if not SYMBOLS:
            log.error("No SYMBOLS provided and no SECURITY_IDS. Set SYMBOLS or SECURITY_IDS in .env.")
            sys.exit(1)
        resolved = resolve_symbols(SYMBOLS)
        if not resolved:
            log.error("No symbols resolved (and AUTO_PICK disabled or failed). Exiting.")
            sys.exit(1)
        payload = build_payload_from_resolved(resolved)
        log.info("Built payload groups: %s", {k: len(v) for k, v in payload.items()})

    last_prices = {}
    while True:
        try:
            resp = call_ltp(payload)
            # Dhan response could be {'data': {...}} or direct mapping
            data = resp.get("data") if isinstance(resp, dict) and "data" in resp else resp
            messages = []
            if isinstance(data, dict):
                for seg, mapping in data.items():
                    if not isinstance(mapping, dict):
                        continue
                    for secid, info in mapping.items():
                        lp = info.get("last_price") or info.get("ltp") or info.get("lastPrice")
                        human = None
                        if resolved:
                            human = next((x["symbol"] for x in resolved if str(x["secid"]) == str(secid) and SEG_MAP.get(str(x.get("segment","")).upper(), str(x.get("segment","")).upper()) == str(seg).upper()), None)
                        if not human:
                            human = f"{seg}:{secid}"
                        prev = last_prices.get(secid)
                        change = ""
                        if prev is not None and lp is not None:
                            try:
                                diff = float(lp) - float(prev)
                                pct = (diff / float(prev)) * 100 if float(prev) != 0 else 0
                                change = f" ({diff:+.2f}, {pct:+.2f}%)"
                            except Exception:
                                change = ""
                        if lp is not None:
                            messages.append(f"<b>{human}</b>: {lp}{change}")
                            last_prices[secid] = lp
            else:
                messages.append(str(data)[:1000])
            if messages:
                text = "<b>LTP Update</b>\n" + "\n".join(messages)
                send_telegram(text)
                log.info("Sent update to Telegram.")
            time.sleep(POLL_INTERVAL)
        except KeyboardInterrupt:
            log.info("Interrupted by user. Exiting.")
            break
        except Exception:
            log.exception("Unexpected error in main loop; retrying after short sleep.")
            time.sleep(5)

if __name__ == "__main__":
    main()
