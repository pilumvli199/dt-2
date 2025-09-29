# bot_auto_resolve.py
"""
DhanHQ -> Telegram LTP Bot (automatic scrip-master resolving + AUTO_PICK)

Features:
- Downloads Dhan scrip master CSV (compact/detailed)
- Auto-detects instrument name / id / segment columns (handles SEM_* column names)
- Resolves SYMBOLS via exact/trad/name/token-subset/fuzzy matching
- Accepts SECURITY_IDS env var to bypass resolution entirely
- If AUTO_PICK=true, will auto-pick best match when ambiguous/unresolved
- Supports REST LTP POST /marketfeed/ltp mode (default)
"""

import os, time, csv, io, sys, json
from urllib.parse import urljoin
import requests
from dotenv import load_dotenv
import difflib
import logging

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("dhan-telegram-bot")

# Config
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
AUTO_PICK = os.getenv("AUTO_PICK", "false").lower() in ("1","true","yes","y")

if not (DHAN_TOKEN and DHAN_CLIENT_ID and TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID):
    log.error("Set DHAN_TOKEN, DHAN_CLIENT_ID, TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID in .env")
    sys.exit(1)

SYMBOLS = [s.strip() for s in SYMBOLS_ENV.split(",") if s.strip()]

# If SECURITY_IDS provided, parse and use directly
def parse_security_ids(raw):
    if not raw:
        return None
    parts = [p.strip() for p in raw.split(",") if p.strip()]
    payload = {}
    for p in parts:
        if ":" in p:
            seg, sid = p.split(":",1)
            seg = seg.strip().upper()
            sid = sid.strip()
        else:
            seg = "NSE_EQ"
            sid = p
        try:
            sid_i = int(sid)
        except:
            continue
        payload.setdefault(seg, []).append(sid_i)
    return payload

SECURITY_PAYLOAD = parse_security_ids(SEC_IDS_ENV)

# Download scrip master
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

# Detect columns robustly (including SEM_*)
def detect_columns(fieldnames):
    # candidate lists
    trad_cands = ["tradingsymbol","trading_symbol","symbol","trade_symbol","scrip"]
    name_cands = ["name","instrumentname","securityname","sem_instrument_name","instrument_name","security_name"]
    id_cands = ["securityid","security_id","securityid","sem_smst_security_id","smst_security_id","id"]
    seg_cands = ["exchangesegment","segment","exchange","sem_segment","seg"]
    def find_candidate(cands):
        for fn in fieldnames:
            for c in cands:
                if fn.strip().lower() == c.strip().lower():
                    return fn
        # substring match if exact not found
        for fn in fieldnames:
            for c in cands:
                if c.strip().lower() in fn.strip().lower():
                    return fn
        return None
    trad_col = find_candidate(trad_cands)
    name_col = find_candidate(name_cands)
    id_col = find_candidate(id_cands)
    seg_col = find_candidate(seg_cands)
    log.info("Detected columns -> trad: %s, name: %s, id: %s, seg: %s", trad_col, name_col, id_col, seg_col)
    return trad_col, name_col, id_col, seg_col

# Build index for searching
def build_index(rows, trad_col, name_col, id_col, seg_col):
    idx = []
    for r in rows:
        trad = (r.get(trad_col) or "").strip().upper() if trad_col else ""
        name = (r.get(name_col) or "").strip().upper() if name_col else ""
        sid = (r.get(id_col) or "").strip() if id_col else ""
        seg = (r.get(seg_col) or "").strip() if seg_col else "NSE_EQ"
        # also prepare alt tokens
        tokens = set([t for t in (trad + " " + name).replace("/", " ").split() if t])
        idx.append({"trad": trad, "name": name, "sid": sid, "seg": seg, "tokens": tokens})
    return idx

# Multi-step resolver: exact trad -> exact name -> token-subset -> difflib fuzzy
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
    # token subset: all tokens in q present in item.tokens
    q_tokens = [t for t in q_up.replace("/", " ").split() if t]
    if q_tokens:
        candidates = []
        for it in idx:
            if not it["sid"]:
                continue
            # compute how many q_tokens are in it.tokens
            hit = sum(1 for t in q_tokens if t in it["tokens"])
            if hit>0:
                candidates.append((hit, len(it["tokens"]), it))
        if candidates:
            # prefer higher hit, then smaller token size (more specific)
            candidates.sort(key=lambda x: (-x[0], x[1]))
            return candidates[0][2]
    # fuzzy match on combined label
    names = [(it["trad"] + " " + it["name"]).strip() for it in idx if it["sid"]]
    # get close matches for q_up
    clos = difflib.get_close_matches(q_up, names, n=5, cutoff=0.6)
    if clos:
        # pick first close match -> find corresponding item
        best_name = clos[0]
        for it in idx:
            if (it["trad"] + " " + it["name"]).strip() == best_name and it["sid"]:
                return it
    return None

# Resolve list of SYMBOLS
def resolve_symbols(symbols):
    rows, fns = download_scrip_master(SCRIP_MASTER_URL)
    trad_col, name_col, id_col, seg_col = detect_columns(fns)
    if not id_col:
        log.error("Could not detect security-id column in scrip master. Exiting.")
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
    # handle unresolved: if AUTO_PICK then attempt pick best fuzzy result even if low confidence
    if unresolved and AUTO_PICK:
        log.info("AUTO_PICK enabled: attempting best-guess for unresolved symbols.")
        for s in unresolved:
            # use global names list fallback
            r = resolve_symbol_single(s, idx)
            if r:
                log.info("AUTO_PICK => '%s' -> %s (%s)", s, r["sid"], r["name"] or r["trad"])
                resolved.append({"symbol": s, "secid": r["sid"], "segment": r["seg"] or "NSE_EQ"})
            else:
                log.warning("AUTO_PICK failed for '%s' too.", s)
    return resolved

# Build payload
def build_payload_from_resolved(resolved):
    payload = {}
    for it in resolved:
        try:
            sid_i = int(it["secid"])
        except:
            continue
        seg = (it.get("segment") or "NSE_EQ")
        payload.setdefault(seg, []).append(sid_i)
    return payload

# Call LTP API
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

# Telegram
def send_telegram(text):
    tg_url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    data = {"chat_id": TELEGRAM_CHAT_ID, "text": text, "parse_mode": "HTML"}
    try:
        r = requests.post(tg_url, json=data, timeout=10)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        log.exception("Telegram send failed: %s", e)
        return None

# Main driver
def main():
    # If user provided SECURITY_IDS explicitly -> use them and skip resolution
    if SECURITY_PAYLOAD:
        payload = SECURITY_PAYLOAD
        log.info("Using SECURITY_IDS from env directly: %s", payload)
    else:
        if not SYMBOLS:
            log.error("No SYMBOLS provided and no SECURITY_IDS. Set SYMBOLS or SECURITY_IDS in .env.")
            sys.exit(1)
        resolved = resolve_symbols(SYMBOLS)
        if not resolved:
            log.error("No symbols resolved (and AUTO_PICK disabled or failed). Exiting.")
            sys.exit(1)
        payload = build_payload_from_resolved(resolved)
        log.info("Built payload groups: %s", {k: len(v) for k,v in payload.items()})
    # One-shot test call then enter poll loop
    def poll_loop():
        last = {}
        while True:
            try:
                resp = call_ltp(payload)
                data = resp.get("data") or resp
                messages = []
                if isinstance(data, dict):
                    for seg, mapping in data.items():
                        if isinstance(mapping, dict):
                            for secid, info in mapping.items():
                                lp = info.get("last_price") or info.get("ltp") or info.get("lastPrice")
                                human = next((x["symbol"] for x in (resolved if 'resolved' in locals() else []) if str(x["secid"])==str(secid) and x["segment"]==seg), str(secid))
                                prev = last.get(secid)
                                change = ""
                                if prev is not None and lp is not None:
                                    try:
                                        diff = float(lp) - float(prev)
                                        pct = (diff/float(prev))*100 if float(prev)!=0 else 0
                                        change = f" ({diff:+.2f}, {pct:+.2f}%)"
                                    except:
                                        change = ""
                                if lp is not None:
                                    messages.append(f"<b>{human}</b>: {lp}{change}")
                                    last[secid] = lp
                if messages:
                    txt = " | ".join(messages)
                    send_telegram(txt)
                    log.info("Sent update: %s", txt)
                time.sleep(POLL_INTERVAL)
            except KeyboardInterrupt:
                log.info("Interrupted")
                break
            except Exception:
                log.exception("Error in poll loop; retrying after short sleep")
                time.sleep(5)
    # run poll
    poll_loop()

if __name__ == "__main__":
    main()
