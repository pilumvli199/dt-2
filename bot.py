# bot_auto_resolve.py
"""
DhanHQ -> Telegram LTP Bot (robust v3)
- Same features as before, plus human-readable names in Telegram updates.
- If SECURITY_IDS provided, attempts to map secids -> instrument names by downloading scrip-master.
- Message format: timestamp + <b>NAME (SEG)</b>: PRICE (diff, pct)
"""

import os, time, csv, io, sys, logging, difflib, requests
from urllib.parse import urljoin
from dotenv import load_dotenv
from datetime import datetime, timezone, timedelta

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
AUTO_PICK = os.getenv("AUTO_PICK", "false").lower() in ("1","true","yes","y")
ALERT_PCT = float(os.getenv("ALERT_PCT", "0"))
COOLDOWN_SEC = int(os.getenv("COOLDOWN_SEC", "0"))

if not (DHAN_TOKEN and DHAN_CLIENT_ID):
    log.error("Missing DHAN_TOKEN or DHAN_CLIENT_ID in .env")
    sys.exit(1)
if not (TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID):
    log.warning("Telegram creds missing → will not send messages.")

SYMBOLS = [s.strip() for s in SYMBOLS_ENV.split(",") if s.strip()]

# Segment normalization
SEG_MAP = {
    "E":"NSE_EQ","EQ":"NSE_EQ","NSE":"NSE_EQ","NSE_EQ":"NSE_EQ",
    "I":"NSE_INDEX","INDEX":"NSE_INDEX","NSE_INDEX":"NSE_INDEX",
    "BSE":"BSE_EQ","BSE_EQ":"BSE_EQ","MCX_COMM":"MCX_COMM",
    "FNO":"NSE_FNO","NSE_FNO":"NSE_FNO"
}

# --- SECURITY_IDS parser ---
def parse_security_ids(raw):
    if not raw: return None
    payload={}
    for p in [x.strip() for x in raw.split(",") if x.strip()]:
        seg,sid=("NSE_EQ",p)
        if ":" in p: seg,sid=p.split(":",1)
        try:
            sid_i=int(sid.strip())
            seg_key=SEG_MAP.get(seg.strip().upper(),seg.strip().upper())
            payload.setdefault(seg_key,[]).append(sid_i)
        except Exception:
            log.warning("Invalid security id part (ignored): %s",p)
    return payload

SECURITY_PAYLOAD = parse_security_ids(SEC_IDS_ENV)

# --- Scrip master helpers ---
def download_scrip_master():
    log.info("Downloading scrip master...")
    r = requests.get(SCRIP_MASTER_URL, timeout=30)
    r.raise_for_status()
    txt = r.content.decode("utf-8", errors="replace")
    f = io.StringIO(txt)
    reader = csv.DictReader(f)
    rows = list(reader)
    log.info("Downloaded %d rows. Fieldnames: %s", len(rows), reader.fieldnames)
    return rows, reader.fieldnames

def detect_columns(fieldnames):
    trad_cands = ["sem_trading_symbol","tradingsymbol","sm_symbol_name","trading_symbol","symbol"]
    name_cands = ["sem_instrument_name","instrumentname","name","securityname"]
    id_cands = ["sem_smst_security_id","securityid","security_id","id"]
    seg_cands = ["sem_segment","sem_exm_exch_id","segment","exchange"]

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
        seg = (r.get(seg_col) or "").strip() if seg_col else ""
        tokens = set((trad + " " + name).replace("/", " ").split())
        idx.append({"trad": trad, "name": name, "sid": sid, "seg": seg, "tokens": tokens})
    return idx

# resolver (same multi-step)
def resolve_symbol_single(q, idx):
    q_up = q.strip().upper()
    for it in idx:
        if it["trad"] == q_up and it["sid"]: return it
    for it in idx:
        if it["name"] == q_up and it["sid"]: return it
    q_tokens = [t for t in q_up.replace("/", " ").split() if t]
    if q_tokens:
        cand = []
        for it in idx:
            if not it["sid"]: continue
            hit = sum(1 for t in q_tokens if t in it["tokens"])
            if hit>0: cand.append((hit,len(it["tokens"]),it))
        if cand:
            cand.sort(key=lambda x:(-x[0], x[1])); return cand[0][2]
    names = [(it["trad"] + " " + it["name"]).strip() for it in idx if it["sid"]]
    close = difflib.get_close_matches(q_up, names, n=1, cutoff=0.6)
    if close:
        best = close[0]
        for it in idx:
            if (it["trad"] + " " + it["name"]).strip() == best and it["sid"]:
                return it
    return None

def resolve_symbols(symbols):
    rows, fns = download_scrip_master()
    trad_col, name_col, id_col, seg_col = detect_columns(fns)
    if not id_col:
        log.error("security-id column not detected in scrip master.")
        return []
    idx = build_index(rows, trad_col, name_col, id_col, seg_col)
    resolved = []
    unresolved = []
    for s in symbols:
        if not s: continue
        r = resolve_symbol_single(s, idx)
        if r:
            log.info("Resolved %s -> sid=%s seg=%s", s, r["sid"], r["seg"])
            resolved.append({"symbol": s, "secid": r["sid"], "segment": r["seg"], "trad": r["trad"], "name": r["name"]})
        else:
            log.warning("Unresolved: %s", s); unresolved.append(s)
    if unresolved and AUTO_PICK:
        log.info("AUTO_PICK trying best guess for unresolved symbols.")
        for s in unresolved:
            r = resolve_symbol_single(s, idx)
            if r:
                resolved.append({"symbol": s, "secid": r["sid"], "segment": r["seg"], "trad": r["trad"], "name": r["name"]})
    return resolved

# When SECURITY_PAYLOAD used, try to map secids -> human names by scanning scrip-master
def map_secids_to_names_from_csv(security_payload):
    rows, fns = download_scrip_master()
    # detect columns
    trad_col, name_col, id_col, seg_col = detect_columns(fns)
    name_map = {}
    # build lookup: (seg_normalized, secid) -> (trad or name)
    for r in rows:
        sid = (r.get(id_col) or "").strip()
        seg_raw = (r.get(seg_col) or "").strip().upper() if seg_col else ""
        # normalize seg to API key
        seg_key = seg_raw
        if "NSE" in seg_raw and "INDEX" in seg_raw:
            seg_key = "NSE_INDEX"
        elif "NSE" in seg_raw:
            seg_key = "NSE_EQ"
        elif "BSE" in seg_raw:
            seg_key = "BSE_EQ"
        else:
            seg_key = seg_raw
        if sid:
            human = (r.get(trad_col) or r.get(name_col) or "").strip()
            name_map.setdefault((seg_key, sid), human)
    # now for each requested secid in payload, pick mapping if exists
    result = {}
    for seg, ids in security_payload.items():
        for sid in ids:
            key = (seg.upper(), str(sid))
            # try exact
            if key in name_map:
                result[key] = name_map[key]
            else:
                # try matching sid ignoring seg
                for (kseg, ks), v in name_map.items():
                    if ks == str(sid):
                        result[key] = v
                        break
                else:
                    result[key] = f"{seg}:{sid}"
    return result

# Build payload for API from resolved list
def build_payload_from_resolved(resolved):
    payload = {}
    human_map = {}  # (seg_api, sid_str) -> human display
    for it in resolved:
        try:
            sid_i = int(it["secid"])
        except:
            continue
        seg_raw = str(it.get("segment") or "").strip().upper()
        seg_api = SEG_MAP.get(seg_raw, seg_raw)
        payload.setdefault(seg_api, []).append(sid_i)
        # prefer trad then name then given symbol
        display = it.get("trad") or it.get("name") or it.get("symbol") or f"{seg_api}:{sid_i}"
        human_map[(seg_api.upper(), str(sid_i))] = display
    return payload, human_map

# Build payload when SECURITY_PAYLOAD given (no resolved)
def build_payload_from_security_payload(security_payload):
    # security_payload: {seg_api: [ids]}
    payload = {}
    for seg, ids in security_payload.items():
        seg_api = SEG_MAP.get(seg.strip().upper(), seg.strip().upper())
        payload.setdefault(seg_api, []).extend(ids)
    return payload

# LTP call with retries
def call_ltp(payload):
    url = urljoin(DHAN_API_BASE + "/", "marketfeed/ltp")
    headers = {"Accept":"application/json","Content-Type":"application/json","access-token":DHAN_TOKEN,"client-id":DHAN_CLIENT_ID}
    tries = 0
    while tries < 4:
        try:
            r = requests.post(url, headers=headers, json=payload, timeout=10)
            r.raise_for_status()
            return r.json()
        except Exception as e:
            tries += 1
            log.warning("LTP call failed (try %d): %s", tries, e)
            time.sleep(1 + tries*2)
    log.error("LTP call failed after retries; returning empty.")
    return {}

# Telegram send
def send_telegram(text):
    if not (TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID):
        log.debug("No Telegram creds; skipping send.")
        return None
    try:
        tg_url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
        payload = {"chat_id": TELEGRAM_CHAT_ID, "text": text, "parse_mode": "HTML"}
        r = requests.post(tg_url, json=payload, timeout=8); r.raise_for_status()
        return r.json()
    except Exception as e:
        log.exception("Telegram send failed: %s", e)
        return None

# Alert / cooldown helpers
last_sent_time = {}  # map (seg,sid) -> timestamp
def should_notify(key, prev, curr):
    # key = (seg_api, sid_str)
    try:
        if prev is None:
            if ALERT_PCT <= 0:
                pass
            else:
                return False
        if prev is None:
            pct = 0.0
        else:
            diff = float(curr) - float(prev)
            pct = abs(diff / float(prev)) * 100.0
    except Exception:
        pct = 0.0
    if ALERT_PCT > 0 and pct < ALERT_PCT:
        return False
    if COOLDOWN_SEC > 0:
        last = last_sent_time.get(key)
        now = time.time()
        if last and (now - last) < COOLDOWN_SEC:
            return False
        last_sent_time[key] = time.time()
    return True

# timestamp helper (IST)
def now_ist_str():
    # IST = UTC+5:30
    ist = datetime.now(timezone.utc) + timedelta(hours=5, minutes=30)
    return ist.strftime("%Y-%m-%d %H:%M:%S IST")

# Main
def main():
    human_map = {}
    if SECURITY_PAYLOAD:
        # we have explicit secids; try to map to names from CSV
        payload = build_payload_from_security_payload(SECURITY_PAYLOAD)
        # create human_map from CSV if possible
        try:
            human_map = map_secids_to_names_from_csv(SECURITY_PAYLOAD)
        except Exception as e:
            log.warning("Could not map secids to names via CSV: %s", e)
    else:
        if not SYMBOLS:
            log.error("No SYMBOLS or SECURITY_IDS provided.")
            sys.exit(1)
        resolved = resolve_symbols(SYMBOLS)
        if not resolved:
            log.error("No symbols resolved (and AUTO_PICK disabled/failed). Exiting.")
            sys.exit(1)
        payload, hm = build_payload_from_resolved(resolved)
        # hm keys are tuples (seg,sid) where seg already API-normalized
        # convert to string-keyed mapping consistent with map_secids function
        human_map = {(k[0].upper(), k[1]): hm[k] for k in hm}

    log.info("Final payload groups: %s", {k: len(v) for k,v in payload.items()})
    last_prices = {}

    # main poll loop
    while True:
        try:
            resp = call_ltp(payload)
            data = resp.get("data") if isinstance(resp, dict) and "data" in resp else resp
            messages = []
            unavailable_msgs = []
            if isinstance(data, dict):
                for seg, mapping in data.items():
                    if not isinstance(mapping, dict): continue
                    for secid_str, info in mapping.items():
                        lp = info.get("last_price") or info.get("ltp") or info.get("lastPrice")
                        key = (seg.upper(), str(secid_str))
                        display = human_map.get(key) or human_map.get((seg.upper(), str(secid_str))) or f"{seg}:{secid_str}"
                        prev = last_prices.get((seg.upper(), str(secid_str)))
                        change = ""
                        if lp is None:
                            # LTP unavailable - prepare small message
                            unavailable_msgs.append(f"[{now_ist_str()}] {display} ({seg}): LTP unavailable")
                            continue
                        try:
                            if prev is not None:
                                diff = float(lp) - float(prev)
                                pct = (diff / float(prev)) * 100 if float(prev) != 0 else 0
                                change = f" ({diff:+.2f}, {pct:+.2f}%)"
                            else:
                                change = ""
                        except Exception:
                            change = ""
                        # notify decision
                        if should_notify(key, prev, lp):
                            messages.append(f"<b>{display} ({seg})</b>: {lp}{change}")
                        # always update last_prices for next iteration
                        last_prices[(seg.upper(), str(secid_str))] = lp
            else:
                messages.append(str(data)[:1000])

            # send unavailable messages first (so user sees missing LTP notices)
            for um in unavailable_msgs:
                send_telegram(um)
                log.info("Sent note: %s", um)

            if messages:
                header = f"<b>LTP Update • {now_ist_str()}</b>\n"
                text = header + "\n".join(messages)
                send_telegram(text)
                log.info("Sent LTP update with %d items.", len(messages))

            time.sleep(POLL_INTERVAL)
        except KeyboardInterrupt:
            log.info("Interrupted by user. Exiting.")
            break
        except Exception as e:
            log.exception("Error in main loop: %s", e)
            time.sleep(5)

if __name__ == "__main__":
    main()
