# bot_auto_resolve.py
"""
DhanHQ -> Telegram LTP Bot (robust v2)
- Auto-resolve symbols via scrip master (SEM_* columns support)
- Segment normalization (SEG_MAP) to API-expected keys
- Retry/backoff for LTP calls
- Telegram sending guarded (won’t crash if missing creds)
- AUTO_PICK option to guess unresolved symbols
- SECURITY_IDS env bypass resolution
"""

import os, time, csv, io, sys, logging, difflib, requests
from urllib.parse import urljoin
from dotenv import load_dotenv

# --- Config ---
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

if not (DHAN_TOKEN and DHAN_CLIENT_ID):
    log.error("Missing DHAN_TOKEN or DHAN_CLIENT_ID in .env")
    sys.exit(1)
if not (TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID):
    log.warning("Telegram creds missing → will not send messages.")

SYMBOLS = [s.strip() for s in SYMBOLS_ENV.split(",") if s.strip()]

# --- Segment normalization ---
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
        except: log.warning("Invalid secid part: %s",p)
    return payload

SECURITY_PAYLOAD=parse_security_ids(SEC_IDS_ENV)

# --- Download scrip master ---
def download_scrip_master():
    log.info("Downloading scrip master...")
    r=requests.get(SCRIP_MASTER_URL,timeout=30); r.raise_for_status()
    rows=list(csv.DictReader(io.StringIO(r.content.decode("utf-8","replace"))))
    log.info("Downloaded %d rows",len(rows))
    return rows

def detect_columns(fns):
    def find_any(cands):
        for fn in fns:
            if fn.lower() in [c.lower() for c in cands]: return fn
        for fn in fns:
            for c in cands:
                if c.lower() in fn.lower(): return fn
        return None
    return (
        find_any(["sem_trading_symbol","tradingsymbol","sm_symbol_name"]),
        find_any(["sem_instrument_name","instrumentname","name"]),
        find_any(["sem_smst_security_id","securityid","id"]),
        find_any(["sem_segment","sem_exm_exch_id","segment"])
    )

def build_index(rows,trad_col,name_col,id_col,seg_col):
    idx=[]
    for r in rows:
        trad=(r.get(trad_col) or "").strip().upper()
        name=(r.get(name_col) or "").strip().upper()
        sid=(r.get(id_col) or "").strip()
        seg=(r.get(seg_col) or "NSE_EQ").strip()
        tokens=set((trad+" "+name).replace("/"," ").split())
        idx.append({"trad":trad,"name":name,"sid":sid,"seg":seg,"tokens":tokens})
    return idx

def resolve_symbol_single(q,idx):
    q=q.upper()
    for it in idx:
        if it["trad"]==q or it["name"]==q: return it
    q_tokens=[t for t in q.split() if t]
    if q_tokens:
        cand=[(sum(t in it["tokens"] for t in q_tokens),len(it["tokens"]),it) for it in idx if it["sid"]]
        cand=[c for c in cand if c[0]>0]
        if cand: return sorted(cand,key=lambda x:(-x[0],x[1]))[0][2]
    names=[(it["trad"]+" "+it["name"]).strip() for it in idx if it["sid"]]
    close=difflib.get_close_matches(q,names,n=1,cutoff=0.6)
    if close:
        for it in idx:
            if (it["trad"]+" "+it["name"]).strip()==close[0]: return it
    return None

def resolve_symbols(symbols):
    rows=download_scrip_master()
    trad_col,name_col,id_col,seg_col=detect_columns(rows[0].keys())
    idx=build_index(rows,trad_col,name_col,id_col,seg_col)
    resolved=[]
    for s in symbols:
        r=resolve_symbol_single(s,idx)
        if r:
            log.info("Resolved %s -> sid=%s seg=%s",s,r["sid"],r["seg"])
            resolved.append({"symbol":s,"secid":r["sid"],"segment":r["seg"]})
        else: log.warning("Unresolved: %s",s)
    return resolved

def build_payload(resolved):
    payload={}
    for it in resolved:
        try: sid_i=int(it["secid"])
        except: continue
        seg=SEG_MAP.get(it["segment"].upper(),it["segment"].upper())
        payload.setdefault(seg,[]).append(sid_i)
    return payload

# --- Call LTP ---
def call_ltp(payload):
    url=urljoin(DHAN_API_BASE+"/","marketfeed/ltp")
    headers={"access-token":DHAN_TOKEN,"client-id":DHAN_CLIENT_ID,"Content-Type":"application/json"}
    for i in range(3):
        try:
            r=requests.post(url,headers=headers,json=payload,timeout=10)
            r.raise_for_status(); return r.json()
        except Exception as e:
            log.warning("LTP call fail try %d: %s",i+1,e); time.sleep(2)
    return {}

# --- Telegram send ---
def send_telegram(text):
    if not (TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID): return
    try:
        url=f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
        requests.post(url,json={"chat_id":TELEGRAM_CHAT_ID,"text":text,"parse_mode":"HTML"},timeout=8)
    except Exception as e: log.warning("Telegram send fail: %s",e)

# --- Main ---
def main():
    if SECURITY_PAYLOAD: payload,resolved=SECURITY_PAYLOAD,None
    else:
        if not SYMBOLS: sys.exit("No SYMBOLS or SECURITY_IDS set")
        resolved=resolve_symbols(SYMBOLS)
        payload=build_payload(resolved)
    log.info("Payload groups: %s",payload)

    last_prices={}
    while True:
        resp=call_ltp(payload)
        data=resp.get("data") if isinstance(resp,dict) else resp
        msgs=[]
        if isinstance(data,dict):
            for seg,m in data.items():
                if not isinstance(m,dict): continue
                for sid,info in m.items():
                    lp=info.get("last_price") or info.get("ltp")
                    if lp is None: continue
                    prev=last_prices.get(sid)
                    change=""
                    if prev is not None:
                        try:
                            diff=float(lp)-float(prev); pct=diff/float(prev)*100
                            change=f" ({diff:+.2f}, {pct:+.2f}%)"
                        except: pass
                    msgs.append(f"<b>{seg}:{sid}</b>: {lp}{change}")
                    last_prices[sid]=lp
        if msgs:
            text="<b>LTP Update</b>\n"+"\n".join(msgs)
            send_telegram(text); log.info("Sent update")
        time.sleep(POLL_INTERVAL)

if __name__=="__main__": main()
