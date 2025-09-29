# bot_auto_resolve.py
import os, time, requests, sys, logging
from dotenv import load_dotenv
from urllib.parse import urljoin
from datetime import datetime, timezone, timedelta

# Reference dicts
try:
    from dhanhq_security_ids import NIFTY50_STOCKS, INDICES_NSE, INDICES_BSE
except ImportError:
    print("⚠️ dhanhq_security_ids.py not found. Please keep it in the same folder.")
    sys.exit(1)

# --- Config ---
load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("dhan-telegram-bot")

DHAN_TOKEN = os.getenv("DHAN_TOKEN")
DHAN_CLIENT_ID = os.getenv("DHAN_CLIENT_ID")
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
POLL_INTERVAL = int(os.getenv("POLL_INTERVAL", "60"))
DHAN_API_BASE = os.getenv("DHAN_API_BASE", "https://api.dhan.co/v2")

SYMBOLS = [s.strip().upper() for s in os.getenv("SYMBOLS", "").split(",") if s.strip()]

if not DHAN_TOKEN or not DHAN_CLIENT_ID:
    log.error("Missing DHAN_TOKEN or DHAN_CLIENT_ID in .env")
    sys.exit(1)

# Aliases
ALIASES = {
    "BANKNIFTY": "NIFTY BANK",
    "NIFTYBANK": "NIFTY BANK",
    "CNX NIFTY": "NIFTY 50",
}

def now_ist_str():
    ist = datetime.now(timezone.utc) + timedelta(hours=5, minutes=30)
    return ist.strftime("%Y-%m-%d %H:%M:%S IST")

def send_telegram(text: str):
    if not (TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID):
        log.warning("Telegram creds missing, skipping message.")
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {"chat_id": TELEGRAM_CHAT_ID, "text": text, "parse_mode": "HTML"}
    try:
        r = requests.post(url, json=payload, timeout=10)
        r.raise_for_status()
    except Exception as e:
        log.error(f"Telegram send failed: {e}")

def resolve_from_reference(symbol: str):
    sym = symbol.upper()
    if sym in ALIASES:
        sym = ALIASES[sym]

    if sym in INDICES_NSE:
        return ("NSE_INDEX", INDICES_NSE[sym], sym)
    if sym in INDICES_BSE:
        return ("BSE_INDEX", INDICES_BSE[sym], sym)
    if sym in NIFTY50_STOCKS:
        return ("NSE_EQ", NIFTY50_STOCKS[sym], sym)
    return None

def call_ltp(payload):
    url = urljoin(DHAN_API_BASE + "/", "marketfeed/ltp")
    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "access-token": DHAN_TOKEN,
        "client-id": DHAN_CLIENT_ID,
    }
    try:
        r = requests.post(url, headers=headers, json=payload, timeout=10)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        log.error(f"LTP call failed: {e}")
        return {}

def call_ohlc(payload):
    url = urljoin(DHAN_API_BASE + "/", "marketfeed/ohlc")
    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "access-token": DHAN_TOKEN,
        "client-id": DHAN_CLIENT_ID,
    }
    try:
        r = requests.post(url, headers=headers, json=payload, timeout=10)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        log.error(f"OHLC call failed: {e}")
        return {}

def main():
    if not SYMBOLS:
        log.error("No SYMBOLS in .env file")
        sys.exit(1)

    payload, display_map = {}, {}
    resolved = []  # (seg, sid, name)

    for s in SYMBOLS:
        ref = resolve_from_reference(s)
        if ref:
            seg, sid, name = ref
            payload.setdefault(seg, []).append(int(sid))
            display_map[(seg, str(sid))] = name
            resolved.append((seg, str(sid), name))
            log.info(f"Resolved {s} -> {seg}:{sid}")
        else:
            log.warning(f"{s} not in reference dicts. Skipping.")

    if not payload:
        log.error("No valid symbols resolved. Exiting.")
        sys.exit(1)

    log.info(f"Final Payload: {payload}")
    last_prices = {}

    while True:
        try:
            data = call_ltp(payload).get("data", {})

            # 🔥 Collect all missing in one payload
            missing_payload = {}
            for seg, sid, name in resolved:
                info = data.get(seg, {}).get(sid, {})
                if not info or "last_price" not in info:
                    missing_payload.setdefault(seg, []).append(int(sid))

            if missing_payload:
                ohlc_data = call_ohlc(missing_payload).get("data", {})
                for seg, sids in missing_payload.items():
                    for sid in map(str, sids):
                        if seg in ohlc_data and sid in ohlc_data[seg]:
                            lp = (
                                ohlc_data[seg][sid].get("last_price")
                                or ohlc_data[seg][sid].get("close")
                            )
                            if lp:
                                if seg not in data:
                                    data[seg] = {}
                                data[seg][sid] = {"last_price": lp}

            msgs = []
            for seg, sid, name in resolved:
                info = data.get(seg, {}).get(sid, {})
                lp = info.get("last_price")
                display = display_map.get((seg, sid), f"{seg}:{sid}")
                prev = last_prices.get((seg, sid))
                change = ""

                if lp is not None:
                    if prev is not None:
                        diff = float(lp) - float(prev)
                        pct = (diff / float(prev)) * 100 if float(prev) != 0 else 0
                        change = f" ({diff:+.2f}, {pct:+.2f}%)"
                    msgs.append(f"<b>{display} ({seg})</b>: {lp}{change}")
                    last_prices[(seg, sid)] = lp
                else:
                    msgs.append(f"<b>{display} ({seg})</b>: (No Data)")

            if msgs:
                text = f"<b>LTP Update • {now_ist_str()}</b>\n" + "\n".join(msgs)
                send_telegram(text)
                log.info("Sent update with %d items", len(msgs))
            time.sleep(POLL_INTERVAL)
        except KeyboardInterrupt:
            log.info("Stopped by user")
            break
        except Exception as e:
            log.error(f"Loop error: {e}")
            time.sleep(5)

if __name__ == "__main__":
    main()
