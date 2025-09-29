import os
import time
import logging
from datetime import datetime
from dhanhq import marketfeed
from telegram import Bot

# ======================================
# Setup Logging
# ======================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[logging.StreamHandler()],
)
log = logging.getLogger(__name__)

# ======================================
# ENV Vars (edit .env file)
# ======================================
CLIENT_ID = os.getenv("DHAN_CLIENT_ID")
ACCESS_TOKEN = os.getenv("DHAN_TOKEN")
TELEGRAM_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

# ======================================
# Symbols (Static Map for now)
# ======================================
SYMBOLS = {
    "NIFTY 50": ("NSE_INDEX", "13"),
    "NIFTY BANK": ("NSE_INDEX", "25"),
    "SENSEX": ("BSE_INDEX", "51"),
    "TATAMOTORS": ("NSE_EQ", "3456"),
    "RELIANCE": ("NSE_EQ", "2885"),
    "TCS": ("NSE_EQ", "11536"),
}

# ======================================
# Telegram Bot Init
# ======================================
tg_bot = Bot(token=TELEGRAM_TOKEN)

# Hold latest LTP data
latest_data = {name: None for name in SYMBOLS.keys()}


# ======================================
# Callback for WebSocket ticks
# ======================================
def on_tick(tick):
    """
    Tick Example:
    {
      'ExchangeSegment': 'NSE_EQ',
      'SecurityId': '2885',
      'LTP': 1375.55,
      'Change': 0.25,
      'PercentChange': 0.02
    }
    """
    try:
        seg = tick.get("ExchangeSegment")
        sid = str(tick.get("SecurityId"))
        ltp = tick.get("LTP")
        chg = tick.get("Change")
        pct = tick.get("PercentChange")

        # Map back to symbol name
        for name, (s, i) in SYMBOLS.items():
            if s == seg and i == sid:
                latest_data[name] = (ltp, chg, pct, seg)
                break
    except Exception as e:
        log.error(f"Tick processing error: {e}")


# ======================================
# Telegram updater (every 1 min)
# ======================================
def send_update():
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S IST")
    msg_lines = [f"LTP Update • {now}"]

    for name, val in latest_data.items():
        if val is None:
            msg_lines.append(f"{name}: (No Data)")
        else:
            ltp, chg, pct, seg = val
            msg_lines.append(f"{name} ({seg}): {ltp} ({chg:+.2f}, {pct:+.2f}%)")

    msg = "\n".join(msg_lines)
    tg_bot.send_message(chat_id=TELEGRAM_CHAT_ID, text=msg)
    log.info("Telegram update sent.")


# ======================================
# Main
# ======================================
def main():
    log.info("Starting WebSocket LTP Bot...")

    # Prepare instruments for subscription
    instruments = list(SYMBOLS.values())

    # Setup feed
    feed = marketfeed.DhanFeed(
        client_id=CLIENT_ID,
        access_token=ACCESS_TOKEN,
        instruments=instruments,
        subscription_code=marketfeed.Ticker,
    )
    feed.on_tick = on_tick
    feed.connect()

    # Run loop
    while True:
        send_update()
        time.sleep(60)


if __name__ == "__main__":
    main()
