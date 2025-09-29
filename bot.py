# bot.py
import logging
from dotenv import load_dotenv
import bot_auto_resolve

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("bot.py")

if __name__ == "__main__":
    load_dotenv()
    log.info("Starting DhanHQ LTP Bot...")
    try:
        bot_auto_resolve.main()
    except KeyboardInterrupt:
        log.info("Bot stopped by user.")
    except Exception as e:
        log.error(f"Fatal error: {e}")
