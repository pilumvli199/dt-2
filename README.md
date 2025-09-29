# DhanHQ -> Telegram LTP Alert Bot

This simple Python bot polls DhanHQ market LTP (last traded price) every N seconds
(default 60) and sends a Telegram message with the latest prices for configured symbols.

## Setup
1. Copy `config.example.env` to `.env` and fill in your `DHAN_API_TOKEN`, `TELEGRAM_BOT_TOKEN`, and `TELEGRAM_CHAT_ID`.
2. (Optional) set `DHAN_API_SECRET` if your Dhan account requires HMAC signing. Set `DHAN_AUTH_METHOD=basic` if you prefer HTTP Basic auth (token:secret).
3. Install requirements:
   ```bash
   pip install requests python-dotenv
   ```
4. Run:
   ```bash
   python3 bot.py
   ```

## Notes
- The bot attempts to resolve trading tokens using the instruments CSV hosted by Dhan. If your symbol isn't resolved, use exact trading symbol or security id in `SYMBOLS` env var.
- The bot sends a single Telegram message each poll with all symbol LTPS joined by ` | `.
- For production use consider adding logging, retries/backoff, rate-limit handling, and running in a process manager (systemd, pm2, docker, etc.).
