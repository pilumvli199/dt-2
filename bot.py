#!/usr/bin/env python3
\"\"\"Simple DhanHQ -> Telegram LTP alert bot for Indian market.
Polls every 60 seconds (configurable) and sends last price for configured symbols.

Usage:
 - copy config.example.env -> .env and fill values, or set env vars directly
 - python3 bot.py
\"\"\"

import os
import time
import json
import csv
import io
import requests
import hmac
import hashlib
from typing import List, Dict, Optional

# --- Configuration (from env) ---
SYMBOLS = [s.strip() for s in os.getenv('SYMBOLS', 'NIFTY,BANKNIFTY,SENSEX,RELIANCE,TCS,TATAMOTORS').split(',') if s.strip()]
POLL_INTERVAL = int(os.getenv('POLL_INTERVAL_SECONDS', '60'))
INSTRUMENTS_CSV_URL = os.getenv('INSTRUMENTS_CSV_URL', 'https://images.dhan.co/api-data/api-scrip-master.csv')
MARKET_LTP_URL = os.getenv('MARKET_LTP_URL', 'https://api.dhan.co/marketfeed/ltp')

DHAN_API_TOKEN = os.getenv('DHAN_API_TOKEN')
DHAN_API_SECRET = os.getenv('DHAN_API_SECRET')
DHAN_AUTH_METHOD = os.getenv('DHAN_AUTH_METHOD', 'bearer').lower()  # 'bearer' or 'basic'
DHAN_ADD_SECRET_HEADER = os.getenv('DHAN_ADD_SECRET_HEADER', '0') == '1'
DHAN_CLIENT_ID = os.getenv('DHAN_CLIENT_ID', '').strip()

TELEGRAM_BOT_TOKEN = os.getenv('TELEGRAM_BOT_TOKEN')
TELEGRAM_CHAT_ID = os.getenv('TELEGRAM_CHAT_ID')

# --- Helpers ---
def compute_hmac_signature(secret: str, payload_bytes: bytes) -> str:
    mac = hmac.new(secret.encode('utf-8'), payload_bytes, hashlib.sha256)
    return mac.hexdigest()

def send_telegram_message(bot_token: str, chat_id: str, text: str) -> bool:
    url = f'https://api.telegram.org/bot{bot_token}/sendMessage'
    try:
        r = requests.post(url, json={'chat_id': chat_id, 'text': text}, timeout=10)
        return r.ok
    except Exception as e:
        print('Telegram send error:', e)
        return False

def fetch_instruments_csv(csv_url: str) -> List[Dict[str,str]]:
    r = requests.get(csv_url, timeout=20)
    r.raise_for_status()
    text = r.content.decode('utf-8', errors='ignore')
    reader = csv.DictReader(io.StringIO(text))
    return [row for row in reader]

def find_tokens_for_symbols(rows: List[Dict[str,str]], symbols: List[str]) -> Dict[str, Optional[str]]:
    cols = list(rows[0].keys()) if rows else []
    # heuristics to find token and symbol columns
    token_cols = [c for c in cols if c.lower() in ('token','instrument_token','securityid','security_id','id','instrumenttoken')]
    symbol_cols = [c for c in cols if 'symbol' in c.lower() or 'tradingsymbol' in c.lower() or 'name' in c.lower()]
    if not token_cols:
        # pick numeric-like column
        for c in cols:
            sample = rows[0].get(c,'')
            if sample.isdigit():
                token_cols.append(c)
                break
    if not symbol_cols:
        symbol_cols = cols[:2]

    resolved = {}
    for sym in symbols:
        sym_up = sym.replace(' ','').upper()
        found = None
        for r in rows:
            for sc in symbol_cols:
                val = (r.get(sc,'') or '').upper().replace(' ','')
                if val.startswith(sym_up) or val == sym_up or sym_up in val:
                    for tc in token_cols:
                        token = r.get(tc) or r.get(tc.lower()) or r.get(tc.upper())
                        if token:
                            found = token
                            break
                    if found:
                        break
            if found:
                break
        resolved[sym] = found
    return resolved

def fetch_ltp_for_tokens(tokens: List[str]) -> Dict[str, Optional[float]]:
    if not tokens:
        return {}
    payload = {'instruments': tokens}
    params = {}
    if DHAN_CLIENT_ID:
        params['clientId'] = DHAN_CLIENT_ID

    headers = {'Content-Type': 'application/json'}
    auth = None
    if DHAN_API_TOKEN and DHAN_API_SECRET and DHAN_AUTH_METHOD == 'basic':
        auth = (DHAN_API_TOKEN, DHAN_API_SECRET)
    elif DHAN_API_TOKEN:
        headers['Authorization'] = f'Bearer {DHAN_API_TOKEN}'
        if DHAN_ADD_SECRET_HEADER and DHAN_API_SECRET:
            headers['X-DHAN-API-SECRET'] = DHAN_API_SECRET

    body_bytes = json.dumps(payload, separators=(',', ':'), ensure_ascii=False).encode('utf-8')
    if DHAN_API_SECRET:
        headers['X-DHAN-SIGNATURE'] = compute_hmac_signature(DHAN_API_SECRET, body_bytes)

    r = requests.post(MARKET_LTP_URL, headers=headers, params=params, json=payload, timeout=10, auth=auth)
    r.raise_for_status()
    data = r.json()

    result = {}
    if isinstance(data, dict):
        if 'data' in data and isinstance(data['data'], list):
            for item in data['data']:
                token = item.get('token') or item.get('instrument') or item.get('tradingsymbol') or item.get('instrument_token')
                ltp = item.get('ltp') or item.get('last_price') or item.get('lastTradedPrice')
                if token is not None and ltp is not None:
                    result[str(token)] = float(ltp)
        else:
            for k,v in data.items():
                try:
                    ltp = v.get('ltp') if isinstance(v, dict) else v
                    result[str(k)] = float(ltp) if ltp is not None else None
                except Exception:
                    continue
    elif isinstance(data, list):
        for item in data:
            token = item.get('token') or item.get('instrument')
            ltp = item.get('ltp') or item.get('last_price')
            if token and ltp is not None:
                result[str(token)] = float(ltp)
    return result

def main():
    if not (DHAN_API_TOKEN and TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID):
        print('ERROR: Set DHAN_API_TOKEN, TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID environment variables.')
        return

    print('Downloading instruments CSV ...')
    try:
        rows = fetch_instruments_csv(INSTRUMENTS_CSV_URL)
    except Exception as e:
        print('Failed to download instruments CSV:', e)
        return

    resolved = find_tokens_for_symbols(rows, SYMBOLS)
    print('Resolved tokens:', resolved)

    token_to_symbol = {}
    for s,t in resolved.items():
        if t:
            token_to_symbol[str(t)] = s
        else:
            print(f'Warning: could not resolve symbol {s} in the instruments CSV. You may need to use exact trading symbol or security id.')

    if not token_to_symbol:
        print('No instruments resolved. Exiting.')
        return

    print(f'Starting polling every {POLL_INTERVAL} seconds. Press Ctrl+C to stop.')
    try:
        while True:
            tokens = list(token_to_symbol.keys())
            try:
                ltp_map = fetch_ltp_for_tokens(tokens)
            except Exception as e:
                print('Error fetching LTP:', e)
                time.sleep(POLL_INTERVAL)
                continue

            parts = []
            for token, sym in token_to_symbol.items():
                ltp = ltp_map.get(str(token))
                if ltp is None:
                    parts.append(f'{sym} ({token}): LTP unavailable')
                else:
                    parts.append(f'{sym}: {ltp}')
            msg = ' | '.join(parts)
            ok = send_telegram_message(TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID, msg)
            now = time.strftime('%Y-%m-%d %H:%M:%S')
            print(f'[{now}] Sent={ok} -> {msg}')
            time.sleep(POLL_INTERVAL)
    except KeyboardInterrupt:
        print('Stopped by user.')

if __name__ == '__main__':
    main()
