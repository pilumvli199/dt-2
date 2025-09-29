# dhanhq_security_ids.py
# DhanHQ Security IDs Reference (Updated Sep 2025)

# ============================================
# MAJOR INDICES - NSE
# ============================================

INDICES_NSE = {
    "NIFTY 50": "13",
    "NIFTY BANK": "25",
    "BANKNIFTY": "25",   # alias
    "NIFTY IT": "369",
    "NIFTY PHARMA": "1045",
    "NIFTY AUTO": "1108",
    "NIFTY METAL": "1314",
    "NIFTY REALTY": "1463",
    "NIFTY FMCG": "364",
    "NIFTY MEDIA": "1311",
    "NIFTY PSU BANK": "1453",
    "NIFTY PVT BANK": "1452",
    "NIFTY FIN SERVICE": "422",
    "NIFTY NEXT 50": "423",
    "NIFTY 100": "288",
    "NIFTY 200": "398",
    "NIFTY 500": "412",
    "NIFTY MIDCAP 50": "453",
    "NIFTY MIDCAP 100": "454",
    "NIFTY MIDCAP 150": "455",
    "NIFTY SMALLCAP 50": "456",
    "NIFTY SMALLCAP 100": "457",
    "NIFTY SMALLCAP 250": "458",
    "NIFTY ENERGY": "362",
    "NIFTY INFRA": "1031",
    "NIFTY COMMODITIES": "347",
    "NIFTY CONSUMPTION": "348",
    "NIFTY CPSE": "349",
    "NIFTY PSE": "1451",
    "NIFTY SERV SECTOR": "1467",
    "NIFTY OIL & GAS": "1361",
    "NIFTY HEALTHCARE": "1021",
    "NIFTY INDIA DIGITAL": "1092",
    "NIFTY INDIA DEFENCE": "1093",
    "NIFTY INDIA MANUFACTURING": "1094",
    "NIFTY MOBILITY": "1095",
}

# ============================================
# MAJOR INDICES - BSE
# ============================================

INDICES_BSE = {
    "SENSEX": "51",
    "BSE 100": "304",
    "BSE 200": "305",
    "BSE 500": "306",
    "BSE MIDCAP": "308",
    "BSE SMALLCAP": "309",
    "BSE BANKEX": "310",
    "BSE TECK": "311",
    "BSE AUTO": "312",
    "BSE METAL": "313",
    "BSE OIL & GAS": "314",
    "BSE REALTY": "315",
    "BSE POWER": "316",
    "BSE CONSUMER DURABLES": "317",
}

# ============================================
# NIFTY 50 STOCKS
# ============================================

NIFTY50_STOCKS = {
    # Banking
    "HDFCBANK": "1333",
    "ICICIBANK": "4963",
    "KOTAKBANK": "1922",
    "AXISBANK": "5900",
    "SBIN": "3045",
    "INDUSINDBK": "5258",
    "BAJFINANCE": "317",
    "BAJAJFINSV": "16675",

    # IT
    "TCS": "11536",
    "INFY": "1594",
    "WIPRO": "3787",
    "HCLTECH": "7229",
    "TECHM": "13538",

    # Auto
    "MARUTI": "10999",
    "M&M": "2031",
    "TATAMOTORS": "3456",
    "BAJAJ-AUTO": "16669",
    "EICHERMOT": "910",
    "HEROMOTOCO": "1348",

    # FMCG
    "HINDUNILVR": "1394",
    "ITC": "1660",
    "NESTLEIND": "17963",
    "BRITANNIA": "547",
    "DABUR": "2732",

    # Pharma
    "SUNPHARMA": "3351",
    "DRREDDY": "881",
    "CIPLA": "694",
    "DIVISLAB": "10940",
    "APOLLOHOSP": "157",

    # Energy & Power
    "RELIANCE": "2885",
    "ONGC": "2475",
    "NTPC": "11630",
    "POWERGRID": "14977",
    "COALINDIA": "20374",
    "BPCL": "526",

    # Metals
    "TATASTEEL": "3499",
    "HINDALCO": "1363",
    "JSWSTEEL": "11723",
    "VEDL": "3063",

    # Others
    "BHARTIARTL": "3666",
    "TITAN": "3506",
    "ASIANPAINT": "212",
    "ULTRACEMCO": "11532",
    "GRASIM": "1232",
    "SHREECEM": "3076",
    "LT": "11483",
    "ADANIENT": "25",
    "ADANIPORTS": "15083",
}

# ============================================
# MIDCAP POPULAR STOCKS
# ============================================

MIDCAP_STOCKS = {
    "TATAPOWER": "3426",
    "GODREJCP": "10099",
    "MARICO": "4067",
    "MUTHOOTFIN": "23650",
    "INDIGO": "11195",
    "PIDILITIND": "2664",
    "BANDHANBNK": "579",
    "LTTS": "11908",
    "POLYCAB": "9590",
    "ABCAPITAL": "5",
    "PNB": "10666",
    "CANBK": "10794",
    "BANKBARODA": "4668",
    "IDFCFIRSTB": "11184",
}

# ============================================
# Exchange Segments
# ============================================

EXCHANGE_SEGMENTS = {
    "NSE_EQ": "NSE Equity",
    "NSE_FNO": "NSE Derivatives (F&O)",
    "BSE_EQ": "BSE Equity",
    "BSE_FNO": "BSE Derivatives",
    "MCX_COMM": "MCX Commodities",
    "NSE_CURRENCY": "NSE Currency",
    "NSE_INDEX": "NSE Indices",
    "BSE_INDEX": "BSE Indices",
}

# ============================================
# Helper Functions
# ============================================

def get_security_id(symbol: str, stock_type: str = "nifty50") -> str:
    """
    Get security ID for a given symbol
    """
    stock_lists = {
        "nifty50": NIFTY50_STOCKS,
        "midcap": MIDCAP_STOCKS,
        "indices_nse": INDICES_NSE,
        "indices_bse": INDICES_BSE,
    }
    stock_dict = stock_lists.get(stock_type.lower(), NIFTY50_STOCKS)
    return stock_dict.get(symbol.upper(), None)


if __name__ == "__main__":
    print("Sample lookups:")
    print("RELIANCE ->", get_security_id("RELIANCE", "nifty50"))
    print("TCS ->", get_security_id("TCS", "nifty50"))
    print("NIFTY 50 ->", get_security_id("NIFTY 50", "indices_nse"))
    print("BANKNIFTY ->", get_security_id("BANKNIFTY", "indices_nse"))
