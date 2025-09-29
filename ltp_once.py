#!/usr/bin/env python3
# ltp_once.py - quick test for SECURITY_IDS

import os, sys, requests
from urllib.parse import urljoin
from dotenv import load_dotenv
load_dotenv()

DHAN_TOKEN=os.getenv("DHAN_TOKEN")
DHAN_CLIENT_ID=os.getenv("DHAN_CLIENT_ID")
SEC_IDS=os.getenv("SECURITY_IDS")

if not (DHAN_TOKEN and DHAN_CLIENT_ID):
    sys.exit("Missing DHAN_TOKEN or DHAN_CLIENT_ID")

def parse_ids(raw):
    payload={}
    for p in raw.split(","):
        if ":" in p: seg,sid=p.split(":",1)
        else: seg,sid="NSE_EQ",p
        try:
            sid_i=int(sid); payload.setdefault(seg.strip().upper(),[]).append(sid_i)
        except: pass
    return payload

def call_ltp(payload):
    url=urljoin("https://api.dhan.co/v2/","marketfeed/ltp")
    h={"access-token":DHAN_TOKEN,"client-id":DHAN_CLIENT_ID,"Content-Type":"application/json"}
    r=requests.post(url,headers=h,json=payload,timeout=10); r.raise_for_status()
    return r.json()

def main():
    raw=sys.argv[1] if len(sys.argv)>1 else SEC_IDS
    if not raw: sys.exit("Provide SECURITY_IDS as env or arg")
    payload=parse_ids(raw)
    print("Payload:",payload)
    resp=call_ltp(payload)
    data=resp.get("data") or resp
    for seg,m in data.items():
        for sid,info in m.items():
            print(seg,sid,"->",info.get("last_price") or info.get("ltp"))

if __name__=="__main__": main()
