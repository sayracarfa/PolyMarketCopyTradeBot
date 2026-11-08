"""
Alchemy (Polygon RPC) baglantisini test eder. Anahtari ekrana yazmaz.

Kullanim (bot klasorunun icinde, sanal ortam etkinken):
    python check_rpc.py      (Windows)
    python3 check_rpc.py     (Mac)
"""

import os
import json
import urllib.request
import urllib.error
from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env"))

url = os.getenv("WSS_RPC_URL") or ""
key = url.split("/v2/")[-1] if "/v2/" in url else ""

print("WSS_RPC_URL tanimli mi :", "evet" if url else "HAYIR")
print("Anahtar uzunlugu       :", len(key))
bad = [c for c in key if c in " \"'<>\r\n\t"]
if bad or key in ("", "ANAHTAR"):
    print("UYARI: Anahtar eksik, sablondaki gibi birakilmis ya da tirnak/bosluk iceriyor.")

if not url:
    raise SystemExit(".env dosyasinda WSS_RPC_URL yok.")

req = urllib.request.Request(
    url.replace("wss://", "https://", 1),
    data=json.dumps({"jsonrpc": "2.0", "id": 1, "method": "eth_blockNumber", "params": []}).encode(),
    headers={"Content-Type": "application/json"},
)
try:
    body = json.loads(urllib.request.urlopen(req, timeout=15).read().decode())
    if "result" in body:
        print(f"BAGLANTI TAMAM - guncel blok: {int(body['result'], 16)}")
    else:
        print("Beklenmeyen cevap:", str(body)[:300])
except urllib.error.HTTPError as e:
    print("Alchemy hatasi:", e.code, e.read().decode()[:300])
except Exception as e:
    print("Baglanti kurulamadi:", e)
