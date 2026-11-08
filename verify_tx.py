"""
Canliya almadan once decode mantigini dogrulamak icin yardimci script.

Kullanim:
    python3 verify_tx.py <tx_hash> [target_address]

Ne yapar:
    Verilen islem hash'inin receipt'ini ceker, icindeki tum OrderFilled
    loglarini bulur, onchain_listener.py ile AYNI decode fonksiyonunu
    kullanarak insan-okur formatta yazdirir. target_address verirsen
    o adres acisindan (maker/taker) BUY/SELL yonunu de gosterir.

    Ciktidaki usdc_amount / price degerlerini Polymarket'in kendi
    arayuzunde (Activity sekmesi) ya da data-api'nin usdcSize/price
    alanlariyla KARSILASTIR. Eslesmiyorsa onchain_listener.py'daki decode
    mantigini (ozellikle V2 kontratlari icin) canliya almadan duzelt.

Ornek:
    python3 verify_tx.py 0xabc123... Hedef_Cüzdan(Ox....)
"""

import os
import sys
import json
import requests
from dotenv import load_dotenv

from onchain_listener import (
    aggregate_tx_fills,
    decode_order_filled,
    EXCHANGE_ADDR_TO_NAME,
    ORDER_FILLED_TOPICS,
)

load_dotenv()

# Herkese acik ucretsiz RPC'ler zaman zaman kimlik dogrulama istemeye
# basliyor / rate-limit uyguluyor (bkz. polygon-rpc.com 401 hatasi). Bu
# yuzden onceligi zaten .env'de tanimli olan WSS_RPC_URL'den turetilen
# HTTP adresine veriyoruz (ayni saglayici, ayni anahtar, sadece sema
# degisiyor). Istersen .env'e ayrica HTTP_RPC_URL de tanimlayabilirsin.

def _resolve_http_rpc() -> str:
    explicit = os.getenv("HTTP_RPC_URL")
    if explicit:
        return explicit
    wss = os.getenv("WSS_RPC_URL")
    if wss:
        return wss.replace("wss://", "https://", 1)
    # Son care - genelde calisir ama garanti degil.
    return "https://polygon-rpc.com"


def get_receipt(tx_hash: str, rpc_url: str = None) -> dict:
    rpc_url = rpc_url or _resolve_http_rpc()
    resp = requests.post(
        rpc_url,
        json={
            "jsonrpc": "2.0", "id": 1,
            "method": "eth_getTransactionReceipt",
            "params": [tx_hash],
        },
        timeout=15,
    )
    resp.raise_for_status()
    body = resp.json()
    if "error" in body:
        raise RuntimeError(f"RPC hatasi: {body['error']}")
    if not body.get("result"):
        raise RuntimeError("Islem bulunamadi (yanlis hash ya da henuz mine edilmemis).")
    return body["result"]


def main():
    if len(sys.argv) < 2:
        print(__doc__)
        sys.exit(1)

    tx_hash = sys.argv[1]
    target = sys.argv[2].lower() if len(sys.argv) > 2 else None

    rpc_url = _resolve_http_rpc()
    masked = rpc_url.split("/v2/")[0] + "/v2/***" if "/v2/" in rpc_url else rpc_url
    print(f"RPC: {masked}\n")

    try:
        receipt = get_receipt(tx_hash, rpc_url)
    except requests.exceptions.HTTPError as e:
        print(f"RPC istegi basarisiz oldu: {e}")
        print(
            "Kullanilan RPC adresi yukarida yazan. Eger kendi Alchemy/Infura "
            "anahtarin degil de genel bir RPC gorunuyorsa, .env icine "
            "WSS_RPC_URL (ya da ayrica HTTP_RPC_URL) ekleyip tekrar dene."
        )
        sys.exit(1)
    known_topics_lower = {t.lower() for t in ORDER_FILLED_TOPICS}
    order_filled_logs = [
        l for l in receipt["logs"]
        if l["topics"] and l["topics"][0].lower() in known_topics_lower
    ]

    if not order_filled_logs:
        print(f"Bilinen topic0'lardan hicbiriyle eslesen OrderFilled event'i bulunamadi ({ORDER_FILLED_TOPICS}).")
        print(f"Islemin gonderildigi kontrat(lar): {[l['address'] for l in receipt['logs']]}\n")

        known_exchange_hits = [
            l for l in receipt["logs"] if l["address"].lower() in EXCHANGE_ADDR_TO_NAME
        ]
        if known_exchange_hits:
            print(
                "AMA bilinen exchange kontratlarindan biri bu tx'de var - demek ki "
                "event VAR  topic0/imza yanlis. Ham log(lar):\n"
            )
            for l in known_exchange_hits:
                name = EXCHANGE_ADDR_TO_NAME.get(l["address"].lower())
                print(f"--- kontrat: {name} ({l['address']}) ---")
                print(f"  topic0 (gercek): {l['topics'][0] if l['topics'] else '(topic yok)'}")
                print(f"  topics          : {l['topics']}")
                print(f"  data            : {l['data']}")
                data_len = len(l['data'][2:]) // 64 if l['data'].startswith('0x') else 0
                print(f"  data alan sayisi (32-byte word): {data_len}")
                print()
          
        return

    print(f"{len(order_filled_logs)} adet OrderFilled event'i bulundu.\n")

    decoded_for_target = []
    for i, l in enumerate(order_filled_logs, 1):
        contract = l["address"].lower()
        exchange = EXCHANGE_ADDR_TO_NAME.get(contract, f"BILINMEYEN({contract})")
        print(f"--- Fill #{i} | kontrat: {exchange} ---")

        maker_topic = l["topics"][2]
        taker_topic = l["topics"][3]
        maker_addr = "0x" + maker_topic[-40:]
        taker_addr = "0x" + taker_topic[-40:]
        print(f"  maker : {maker_addr}")
        print(f"  taker : {taker_addr}")

        # target verilmemisse maker'i hedef kabul edip decode et (gorsel kontrol icin yeterli)
        probe_target = target or maker_addr
        trade = decode_order_filled(l, probe_target)
        if trade is None:
            print("  (decode edilemedi ya da target bu fill'de maker/taker degil)")
        else:
            if target:
                decoded_for_target.append(trade)
            rol = "kendi emri (maker alani)" if trade["role"] == "maker" else "karsi taraf (taker alani)"
            print(f"  hedefin rolu: {rol}")
            print(f"  yon (target={probe_target[:10]}...): {trade['side']}")
            print(f"  usdc_amount : {trade['usdc_amount']}")
            print(f"  token_amount: {trade['token_amount']}")
            print(f"  price       : {trade['price']}")
            print(f"  token_id    : {trade['token_id']}")
        print()

    if target:
        own = [t for t in decoded_for_target if t["role"] == "maker"]
        other = [t for t in decoded_for_target if t["role"] == "taker"]
        print("=== BOTUN KOPYALAYACAGI (tx bazinda birlestirilmis) ===")
        for t in aggregate_tx_fills(decoded_for_target):
            print(
                f"  {t['side']} | ${t['usdc_amount']:.2f} | {t['token_amount']:.2f} hisse @ {t['price']} | "
                f"{t['fill_count']} fill birlesti, {t['ignored_fills']} mukerrer atildi | token={t['token_id'][:16]}..."
            )
        print()
        print("=== CIFT SAYMA KONTROLU ===")
        if own and other:
            own_usdc = sum(t["usdc_amount"] for t in own)
            other_usdc = sum(t["usdc_amount"] for t in other)
            print(
                f"  Bu islemde hedef hem kendi emriyle ({len(own)} fill, ${own_usdc:.2f}) hem karsi taraf\n"
                f"  olarak ({len(other)} fill, ${other_usdc:.2f}) gorunuyor"
            )
        elif own:
            print("  Hedef sadece kendi emriyle gorunuyor - bu islemde cift sayma riski yok.")
        elif other:
            print(
                "  Hedef SADECE karsi taraf (taker alani) olarak gorunuyor, kendi emrine ait ozet\n"
                "  event yok. Bot bu durumda taker fill'lerini sayar. Birlesik tutar/yon arayuzle\n"
                
            )
        else:
            print("  Bu islemde hedef adres hic gorunmuyor.")
        print()

    print(
        "Bu degerleri Polymarket arayuzundeki (Activity) ya da data-api'nin\n"
        "usdcSize / price alanlariyla karsilastir. Eslesiyorsa decode dogru demektir."
    )


if __name__ == "__main__":
    main()