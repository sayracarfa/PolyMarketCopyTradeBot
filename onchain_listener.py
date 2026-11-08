"""
Polymarket copy-trade botu icin ZINCIR-UZERI (on-chain) gercek-zamanli dinleyici.


Polygon zincirindeki `OrderFilled` event'ini WebSocket uzerinden DOGRUDAN dinler; hedef adresin
islemi zincire yazildigi an (Polygon'da ortalama ~2 sn blok suresi) haberin
olur, Polymarket'in kendi API gecikmesi araya girmez.

 Ilk etapta gercek para yerine sadece log/dry-run modunda, mevcut poll'lu
   botunla PARALEL calistirip bir sure ayni islemleri yakalayip
   yakalamadigini karsilastirmani oneririm.

Kurulum:
    pip install websockets web3 python-dotenv --break-system-packages

.env icine eklenecekler:
    WSS_RPC_URL=wss://polygon-mainnet.g.alchemy.com/v2/<API_KEY>
    # Alchemy/Infura/Chainstack/QuickNode gibi bir saglayicidan ucretsiz
    # Polygon mainnet WebSocket RPC anahtari alabilirsin.
"""

import os
import json
import time
import asyncio
import logging
from dotenv import load_dotenv
from web3 import Web3

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger("onchain-listener")

WSS_RPC_URL = os.getenv("WSS_RPC_URL")

# Polymarket exchange kontratlari (Polygon mainnet). Bkz. yukaridaki uyari.
EXCHANGES = {
    "ctf_v1":     "0x4bFb41d5B3570DeFd03C39a9A4D8dE6Bd8B8982E",
    "negrisk_v1": "0xC5d563A36AE78145C45a50134d48A1215220f80a",
    "ctf_v2":     "0xE111180000d2663C0091e4f400237545B87B996B",
    "negrisk_v2": "0xe2222d279d744050d28e00520010520000310F59",
}
EXCHANGE_ADDR_TO_NAME = {addr.lower(): name for name, addr in EXCHANGES.items()}
EXCHANGE_ADDRESSES = list(EXCHANGES.values())

# event OrderFilled(bytes32 indexed orderHash, address indexed maker,
#                    address indexed taker, uint256 makerAssetId,
#                    uint256 takerAssetId, uint256 makerAmountFilled,
#                    uint256 takerAmountFilled, uint256 fee)
# Bu V1 exchange'lerin (ctf_v1, negrisk_v1) kullandigi imza - Trading.sol
# kaynak kodundan dogrulandi.
ORDER_FILLED_SIGNATURE_V1 = (
    "OrderFilled(bytes32,address,address,uint256,uint256,uint256,uint256,uint256)"
)
ORDER_FILLED_TOPIC0_V1 = "0x" + Web3.keccak(text=ORDER_FILLED_SIGNATURE_V1).hex().removeprefix("0x")

# V2 exchange'lerin (ctf_v2, negrisk_v2) FARKLI bir topic0 kullandigi 15 Eylul
# 2026'da gercek bir islemle (verify_tx.py ile) DOGRULANDI 

ORDER_FILLED_TOPIC0_V2 = "0xd543adfd945773f1a62f74f0ee55a5e3b9b1a28262980ba90b1a89f2ea84d8ee"

ORDER_FILLED_TOPICS = [ORDER_FILLED_TOPIC0_V1, ORDER_FILLED_TOPIC0_V2]
TOPIC_TO_LAYOUT = {
    ORDER_FILLED_TOPIC0_V1.lower(): "v1",
    ORDER_FILLED_TOPIC0_V2.lower(): "v2",
}

USDC_ASSET_ID = 0  # Polymarket'te makerAssetId/takerAssetId == 0 => USDC 


def pad_address_topic(address: str) -> str:
    """Bir adresi 32-byte'lik topic formatina (sol dolgulu) cevirir."""
    return "0x" + "0" * 24 + address.lower().replace("0x", "")


def topic_to_address(topic_hex: str) -> str:
    """32-byte topic'ten son 20 byte'i (adres) cikarir, checksum'lar."""
    return Web3.to_checksum_address("0x" + topic_hex[-40:])


def decode_order_filled(log_entry: dict, target_address: str):
    """
    Ham bir eth_subscribe 'logs' bildirimini normalize edilmis bir trade
    dict'ine cevirir. target_address maker VEYA taker degilse None doner.
    """
    topics = log_entry["topics"]
    if len(topics) < 4:
        return None

    layout = TOPIC_TO_LAYOUT.get(topics[0].lower())
    if layout is None:
        return None  # bilinmeyen topic0 (orn. OrdersMatched benzeri yardimci event) - atla

    order_hash = topics[1]
    maker = topic_to_address(topics[2])
    taker = topic_to_address(topics[3])

    data = log_entry["data"]
    data_hex = data[2:] if data.startswith("0x") else data
    words = [int(data_hex[i:i + 64], 16) for i in range(0, len(data_hex), 64)]
    if len(words) < 4:
        log.warning(f"Beklenmeyen data uzunlugu ({layout}), decode edilemedi: {log_entry}")
        return None
    
    
    maker_asset_id, taker_asset_id, maker_amount, taker_amount = words[:4]
    fee = words[4] if len(words) > 4 else 0

    target = target_address.lower()
    if maker.lower() == target:
        # Hedefin KENDI emrinin dolumu (emir sahibi = event'teki "maker" alani).
        role = "maker"
        given_asset, given_amt = maker_asset_id, maker_amount
        recv_asset, recv_amt = taker_asset_id, taker_amount
    elif taker.lower() == target:
        # Hedef, BASKASININ emrinin karsi tarafi. Bkz. aggregate_tx_fills.
        role = "taker"
        given_asset, given_amt = taker_asset_id, taker_amount
        recv_asset, recv_amt = maker_asset_id, maker_amount
    else:
        # Filtre yanlissa ya da relay'den beklenmeyen bir log gelirse buraya duser.
        return None

    if given_asset == USDC_ASSET_ID and recv_asset != USDC_ASSET_ID:
        side = "BUY"
        token_id = recv_asset
        token_amount = recv_amt
        usdc_amount = given_amt
    elif recv_asset == USDC_ASSET_ID and given_asset != USDC_ASSET_ID:
        side = "SELL"
        token_id = given_asset
        token_amount = given_amt
        usdc_amount = recv_amt
    else:
        # Iki taraf da USDC ya da iki taraf da token: split/merge/redeem
        # turevi beklenmeyen bir durum - guvenlik icin atla, kopyalama.
        log.warning(
            f"Beklenmeyen asset kombinasyonu (given={given_asset}, recv={recv_asset}), "
            f"atlaniyor. tx={log_entry.get('transactionHash')}"
        )
        return None

    price = (usdc_amount / token_amount) if token_amount else 0.0
    contract_addr = log_entry["address"].lower()
    exchange_name = EXCHANGE_ADDR_TO_NAME.get(contract_addr, contract_addr)

    return {
        "source": exchange_name,
        "order_hash": order_hash,
        "maker": maker,
        "taker": taker,
        "role": role,
        "side": side,
        "token_id": str(token_id),
        "token_amount_raw": token_amount,
        "usdc_amount_raw": usdc_amount,
        # Polymarket'te hem USDC hem outcome token'lari 6 ondalikla temsil
        # edilir (data-api'nin usdcSize'iyla ayni olcek); dogrulamak icin
        # verify_tx.py ciktisini data-api ile karsilastir.
        "usdc_amount": usdc_amount / 1_000_000,
        "token_amount": token_amount / 1_000_000,
        "price": round(price, 4),
        "tx_hash": log_entry.get("transactionHash"),
        "log_index": int(log_entry.get("logIndex", "0x0"), 16),
        "block_number": int(log_entry.get("blockNumber", "0x0"), 16),
        "raw_log": log_entry,
    }


def aggregate_tx_fills(fills: list) -> list:
    """
    Ayni islemdeki (tx) ham fill'leri hedefin GERCEK islemine indirger:
    (tx, token, yon) basina TEK bir trade dondurur.

    Neden gerekli (cift sayma):
    V1 kontratlarinda (Trading.sol) hedef emri VEREN taraf oldugunda ayni
    islem icin iki tur OrderFilled yayinlanir:
      a) eslestigi her karsi emir icin bir tane  -> maker=karsi taraf, taker=HEDEF
      b) hedefin kendi emri icin bir OZET        -> maker=HEDEF, taker=exchange
    Hem maker hem taker filtresini dinledigimiz icin ikisi de gelir; hepsini
    ayri ayri kopyalarsak ayni alim IKI KEZ acilir. Ustelik (a) turundeki
    loglar karsi emrin bakisindan yazildigi icin, neg-risk/mint eslesmelerinde
    hedefin yonunu/token'ini YANLIS da gosterebilir.

    Kural: bir tx'te hedefin kendi emrine ait (role == "maker") fill varsa
    SADECE onlar sayilir. Hic yoksa (V2'nin ozet event'i yayinlamadigi bir
    durum olursa diye) taker fill'lerine geri dusulur - boylece iki
    varsayimda da islem tam bir kez sayilir.

    Ayrica hedefin tek emri N parcada dolsa bile bot N degil 1 emir gonderir.
    """
    by_tx = {}
    for f in fills:
        by_tx.setdefault(f["tx_hash"], []).append(f)

    out = []
    for tx_hash, group in by_tx.items():
        own = [f for f in group if f["role"] == "maker"]
        used = own if own else group
        buckets = {}
        for f in used:
            buckets.setdefault((f["token_id"], f["side"]), []).append(f)
        for (token_id, side), fs in buckets.items():
            usdc_raw = sum(f["usdc_amount_raw"] for f in fs)
            token_raw = sum(f["token_amount_raw"] for f in fs)
            out.append({
                "source": fs[0]["source"],
                "side": side,
                "token_id": token_id,
                "usdc_amount_raw": usdc_raw,
                "token_amount_raw": token_raw,
                "usdc_amount": usdc_raw / 1_000_000,
                "token_amount": token_raw / 1_000_000,
                "price": round(usdc_raw / token_raw, 4) if token_raw else 0.0,
                "tx_hash": tx_hash,
                "block_number": fs[0]["block_number"],
                "fill_count": len(fs),              # kac ham fill birlestirildi
                "ignored_fills": len(group) - len(used),  # cift sayilmasin diye atilanlar
            })
    return out


async def _emit_trades(fills: list, on_fill, label: str):
    """Ham fill'leri tx bazinda birlestirir ve her gercek islem icin on_fill cagirir."""
    for trade in aggregate_tx_fills(fills):
        log.info(
            f"{label} | {trade['side']} | ${trade['usdc_amount']:.2f} @ {trade['price']:.3f} | "
            f"token={trade['token_id'][:12]}... | kaynak={trade['source']} | "
            f"{trade['fill_count']} fill birlesti, {trade['ignored_fills']} mukerrer atildi | "
            f"tx={trade['tx_hash']}"
        )
        res = on_fill(trade)
        if asyncio.iscoroutine(res):
            await res


async def _rpc_call(ws, method: str, params: list, req_id: int):
    await ws.send(json.dumps({"jsonrpc": "2.0", "id": req_id, "method": method, "params": params}))
    while True:
        raw = await ws.recv()
        msg = json.loads(raw)
        if msg.get("id") == req_id:
            return msg


# Alchemy ucretsiz plan eth_getLogs'ta en fazla 10 bloklik araliga izin
# veriyor (hata mesaji: "up to a 10 block range"). Odemeli plandaysan
# .env'e GETLOGS_CHUNK_BLOCKS=10000 gibi daha buyuk bir deger yazip hizlandirabilirsin.
GETLOGS_CHUNK_BLOCKS = int(os.getenv("GETLOGS_CHUNK_BLOCKS", "10"))
# Ard arda istekler arasinda kisa bir bekleme - ucretsiz planin saniyedeki
# istek limitine takilmamak icin. Odemeli plandaysan .env'den dusurebilirsin.
GETLOGS_REQUEST_DELAY = float(os.getenv("GETLOGS_REQUEST_DELAY", "0.15"))
# Cok uzun sure kapali kaldiysan (gunler vs.), binlerce 10'luk istek atip
# botu dakikalarca telafiye kitlememek icin bir ust sinir. Asilirsa en eski
# kisim ATLANIR (uyari basar) ve sadece son N blok telafi edilir.
MAX_BACKFILL_BLOCKS = int(os.getenv("MAX_BACKFILL_BLOCKS", "2000"))
# Canli dinlemede bir tx'in tum fill'lerinin gelmesi icin beklenen sessizlik
# suresi (sn). Ayni tx'in loglari ayni blok bildirimiyle art arda gelir; bu
# sure kopyalamaya eklenen en fazla gecikmedir.
TX_FLUSH_DELAY = float(os.getenv("TX_FLUSH_DELAY", "0.4"))


async def _get_logs_chunked(ws, address_list, topics, from_block, to_block, chunk=None):
    """eth_getLogs'u blok araligini parcalayarak cagirir (RPC limitlerine takilmamak icin)."""
    chunk = chunk or GETLOGS_CHUNK_BLOCKS
    all_logs = []
    start = from_block
    req_id = 1000
    while start <= to_block:
        end = min(start + chunk - 1, to_block)
        req_id += 1
        resp = await _rpc_call(
            ws, "eth_getLogs",
            [{
                "address": address_list,
                "topics": topics,
                "fromBlock": hex(start),
                "toBlock": hex(end),
            }],
            req_id=req_id,
        )
        if "error" in resp:
            raise RuntimeError(f"eth_getLogs hata verdi: {resp['error']}")
        all_logs.extend(resp.get("result", []))
        start = end + 1
        if start <= to_block and GETLOGS_REQUEST_DELAY:
            await asyncio.sleep(GETLOGS_REQUEST_DELAY)
    return all_logs


async def _backfill(ws, target_address, target_topic, from_block, to_block, on_fill, seen):
    """Baglanti kopmasi/yeniden baslama sirasinda kacan bloklari eth_getLogs ile telafi eder."""
    if from_block > to_block:
        return
    total_gap = to_block - from_block + 1
    if total_gap > MAX_BACKFILL_BLOCKS:
        skipped = total_gap - MAX_BACKFILL_BLOCKS
        log.warning(
            f"Telafi araligi cok genis ({total_gap} blok) - en eski {skipped} blok ATLANIYOR, "
            f"sadece son {MAX_BACKFILL_BLOCKS} blok telafi edilecek. Bu araliktaki islemleri "
            f"manuel kontrol etmek istersen verify_tx.py kullan."
        )
        from_block = to_block - MAX_BACKFILL_BLOCKS + 1
    log.info(f"Kacan blok telafisi: {from_block} -> {to_block} ({to_block - from_block + 1} blok)")
    maker_logs = await _get_logs_chunked(
        ws, EXCHANGE_ADDRESSES, [ORDER_FILLED_TOPICS, None, target_topic], from_block, to_block
    )
    taker_logs = await _get_logs_chunked(
        ws, EXCHANGE_ADDRESSES, [ORDER_FILLED_TOPICS, None, None, target_topic], from_block, to_block
    )
    combined = maker_logs + taker_logs
    # ayni log iki filtreden de gelebilir (teorik) - (tx_hash, logIndex) ile tekillestir,
    # sonra blok+logIndex sirasina gore isle.
    combined.sort(key=lambda l: (int(l["blockNumber"], 16), int(l["logIndex"], 16)))
    fills = []
    for result in combined:
        key = (result.get("transactionHash"), result.get("logIndex"))
        if key in seen:
            continue
        seen.add(key)
        trade = decode_order_filled(result, target_address)
        if trade is None:
            continue
        fills.append(trade)
    # Ayni tx'in tum fill'leri elimizde - tx bazinda birlestirip gonder.
    await _emit_trades(fills, on_fill, "⏮️  TELAFI ISLEM")


async def watch_target(target_address: str, on_fill, wss_url: str = None,
                        from_block: int = None, on_block_progress=None):
    """
    Hedef adresin maker VEYA taker oldugu tum OrderFilled event'lerini
    gercek zamanli dinler. Her yeni fill'de on_fill(trade_dict) cagrilir
    (on_fill async da olabilir, normal fonksiyon da).

    from_block verilirse, (yeniden) baglanildiginda o bloktan itibaren
    kacirilmis olabilecek fill'leri eth_getLogs ile once telafi eder, sonra
    canli dinlemeye gecer. on_block_progress(block_number) verilirse, islenen
    her fill sonrasi en son blok numarasiyla cagrilir - bunu bir dosyaya
    yazarsan, bot yeniden baslatildiginda kaldigi yerden devam edebilir.

    Baglanti koparsa otomatik yeniden baglanir (ustel backoff) ve araya
    kacan bloklari telafi eder.
    """
    import websockets

    wss_url = wss_url or WSS_RPC_URL
    if not wss_url:
        raise RuntimeError("WSS_RPC_URL tanimli degil (.env dosyana ekle).")

    target_topic = pad_address_topic(target_address)
    backoff = 2
    last_block = from_block
    # Sadece bu process omru boyunca telafi+canli sinirinda cift islemeyi onlemek icin.
    seen = set()
    MAX_SEEN = 5000

    while True:
        try:
            log.info(f"WSS baglaniliyor: {wss_url.split('/v2/')[0]}/v2/*** (gizlendi)")
            async with websockets.connect(wss_url, ping_interval=20, ping_timeout=20) as ws:
                latest_resp = await _rpc_call(ws, "eth_blockNumber", [], req_id=999)
                current_block = int(latest_resp["result"], 16)

                if last_block is not None and current_block > last_block:
                    await _backfill(ws, target_address, target_topic, last_block + 1, current_block, on_fill, seen)
                    if len(seen) > MAX_SEEN:
                        seen.clear()  # basit tasma korumasi
                    # Telafi bitince ilerlemeyi HEMEN kaydet - aksi halde bot tam
                    # bu noktada coker/yeniden baslarsa (last_block hala eski
                    # deger oldugundan) ayni araligi tekrar telafi eder ve ayni
                    # fill'i CANLI modda IKINCI KEZ gonderebilir.
                    last_block = current_block
                    if on_block_progress:
                        on_block_progress(last_block)

                # Filtre 1: hedef MAKER iken (topics[2] == hedef)
                sub_maker = await _rpc_call(
                    ws, "eth_subscribe",
                    ["logs", {
                        "address": EXCHANGE_ADDRESSES,
                        "topics": [ORDER_FILLED_TOPICS, None, target_topic],
                    }],
                    req_id=1,
                )
                # Filtre 2: hedef TAKER iken (topics[3] == hedef)
                sub_taker = await _rpc_call(
                    ws, "eth_subscribe",
                    ["logs", {
                        "address": EXCHANGE_ADDRESSES,
                        "topics": [ORDER_FILLED_TOPICS, None, None, target_topic],
                    }],
                    req_id=2,
                )
                sub_ids = {sub_maker.get("result"), sub_taker.get("result")}
                if None in sub_ids:
                    raise RuntimeError(f"Subscribe basarisiz: {sub_maker} / {sub_taker}")

                log.info(f"Dinleniyor: {target_address} (maker+taker, {len(EXCHANGE_ADDRESSES)} kontrat)")
                backoff = 2  # basarili baglanti sonrasi resetle
                last_block = current_block

                # Bir tx'in fill'leri ayri ayri log bildirimi olarak gelir. Onlari
                # burada biriktirip, tx tamamlaninca (baska bir tx'in logu
                # geldiginde ya da TX_FLUSH_DELAY kadar sessizlik oldugunda)
                # TEK islem olarak gonderiyoruz - bkz. aggregate_tx_fills.
                pending = []

                async def flush():
                    nonlocal pending, last_block
                    if not pending:
                        return
                    batch, pending = pending, []
                    await _emit_trades(batch, on_fill, "⚡ ZINCIR-UZERI ISLEM")
                    last_block = max(last_block or 0, max(f["block_number"] for f in batch))
                    if on_block_progress:
                        on_block_progress(last_block)

                try:
                    while True:
                        try:
                            if pending:
                                raw = await asyncio.wait_for(ws.recv(), timeout=TX_FLUSH_DELAY)
                            else:
                                raw = await ws.recv()
                        except asyncio.TimeoutError:
                            await flush()
                            continue

                        msg = json.loads(raw)
                        params = msg.get("params")
                        if not params or params.get("subscription") not in sub_ids:
                            continue
                        result = params["result"]
                        if result.get("removed"):
                            # Reorg nedeniyle geri alinan log - kopyalama.
                            log.warning(f"Log geri alindi (reorg): {result.get('transactionHash')}")
                            continue

                        key = (result.get("transactionHash"), result.get("logIndex"))
                        if key in seen:
                            continue
                        seen.add(key)
                        if len(seen) > MAX_SEEN:
                            seen.clear()

                        trade = decode_order_filled(result, target_address)
                        if trade is None:
                            continue
                        if pending and trade["tx_hash"] != pending[0]["tx_hash"]:
                            await flush()
                        pending.append(trade)
                finally:
                    # Baglanti koparsa elde bekleyen fill'leri kaybetme.
                    await flush()
        except Exception as e:
            log.error(f"Baglanti koptu/hata: {e}. {backoff}sn sonra yeniden denenecek.")
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 60)


if __name__ == "__main__":
    TARGET = os.getenv("TARGET_ADDRESS", "Ox......")  # :)

    def on_fill(trade):
        # Buraya kendi copy_trade(...) cagrini baglayacaksin.
        # Simdilik sadece yazdiriyor (dry-run).
        print(json.dumps({k: v for k, v in trade.items() if k != "raw_log"}, indent=2))

    asyncio.run(watch_target(TARGET, on_fill))