"""
Polymarket copy-trade botu - ZINCIR-UZERI (on-chain) versiyon.



  1) Hedefin islemlerini `data-api` polling yerine Polygon'daki
     `OrderFilled` event'ini WebSocket uzerinden CANLI dinleyerek yakalar
     (bkz. onchain_listener.py). Gecikme ~saniyeler mertebesine iner.
  2) Sadece BUY degil, SELL/pozisyon kapama da kopyalanir - hedef bir
     pozisyonu sattiginda, botun KENDI pozisyonunun ayni ORANDA satilir
     (dolar bazinda degil, hisse/oran bazinda - asagida aciklaniyor).
  3) Baglanti kopmasi/yeniden baslatma durumunda kacan islemleri telafi
     eder ve pozisyon durumunu diske kaydeder (bot_state.json).
  4) Baslamadan once (canli modda) gerekli tum ortam degiskenlerini
     kontrol eder, eksikse ANLASILIR bir hata ile durur (API hatasi
     bekleyip kafa karistirmak yerine).
  5) Basit bir toplam maruziyet (exposure) tavani var (MAX_TOTAL_EXPOSURE).

SELL kopyalama mantigi
-----------------------
Sen hedefin islemlerinin sadece COPY_RATIO kadarini kopyaliyorsun (orn.
%5). Yani hedef ALIRKEN dolar bazinda kopyalamak doğru: onun harcadigi
USDC'nin %5'i kadar harciyorsun. Ama hedef SATARKEN dolar bazinda
kopyalamak YANLIS olur - cunku elindeki pozisyon zaten onunkinden kucuk.
Bunun yerine: hedef kendi pozisyonunun ne KADARINI (yuzde olarak)
satiyorsa, sen de kendi pozisyonunun ayni yuzdesini satarsin. Bu yuzden
bot hem hedefin hem kendi pozisyonunu token bazinda takip eder
(target_position / our_position).

Bilinen sinirlama: Bot ilk calistigi anda hedefin o ana kadarki mevcut
pozisyonlarindan HABERSIZDIR - sadece bot baslatildiktan sonra gozlemlenen
BUY'lardan pozisyon insa eder. Yani hedef, bot baslamadan once aldigi bir
pozisyonu satarsa, bot o satisin ORANINI dogru hesaplayamaz (target_position
o token icin 0'dan basladigindan fraction=1.0 varsayar, yani KENDI
pozisyonunun tamamini satar). Bu genelde makul bir varsayilan davranistir
(pozisyonu tamamen kapatmak), ama bilerek boyle tasarlandigini bil.

Kurulum:
    pip install websockets web3 python-dotenv requests py-clob-client-v2 --break-system-packages

.env:
    PRIVATE_KEY=...
    FUNDER_ADDRESS=0x...        # Polymarket cuzdan adresin (zorunlu)
    SIGNATURE_TYPE=3            # hesap turune gore emir imzalama tipi
    CLOB_API_KEY=...
    CLOB_API_SECRET=...
    CLOB_API_PASSPHRASE=...
    WSS_RPC_URL=wss://polygon-mainnet.g.alchemy.com/v2/<KEY>
    TARGET_ADDRESS=Ox.....
    COPY_RATIO=0.05
    MIN_AMOUNT=1.0
    MAX_AMOUNT=4.0              # tek islem tavani. 0 = kapali.
    MAX_PER_TOKEN=0             # tek hissede en fazla acik dolar. 0 = kapali. Ornek: 1000
    MAX_PRICE=0.97
    MIN_PRICE=0.45
    MAX_TOTAL_EXPOSURE=0        # 0 = sinirsiz (kapali). Ornek: 100 -> ayni anda en fazla $100 acik pozisyon.

    Birebir kopya + hisse basi 1000$ tavan icin:
        COPY_RATIO=1.0
        MAX_AMOUNT=0
        MAX_PER_TOKEN=1000
    SELL_DUST_THRESHOLD=0.5     # bu miktarin altindaki hisse satislarini atla (gas'a degmez)
    MIN_ORIGINAL=0              # hedefin islem buyuklugu ($ olarak) bunun altindaysa kopyalama (0 = kapali)

Calistirma:
    python3 copytrade_bot.py            # dry-run (emir gondermez)
    python3 copytrade_bot.py --live      # gercek emir gonderir
"""

import os
import json
import math
import asyncio
import logging
from dotenv import load_dotenv

from onchain_listener import watch_target

load_dotenv()

NAME = os.getenv("BOT_NAME", "Flaznorp")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(), logging.FileHandler(f"{NAME.lower()}_copy_onchain.log")],
)
log = logging.getLogger(f"copy-{NAME}")

# --- Ayarlar -----------------------------------------------------------
TARGET             = os.getenv("TARGET_ADDRESS", "0x.. KOPYALANACAK CÜZDAN ADRESİ").lower()
PRIVATE_KEY        = os.getenv("PRIVATE_KEY")

FUNDER             = os.getenv("FUNDER_ADDRESS", "")
# Polymarket hesap turune gore emir imzalama tipi (py_clob_client_v2).
SIGNATURE_TYPE     = int(os.getenv("SIGNATURE_TYPE", "3"))
COPY_RATIO         = float(os.getenv("COPY_RATIO", "0.05"))
MIN_AMOUNT         = float(os.getenv("MIN_AMOUNT", "1.0"))
MAX_AMOUNT         = float(os.getenv("MAX_AMOUNT", "4.0"))   # tek islem tavani; 0 = kapali
# Tek bir hissede (token) ayni anda acik tutulacak en fazla dolar. Hedef o
# hisseye ne kadar eklerse eklesin, bizim o hissedeki maliyetimiz bunu gecmez;
# tavana yaklasinca sadece kalan kadar alinir, dolunca alim atlanir.
# SELL'ler etkilenmez. 0 = kapali.
MAX_PER_TOKEN      = float(os.getenv("MAX_PER_TOKEN", "0"))
MAX_PRICE          = float(os.getenv("MAX_PRICE", "0.97"))
MIN_PRICE          = float(os.getenv("MIN_PRICE", "0.45"))
MAX_TOTAL_EXPOSURE = float(os.getenv("MAX_TOTAL_EXPOSURE", "0"))   # 0 = kapali
SELL_DUST_THRESHOLD = float(os.getenv("SELL_DUST_THRESHOLD", "0.5"))
MIN_ORIGINAL       = float(os.getenv("MIN_ORIGINAL", "0"))   # 0 = kapali
STATE_FILE         = os.getenv("STATE_FILE", f"{NAME.lower()}_bot_state.json")


# --- Durum (pozisyon) yonetimi -----------------------------------------
def load_state():
    state = {"last_block": None, "target_position": {}, "our_position": {}, "usdc_deployed": 0.0}
    if os.path.exists(STATE_FILE):
        try:
            with open(STATE_FILE, "r") as f:
                state = json.load(f)
        except Exception as e:
            log.warning(f"State dosyasi okunamadi ({e}), sifirdan basliyor.")
    # Hisse basina harcanan dolar (MAX_PER_TOKEN icin). Eski state
    # dosyalarinda bu alan yok - o pozisyonlarin maliyeti bilinmedigi icin
    # 0'dan baslar.
    state.setdefault("usdc_per_token", {})
    return state


def save_state(state):
    tmp = STATE_FILE + ".tmp"
    with open(tmp, "w") as f:
        json.dump(state, f, indent=2)
    os.replace(tmp, STATE_FILE)  # atomik - yaziya ara girerse dosya bozulmaz


# --- CLOB istemcisi ------------------------------------------------------
def get_clob_client():
    try:
        from py_clob_client_v2.client import ClobClient
        from py_clob_client_v2.clob_types import ApiCreds

        api_key = os.getenv("CLOB_API_KEY")
        api_secret = os.getenv("CLOB_API_SECRET")
        api_passphrase = os.getenv("CLOB_API_PASSPHRASE")
        creds = ApiCreds(api_key=api_key, api_secret=api_secret, api_passphrase=api_passphrase)
        client = ClobClient(
            host="https://clob.polymarket.com",
            key=PRIVATE_KEY,
            chain_id=137,
            signature_type=SIGNATURE_TYPE,
            funder=FUNDER,
            creds=creds,
        )
        log.info(f"✅ CLOB istemcisi hazir (funder={FUNDER}).")
        return client
    except Exception as e:
        log.error(f"CLOB baslatilamadi: {e}")
        return None


def validate_live_config():
    """Canli moda gecmeden once eksik ayarlari yakalar - API hatasindan once."""
    missing = []
    if not PRIVATE_KEY:
        missing.append("PRIVATE_KEY")
    if not (FUNDER.startswith("0x") and len(FUNDER) == 42):
        missing.append("FUNDER_ADDRESS")
    if not os.getenv("CLOB_API_KEY"):
        missing.append("CLOB_API_KEY")
    if not os.getenv("CLOB_API_SECRET"):
        missing.append("CLOB_API_SECRET")
    if not os.getenv("CLOB_API_PASSPHRASE"):
        missing.append("CLOB_API_PASSPHRASE")
    if not os.getenv("WSS_RPC_URL"):
        missing.append("WSS_RPC_URL")
    if not TARGET.startswith("0x") or len(TARGET) != 42:
        missing.append(f"TARGET_ADDRESS gecersiz gorunuyor ({TARGET})")
    if missing:
        raise SystemExit(
            "❌ Canli mod icin eksik/hatali ayarlar: " + ", ".join(missing) +
            "\n.env dosyani kontrol et."
        )


# --- Emir gonderme --------------------------------------------------------
def submit_buy(client, token_id: str, usdc_amount: float):
    from py_clob_client_v2.clob_types import MarketOrderArgs, OrderType
    from py_clob_client_v2.order_builder.constants import BUY

    order = client.create_market_order(
        MarketOrderArgs(token_id=token_id, amount=round(usdc_amount, 2), side=BUY)
    )
    return client.post_order(order, OrderType.FOK)


def submit_sell(client, token_id: str, shares: float):
    # DIKKAT: py-clob-client / py_clob_client_v2'de market SELL emrinde
    # 'amount' USD degil, satilacak HISSE (share) sayisidir - BUY'daki
    # anlamiyla farkli. Bunu canliya almadan once kucuk bir miktarla
    # (orn. $1'lik) test ederek dogrula.
    from py_clob_client_v2.clob_types import MarketOrderArgs, OrderType
    from py_clob_client_v2.order_builder.constants import SELL

    order = client.create_market_order(
        MarketOrderArgs(token_id=token_id, amount=round(shares, 2), side=SELL)
    )
    return client.post_order(order, OrderType.FOK)


# --- Fill isleme -----------------------------------------------------------
def handle_buy(client, dry_run, state, trade):
    price = trade["price"]
    token_id = trade["token_id"]

    # target_position'i FILTRELERDEN BAGIMSIZ, kosulsuz guncelle - bu bizim
    # kopyalayip kopyalamadigimizdan bagimsiz olarak hedefin GERCEK
    # pozisyonunun modeli. Bunu filtre/tavan kontrollerinden sonraya
    # koyarsak (once oyle bir hata vardi), bir BUY fiyat filtresine takilip
    # atlandiginda hedefin pozisyonu bizim defterimizde eksik kalir ve
    # sonraki bir SELL'in oranini yanlis hesaplariz.
    state["target_position"][token_id] = state["target_position"].get(token_id, 0.0) + trade["token_amount"]

    if MIN_ORIGINAL > 0 and trade["usdc_amount"] < MIN_ORIGINAL:
        log.info(f"  ⏭️  Hedefin islemi kucuk (${trade['usdc_amount']:.2f} < ${MIN_ORIGINAL}), atlandi.")
        return

    if price > MAX_PRICE:
        log.info(f"  ⚠️  Fiyat {price:.3f} > MAX_PRICE, atlandi.")
        return
    if price < MIN_PRICE:
        log.info(f"  ⚠️  Fiyat {price:.3f} < MIN_PRICE, atlandi.")
        return

    our_amount = trade["usdc_amount"] * COPY_RATIO
    if MAX_AMOUNT > 0:
        our_amount = min(MAX_AMOUNT, our_amount)
    our_amount = round(max(MIN_AMOUNT, our_amount), 2)

    if MAX_PER_TOKEN > 0:
        spent = state["usdc_per_token"].get(token_id, 0.0)
        remaining = MAX_PER_TOKEN - spent
        if remaining < MIN_AMOUNT:
            log.info(
                f"  ⛔ Hisse tavani dolu (${spent:.2f} / ${MAX_PER_TOKEN:.2f}), alim atlandi. "
                f"token={token_id[:12]}..."
            )
            return
        if our_amount > remaining:
            # Asagi yuvarla - yuvarlama yuzunden tavani kurus bile asmasin.
            capped = math.floor(remaining * 100) / 100
            log.info(f"  ✂️  Hisse tavani: ${our_amount:.2f} yerine kalan ${capped:.2f} alinacak.")
            our_amount = capped

    if MAX_TOTAL_EXPOSURE > 0 and state["usdc_deployed"] + our_amount > MAX_TOTAL_EXPOSURE:
        log.info(
            f"  ⚠️  MAX_TOTAL_EXPOSURE asilirdi (${state['usdc_deployed']:.2f} + "
            f"${our_amount:.2f} > ${MAX_TOTAL_EXPOSURE:.2f}), atlandi."
        )
        return

    log.info(f"  📈 BUY kopyalaniyor: ${our_amount:.2f} @ {price:.3f} (hedef ${trade['usdc_amount']:.2f} harcadi)")

    if dry_run:
        log.info(f"  [RUN] ${our_amount:.2f} ile BUY acildi.")
        our_filled_shares = our_amount / price if price else 0.0
    else:
        resp = submit_buy(client, token_id, our_amount)
        if not (resp and resp.get("success")):
            log.warning(f"  ❌ BUY reddedildi: {resp}")
            return
        log.info(f"  ✅ BUY ACILDI | ${our_amount}")
        # Gercek fill miktari client yanitinda varsa onu kullanmak daha
        # dogrudur (resp.get('makingAmount') vb. - client'in API'sine gore
        # degisir); yoksa fiyat uzerinden yaklasik hesapliyoruz.
        our_filled_shares = resp.get("filledTokenAmount") or (our_amount / price if price else 0.0)

    state["our_position"][token_id] = state["our_position"].get(token_id, 0.0) + our_filled_shares
    state["usdc_deployed"] = state.get("usdc_deployed", 0.0) + our_amount
    state["usdc_per_token"][token_id] = state["usdc_per_token"].get(token_id, 0.0) + our_amount


def handle_sell(client, dry_run, state, trade):
    token_id = trade["token_id"]
    target_held = state["target_position"].get(token_id, 0.0)
    our_held = state["our_position"].get(token_id, 0.0)

    # target_position'i (BUY'daki gibi) her durumda, erken cikislardan ONCE
    # guncelle - hedefin gercek pozisyonu bizim kopyalayip kopyalamadigimizdan
    # bagimsiz olmali, yoksa ileride yanlis fraction hesaplariz.
    if target_held > 0:
        fraction = min(1.0, trade["token_amount"] / target_held)
    else:
        fraction = 1.0
    state["target_position"][token_id] = max(0.0, target_held - trade["token_amount"])

    if our_held <= 0:
        log.info(f"  ⏭️  Hedef satti ama bizde bu token'dan pozisyon yok, atlandi. token={token_id[:12]}...")
        return

    if MIN_ORIGINAL > 0 and trade["usdc_amount"] < MIN_ORIGINAL:
        log.info(f"  ⏭️  Hedefin satisi kucuk (${trade['usdc_amount']:.2f} < ${MIN_ORIGINAL}), atlandi.")
        return

    our_sell_amount = round(our_held * fraction, 4)

    if our_sell_amount < SELL_DUST_THRESHOLD:
        log.info(f"  ⏭️  Satilacak miktar cok kucuk (${our_sell_amount:.4f} hisse), atlandi.")
        return

    log.info(
        f"  📉 SELL kopyalaniyor: {our_sell_amount:.4f} hisse (hedefin pozisyonunun %{fraction*100:.1f}'i) "
        f"@ {trade['price']:.3f}"
    )

    if dry_run:
        log.info(f"  [DRY RUN] {our_sell_amount:.4f} hisse SATILIRDI.")
    else:
        resp = submit_sell(client, token_id, our_sell_amount)
        if not (resp and resp.get("success")):
            log.warning(f"  ❌ SELL reddedildi: {resp}")
            return
        log.info(f"  ✅ SELL ACILDI | {our_sell_amount:.4f} hisse")

    state["our_position"][token_id] = max(0.0, our_held - our_sell_amount)
    state["usdc_deployed"] = max(0.0, state.get("usdc_deployed", 0.0) - our_sell_amount * trade["price"])
    # Hisse tavani "acik pozisyonun maliyeti" uzerinden calisir: pozisyonun
    # yuzde kaci satildiysa o hissedeki harcama kaydi da o oranda duser,
    # boylece hedef satip yeniden girerse tavana kadar yeniden yer acilir.
    spent = state["usdc_per_token"].get(token_id, 0.0)
    left = spent * (1 - min(1.0, our_sell_amount / our_held))
    if left < 0.01:
        state["usdc_per_token"].pop(token_id, None)
    else:
        state["usdc_per_token"][token_id] = left


def make_on_fill(client, dry_run, state):
    def on_fill(trade):
        log.info(
            f"\n{'─'*50}\n"
            f"  {trade['side']} | token={trade['token_id'][:16]}...\n"
            f"  Fiyat: {trade['price']:.3f} | Hedef miktar: {trade['token_amount']:.2f} hisse / "
            f"${trade['usdc_amount']:.2f}\n"
            f"  Kaynak: {trade['source']} | tx={trade['tx_hash']}\n"
            f"{'─'*50}"
        )
        if trade["side"] == "BUY":
            handle_buy(client, dry_run, state, trade)
        elif trade["side"] == "SELL":
            handle_sell(client, dry_run, state, trade)
        save_state(state)

    return on_fill


# --- Ana calisma -----------------------------------------------------------
async def run(dry_run: bool = True):
    log.info("=" * 60)
    log.info(f"  {NAME} Copy-Trade — ZINCIR-UZERI (on-chain) mod")
    log.info(f"  Mod    : {'DRY RUN' if dry_run else '💰 GERCEK'}")
    log.info(f"  Oran   : %{COPY_RATIO*100:.0f} | Min:${MIN_AMOUNT} Max:{'kapali' if MAX_AMOUNT == 0 else f'${MAX_AMOUNT}'}")
    log.info(f"  Fiyat  : {MIN_PRICE} – {MAX_PRICE}")
    log.info(f"  Hisse basi tavan: {'kapali' if MAX_PER_TOKEN == 0 else f'${MAX_PER_TOKEN}'}")
    log.info(f"  Maruziyet tavani: {'kapali' if MAX_TOTAL_EXPOSURE == 0 else f'${MAX_TOTAL_EXPOSURE}'}")
    log.info("=" * 60)

    client = None
    if not dry_run:
        validate_live_config()
        client = get_clob_client()
        if not client:
            log.error("CLOB istemcisi kurulamadi, cikiliyor.")
            return

    state = load_state()
    on_fill = make_on_fill(client, dry_run, state)

    def on_block_progress(block_number):
        state["last_block"] = block_number
        # save_state zaten her fill'den sonra cagriliyor (make_on_fill icinde);
        # burada ayrica cagirmaya gerek yok, last_block referansi state
        # dict'i uzerinden zaten guncellendi.

    log.info(f"Izleme basladi (hedef={TARGET}). Ctrl+C ile durdur.\n")
    try:
        await watch_target(
            TARGET, on_fill,
            from_block=state.get("last_block"),
            on_block_progress=on_block_progress,
        )
    except KeyboardInterrupt:
        pass
    finally:
        save_state(state)
        log.info("Bot durduruldu, durum kaydedildi.")


if __name__ == "__main__":
    import argparse
    p = argparse.ArgumentParser()
    p.add_argument("--live", action="store_true")
    args = p.parse_args()
    try:
        asyncio.run(run(dry_run=not args.live))
    except KeyboardInterrupt:
        pass