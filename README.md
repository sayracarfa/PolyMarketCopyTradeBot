# Polymarket copy-trade botu — zincir-üzeri (on-chain) versiyon

## Dosyalar

- **`copytrade_bot.py`** — data-api polling yerine on-chain WebSocket dinleme kullanır,
  hem BUY hem SELL'i kopyalar, pozisyon durumunu diske kaydeder, canlı moda
  geçmeden önce ayarları doğrular.
- **`onchain_listener.py`** — `copytrade_bot.py`'ın kullandığı düşük seviye
  dinleyici: `OrderFilled` event'ini 4 bilinen Polymarket exchange
  kontratından (V1 + V2, binary + neg-risk) WebSocket üzerinden yakalar,
  BUY/SELL yönünü ve miktarları decode eder; bağlantı koparsa kaçan
  blokları `eth_getLogs` ile telafi eder.
- **`verify_tx.py`** — **canlıya almadan önce mutlaka çalıştır.** Bilinen bir
  işlem hash'ini decode edip Polymarket arayüzü/data-api ile
  karşılaştırman için.

## Hız Satış Kaydetme

1. **Hız**: 10 saniyelik polling yerine, hedefin işlemi zincire yazıldığı an
   (Polygon'da ortalama ~2 sn) haberin oluyor.
2. **SELL artık kopyalanıyor**: Hedef bir pozisyonu sattığında bot da kendi
   pozisyonunun _aynı oranını_ satıyor (dolar bazında değil — neden dolar
   bazında yapmanın yanlış olduğu `copytrade_bot.py`'ın en üstündeki
   docstring'de açıklanıyor).
3. **Pozisyon takibi diske kaydediliyor** (`<bot_adı>_bot_state.json`) — bot
   yeniden başlatılırsa kaldığı bloktan devam eder, pozisyon bilgisini
   kaybetmez.
4. **Bilinen sınırlama**: Bot ilk çalıştığı andan itibaren hedefin
   pozisyonlarını izlemeye başlar. Hedefin bot başlamadan önce açtığı bir
   pozisyonu satması durumunda, bot kendi pozisyonunun tamamını kapatır
   (oranı bilemediği için güvenli varsayım). Ayrıntı docstring'de var.

## Kurulum

Bot, paketleri sistemden ayrı tutan bir **sanal ortam (venv)** içinde çalışır.
Aşağıdaki komutları bot klasörünün içindeyken çalıştır.

### 1. Sanal ortamı oluştur (yalnızca ilk kurulumda bir kez)

**Mac / Linux (Terminal):**

```bash
python3 -m venv venv
```

**Windows (PowerShell):**

```powershell
py -m venv venv
```

### 2. Sanal ortamı aktif et (her yeni pencerede)

**Mac / Linux:**

```bash
source venv/bin/activate
```

**Windows (PowerShell):**

```powershell
.\venv\Scripts\Activate.ps1
```

**Windows (Komut İstemi / cmd):**

```bat
venv\Scripts\activate.bat
```

Aktif olduğunda satırın başında `(venv)` yazısı görünür. Botu çalıştırmadan
önce bunu kontrol et; görmüyorsan `ModuleNotFoundError` hatası alırsın.

Windows'ta "running scripts is disabled on this system" hatası alırsan bir
kez şunu çalıştır, soru sorarsa `Y` de:

```powershell
Set-ExecutionPolicy -Scope CurrentUser RemoteSigned
```

Sanal ortamdan çıkmak için: `deactivate`

### 3. Paketleri yükle (yalnızca ilk kurulumda, venv aktifken)

```bash
pip install --upgrade pip
pip install -r requirements.txt
```

Windows'ta loglardaki ⚡ ve ⛔ işaretlerinin hata vermemesi için bir kez
`setx PYTHONUTF8 1` çalıştırıp PowerShell'i kapatıp yeniden aç.

### 4. Ayar dosyası

`.env.example` dosyasını `.env` adıyla kopyala ve doldur:

- Mac / Linux: `cp .env.example .env` ardından `nano .env`
- Windows: `copy .env.example .env` ardından `notepad .env`

`.env` dosyasının içeriği:

```
PRIVATE_KEY=...
FUNDER_ADDRESS=0x...
SIGNATURE_TYPE=3
CLOB_API_KEY=...
CLOB_API_SECRET=...
CLOB_API_PASSPHRASE=...
WSS_RPC_URL=wss://polygon-mainnet.g.alchemy.com/v2/<API_KEY>
TARGET_ADDRESS=  Ox... Kopyalamak istediğiniz cüzdan
COPY_RATIO=0.05
MIN_AMOUNT=1.0
MAX_AMOUNT=4.0
MAX_PRICE=0.97
MIN_PRICE=0.45
MAX_TOTAL_EXPOSURE=0
SELL_DUST_THRESHOLD=0.5
MAX_PER_TOKEN=0
```

### Birebir kopya + hisse başı tavan

Hedefin açtığı tutarın aynısını açmak, ama tek bir hissede en fazla 1000 $
tutmak için:

```
COPY_RATIO=1.0
MAX_AMOUNT=0
MAX_PER_TOKEN=1000
```

- `MAX_AMOUNT` tek işlemin tavanıdır (`0` = kapalı).
- `MAX_PER_TOKEN` tek hissedeki açık pozisyonun maliyet tavanıdır (`0` =
  kapalı). Tavana yaklaşınca yalnızca kalan kadar alınır, dolunca hedef
  eklemeye devam etse bile alım atlanır. Satışlar etkilenmez; pozisyon
  satıldıkça o hissede yeniden yer açılır.
- Hisse başı harcama `usdc_per_token` alanında durum dosyasına yazılır. Bu
  özellik eklenmeden önce açılmış pozisyonların maliyeti bilinmediği için
  onlar 0'dan başlar.

### Çift sayma koruması

Hedef emri veren taraf olduğunda aynı işlem zincirde birden fazla
`OrderFilled` olayı üretir. Dinleyici artık bir işlemin (tx) tüm olaylarını
birleştirip hedefin gerçek işlemini **tek sefer** kopyalar. Gerçek bir işlemde
kontrol etmek için `verify_tx.py` çıktısının sonundaki "ÇİFT SAYMA KONTROLÜ"
bölümüne bak.

`WSS_RPC_URL` için Alchemy, Infura, Chainstack ya da QuickNode'dan ücretsiz
bir Polygon mainnet WebSocket RPC anahtarı alabilirsin.

## Canlıya almadan önce sırayla yap

1. **Decode doğrulaması** — Polymarket arayüzünde hedefin (Ox..) yakın
   zamanda yaptığı birkaç işlemin tx hash'ini bul (Activity sekmesinde
   işleme tıklayıp "View on Polygonscan"), sonra:

   ```bash
   python3 verify_tx.py <tx_hash>
   ```

   Çıkan `usdc_amount` / `price` değerlerini Polymarket arayüzü/data-api ile
   karşılaştır. Özellikle kontrat `ctf_v2` ya da `negrisk_v2` olarak görünen
   fill'lere dikkat et — bu kontratların event şeması doğrulanmadı.

2. **Dry-run** — gerçek para harcamadan bir süre çalıştır, hedefin
   işlemlerini doğru yakalayıp yakalamadığını, BUY/SELL yönünü doğru
   çözümleyip çözümlemediğini logdan takip et:

   ```bash
   python3 copytrade_bot.py
   ```

3. **Küçük miktarla canlı test** — `MIN_AMOUNT`/`MAX_AMOUNT`'ı çok düşük
   tutarak (örn. `MIN_AMOUNT=1 MAX_AMOUNT=1`) gerçek modda bir-iki işlem
   dene, özellikle SELL emrinin gerçekten hisse cinsinden gönderildiğini
   (dosyadaki `submit_sell`'deki uyarıya bak) borsa arayüzünden doğrula:

   ```bash
   python3 copytrade_bot.py --live
   ```

4. Sorun yoksa limitleri gerçek kullanacağın seviyeye çek.

## BOL ŞANSLAR
