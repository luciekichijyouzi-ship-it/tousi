"""
高配当株スクリーナー: データ収集スクリプト v2

処理の流れ
  1. JPXの上場銘柄一覧から普通株を抽出（英字入りの新コード 130A 等にも対応）
  2. 全銘柄の株価と直近1年の配当を一括取得し、実績利回りで一次選別（API呼び出しを約1/4に削減）
  3. 一次選別を通過した銘柄だけ、4年分の財務を取得（30日キャッシュ・リトライ付き）
  4. 景気シグナル（TOPIX連動ETF・米10年金利・ドル円・VIX）を取得
  5. docs/data.json に保存

環境変数
  UNIVERSE_LIMIT  テスト用。先頭N銘柄だけ処理（例: 50）
  MIN_YIELD       一次選別の最低利回り%（既定 2.5）
  CACHE_DAYS      財務キャッシュの有効日数（既定 30）
  MARKETS         対象市場（既定 "プライム,スタンダード,グロース"＝日本の上場株すべて）
  COUNTRIES       対象国（既定 "JP,US"）。US は米国高配当ETF（VYM・HDV・SPYD）
  US_STOCKS       1 にすると米国の個別株（S&P500採用銘柄）も集める（既定 0＝ETFだけ）
  MIN_YIELD_US    米国個別株の一次選別の最低利回り%（既定 2.0）
"""
import io, json, math, os, random, re, time, datetime as dt
from urllib.parse import urljoin
from pathlib import Path

import pandas as pd
import requests
import yfinance as yf

SP500_LIST = "https://raw.githubusercontent.com/datasets/s-and-p-500-companies/main/data/constituents.csv"
JPX_LIST = "https://www.jpx.co.jp/markets/statistics-equities/misc/tvdivq0000001vg2-att/data_j.xls"
# 保存先：docs フォルダにアプリがあればそこ、なければリポジトリの一番上（index.html と同じ場所）
OUT = (Path("docs") if Path("docs/index.html").exists() else Path(".")) / "data.json"
CACHE_DIR = Path("cache")
CACHE_DIR.mkdir(exist_ok=True)
JST = dt.timezone(dt.timedelta(hours=9))

LIMIT = int(os.getenv("UNIVERSE_LIMIT") or 0)
MIN_YIELD = float(os.getenv("MIN_YIELD") or 2.5)
CACHE_DAYS = int(os.getenv("CACHE_DAYS") or 30)
COUNTRIES = [c.strip().upper() for c in (os.getenv("COUNTRIES") or "JP,US").split(",") if c.strip()]
MIN_YIELD_US = float(os.getenv("MIN_YIELD_US") or 2.0)
US_STOCKS = (os.getenv("US_STOCKS") or "0").strip() in ("1", "true", "yes")
MARKETS = [m.strip() for m in (os.getenv("MARKETS") or "プライム,スタンダード,グロース").split(",") if m.strip()]


def log(*a):
    print(dt.datetime.now(JST).strftime("%H:%M:%S"), *a, flush=True)


def retry(fn, tries=4, base=5, label=""):
    """レート制限・一時エラー向けの指数バックオフ"""
    for k in range(tries):
        try:
            return fn()
        except Exception as e:
            wait = base * (2 ** k) + random.random() * 3
            log(f"{label} 失敗({k + 1}/{tries}): {str(e)[:120]} → {wait:.0f}秒待機")
            time.sleep(wait)
    return None


def clean(v):
    """NaN/inf を None にして JSON を壊さない"""
    try:
        if v is None:
            return None
        f = float(v)
        return None if math.isnan(f) or math.isinf(f) else f
    except Exception:
        return None


# ---------- 1. 銘柄一覧 ----------
JPX_PAGE = "https://www.jpx.co.jp/markets/statistics-equities/misc/01.html"
JPX_PAGE_EN = "https://www.jpx.co.jp/english/markets/statistics-equities/misc/01.html"
JPX_LIST_EN = "https://www.jpx.co.jp/english/markets/statistics-equities/misc/tvdivq0000001vg2-att/data_e.xls"
UA = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/126.0 Safari/537.36",
      "Accept": "text/html,application/xhtml+xml,application/vnd.ms-excel,*/*;q=0.8",
      "Accept-Language": "ja,en;q=0.8", "Referer": JPX_PAGE}
MKT_EN = {"プライム": "Prime", "スタンダード": "Standard", "グロース": "Growth"}
SECTOR_EN_JA = {"Fishery, Agriculture & Forestry": "水産・農林業", "Mining": "鉱業", "Construction": "建設業", "Foods": "食料品",
    "Textiles & Apparels": "繊維製品", "Pulp & Paper": "パルプ・紙", "Chemicals": "化学", "Pharmaceutical": "医薬品",
    "Oil & Coal Products": "石油・石炭製品", "Rubber Products": "ゴム製品", "Glass & Ceramics Products": "ガラス・土石製品",
    "Iron & Steel": "鉄鋼", "Nonferrous Metals": "非鉄金属", "Metal Products": "金属製品", "Machinery": "機械",
    "Electric Appliances": "電気機器", "Transportation Equipment": "輸送用機器", "Precision Instruments": "精密機器",
    "Other Products": "その他製品", "Electric Power & Gas": "電気・ガス業", "Land Transportation": "陸運業",
    "Marine Transportation": "海運業", "Air Transportation": "空運業",
    "Warehousing & Harbor Transportation Services": "倉庫・運輸関連業", "Information & Communication": "情報・通信業",
    "Wholesale Trade": "卸売業", "Retail Trade": "小売業", "Banks": "銀行業",
    "Securities & Commodity Futures": "証券、商品先物取引業", "Insurance": "保険業",
    "Other Financing Business": "その他金融業", "Real Estate": "不動産業", "Services": "サービス業"}
# Yahoo Financeの業種（最後の手段で使う）を日本語の大分類に
YSECTOR_JA = {"Technology": "情報技術", "Industrials": "資本財", "Consumer Cyclical": "一般消費財",
    "Consumer Defensive": "生活必需品", "Financial Services": "金融", "Healthcare": "ヘルスケア",
    "Basic Materials": "素材", "Communication Services": "通信サービス", "Energy": "エネルギー",
    "Utilities": "公益", "Real Estate": "不動産"}
UNIVERSE_CACHE = CACHE_DIR / "universe_jp.json"
# 実際の一覧は約3,800銘柄。これより極端に少ない一覧は壊れているとみなす（テスト用の UNIVERSE_LIMIT 指定時は緩める）
MIN_UNIVERSE = int(os.getenv("MIN_UNIVERSE") or (1 if LIMIT else 1000))
UNIVERSE_SOURCE = "jpx"


def _is_excel(b: bytes) -> bool:
    return b[:4] == b"\xd0\xcf\x11\xe0" or b[:2] == b"PK"      # .xls / .xlsx の先頭


def _get(url, label):
    """取得して結果をログに残す（止まった時にログで理由が分かるように）"""
    try:
        r = requests.get(url, timeout=60, headers=UA)
    except Exception as e:
        log(f"{label}: 接続失敗 {str(e)[:150]}")
        return None
    try:   # 記録に失敗しても取得結果は使う
        ctype = (getattr(r, "headers", None) or {}).get("Content-Type", "?")
        log(f"{label}: HTTP {getattr(r, 'status_code', '?')}・{len(getattr(r, 'content', b'') or b''):,}バイト・{ctype}")
    except Exception:
        pass
    return r


def _fetch_excel(url, label):
    for k in range(3):
        r = _get(url, label)
        if r is not None and r.status_code == 200 and _is_excel(r.content):
            return r.content
        if r is not None and r.status_code == 200:
            log(f"{label}: Excelではない内容が返されました（先頭: {r.content[:60]!r}）")
        time.sleep(5 * (k + 1))
    return None


def _find_link(page_url, pattern, label):
    r = _get(page_url, label)
    if r is None or r.status_code != 200:
        return None
    m = re.search(pattern, r.text)
    return urljoin(page_url, m.group(1)) if m else None


def _parse_jpx(content, english=False) -> pd.DataFrame:
    df = pd.read_excel(io.BytesIO(content))
    df.columns = [str(c).strip() for c in df.columns]

    def col(*cands):
        for c in cands:
            if c in df.columns:
                return c
        for c in cands:
            hit = next((x for x in df.columns if c.lower() in x.lower()), None)
            if hit:
                return hit
        return None

    if english:
        code_col, name_col = col("Local Code", "Code"), col("Name (English)", "Name")
        mkt_col, sec_col = col("Section/Products", "Market"), col("33 Sector(name)", "33 Sector")
    else:
        code_col, name_col = col("コード"), col("銘柄名")
        mkt_col, sec_col = col("市場・商品区分", "市場"), col("33業種区分")
    if not all([code_col, name_col, mkt_col]):
        raise ValueError(f"銘柄一覧の列名が想定と違います: {list(df.columns)}")
    df = df.rename(columns={code_col: "code", name_col: "name", mkt_col: "market"})
    df["sector"] = df[sec_col].astype(str).str.strip() if sec_col else "未分類"
    if english:
        df["sector"] = df["sector"].map(lambda x: SECTOR_EN_JA.get(x, x))
    df["code"] = df["code"].astype(str).str.strip().str.upper().str.replace(r"\.0$", "", regex=True)
    df = df[df["code"].str.fullmatch(r"\d{3}[0-9A-Z]")]                  # 普通株（新コード対応）
    keys = [MKT_EN.get(k, k) for k in MARKETS] if english else MARKETS
    df = df[df["market"].astype(str).apply(lambda m: any(k in m for k in keys) and "外国" not in m and "Foreign" not in m)]
    df = df[~df["sector"].isin(["-", "nan", ""])]
    return df[["code", "name", "market", "sector"]].drop_duplicates("code").reset_index(drop=True)


def _scan_universe() -> pd.DataFrame:
    """最後の手段：証券コード1300〜9999をYahoo Financeで総当たり（名前・業種は後で補う）"""
    log("JPXの一覧が取れないため、証券コードを総当たりで確認します（銘柄名は英語表記になります）")
    codes = [str(c) for c in range(1300, 10000)]
    return pd.DataFrame({"code": codes, "name": codes, "market": "東証", "sector": "", "need_info": True})


def load_universe() -> pd.DataFrame:
    global UNIVERSE_SOURCE
    log("上場銘柄一覧を取得")
    df = None
    tries = [("JPX一覧", lambda: JPX_LIST, False),
             ("JPXページから一覧を探す", lambda: _find_link(JPX_PAGE, r'href="([^"]*data_j\.xlsx?)"', "JPXページ"), False),
             ("JPX英語版一覧", lambda: JPX_LIST_EN, True),
             ("JPX英語版ページから探す", lambda: _find_link(JPX_PAGE_EN, r'href="([^"]*data_e\.xlsx?)"', "JPX英語版ページ"), True)]
    for label, url_fn, en in tries:
        url = url_fn()
        if not url:
            continue
        content = _fetch_excel(url, label)
        if not content:
            continue
        try:
            df = _parse_jpx(content, english=en)
            if len(df) < MIN_UNIVERSE:
                log(f"{label}: 銘柄数が少なすぎます（{len(df)}）。別の取得方法を試します")
                df = None
                continue
            UNIVERSE_SOURCE = "jpx_en" if en else "jpx"
            UNIVERSE_CACHE.write_text(df.to_json(orient="records", force_ascii=False))
            break
        except Exception as e:
            log(f"{label}: 読み込み失敗 {str(e)[:200]}")
            df = None
    if df is None and UNIVERSE_CACHE.exists():
        try:
            df = pd.read_json(io.StringIO(UNIVERSE_CACHE.read_text()), orient="records", dtype={"code": str})
            UNIVERSE_SOURCE = "cache"
            log(f"JPXから取れないため、前回の一覧（{len(df)}銘柄）を使います")
        except Exception as e:
            log(f"前回の一覧も読めません: {e}")
            df = None
    if df is None:
        df = _scan_universe()
        UNIVERSE_SOURCE = "yahoo_scan"
    if LIMIT:
        df = df.head(LIMIT)
    log(f"対象 {len(df)} 銘柄（一覧の取得元: {UNIVERSE_SOURCE}）")
    return df


def fill_info(row):
    """総当たりで見つけた銘柄の名前・業種・種類をYahooから補う。普通株以外（ETF・REIT等）は None"""
    def get():
        return yf.Ticker(row["yft"]).info
    info = retry(get, tries=2, base=5, label=f"{row['code']} 銘柄情報") or {}
    if info.get("quoteType") not in (None, "EQUITY"):
        return None
    ind = str(info.get("industry") or "")
    if "REIT" in ind.upper():
        return None
    row = row.copy()
    row["name"] = info.get("shortName") or info.get("longName") or row["code"]
    row["sector"] = YSECTOR_JA.get(info.get("sector"), info.get("sector") or "未分類")
    return row


GICS_JA = {"Consumer Staples": "生活必需品", "Utilities": "公益", "Health Care": "ヘルスケア", "Energy": "エネルギー",
           "Financials": "金融", "Information Technology": "情報技術", "Communication Services": "通信サービス",
           "Industrials": "資本財", "Materials": "素材", "Real Estate": "不動産", "Consumer Discretionary": "一般消費財"}


def load_us_universe() -> pd.DataFrame:
    log("米国株（S&P500）の一覧を取得")
    r = retry(lambda: requests.get(SP500_LIST, timeout=60), label="S&P500一覧")
    if r is None or r.status_code != 200:
        log("S&P500一覧を取得できませんでした。米国株を除いて続行します")
        return pd.DataFrame(columns=["code", "name", "market", "sector", "yft"])
    df = pd.read_csv(io.StringIO(r.text))
    df = df.rename(columns={"Symbol": "code", "Security": "name", "GICS Sector": "sector"})
    df["code"] = df["code"].astype(str).str.strip().str.upper()
    df["yft"] = df["code"].str.replace(".", "-", regex=False)          # BRK.B → BRK-B（Yahoo表記）
    df["sector"] = "米・" + df["sector"].map(GICS_JA).fillna("その他")
    df["market"] = "S&P500"
    df = df[["code", "name", "market", "sector", "yft"]].drop_duplicates("code").reset_index(drop=True)
    if LIMIT:
        df = df.head(LIMIT)
    log(f"米国 {len(df)} 銘柄")
    return df


# ---------- 2. 株価＋直近1年配当（一括） ----------
def load_prices_and_ttm(tickers):
    log(f"株価と直近配当を一括取得（{len(tickers)}銘柄）")
    out = {}
    cutoff = pd.Timestamp.now(tz=None) - pd.Timedelta(days=365)
    for i in range(0, len(tickers), 200):
        chunk = tickers[i:i + 200]
        data = retry(lambda: yf.download(chunk, period="13mo", group_by="ticker", actions=True,
                                         threads=True, progress=False, auto_adjust=False),
                     label=f"一括取得 {i}")
        if data is None or data.empty:
            continue
        for t in chunk:
            try:
                d = data[t] if isinstance(data.columns, pd.MultiIndex) else data
                close = d["Close"].dropna()
                if close.empty:
                    continue
                idx = d.index.tz_localize(None) if d.index.tz is not None else d.index
                divs = d["Dividends"].fillna(0) if "Dividends" in d else pd.Series(0, index=d.index)
                ttm = float(divs[idx >= cutoff].sum())
                out[t] = {"price": float(close.iloc[-1]), "ttm": ttm}
            except Exception:
                pass
        time.sleep(1.5)
    log(f"株価取得 {len(out)} 銘柄")
    return out


# ---------- 3. 財務（通過銘柄のみ） ----------
def pick(df, keys):
    if df is None or getattr(df, "empty", True):
        return None
    for k in keys:
        if k in df.index:
            return df.loc[k]
    return None


def fetch_financials(code: str, yft: str = None):
    yft = yft or f"{code}.T"
    cache = CACHE_DIR / (f"{code}.json" if yft.endswith(".T") else f"US_{yft}.json")
    if cache.exists():
        try:
            d = json.loads(cache.read_text())
            if (dt.date.today() - dt.date.fromisoformat(d["fetched"])).days < CACHE_DAYS:
                return d["years"]
        except Exception:
            pass

    def get():
        t = yf.Ticker(yft)
        return t.income_stmt, t.balance_sheet, t.cashflow, t.dividends

    res = retry(get, tries=3, base=8, label=f"{code} 財務")
    if res is None:
        return None
    inc, bs, cf, div = res
    if inc is None or inc.empty:
        return None

    sales = pick(inc, ["Total Revenue", "Operating Revenue"])
    op = pick(inc, ["Operating Income", "Total Operating Income As Reported"])
    eps = pick(inc, ["Diluted EPS", "Basic EPS"])
    ni = pick(inc, ["Net Income Common Stockholders", "Net Income"])
    equity = pick(bs, ["Stockholders Equity", "Common Stock Equity"])
    assets = pick(bs, ["Total Assets"])
    opcf = pick(cf, ["Operating Cash Flow", "Cash Flow From Continuing Operating Activities"])
    debt = pick(bs, ["Total Debt"])
    cash = pick(bs, ["Cash And Cash Equivalents", "Cash Cash Equivalents And Short Term Investments", "Cash Financial"])
    if div is not None and len(div):
        div = div.copy()
        if div.index.tz is not None:
            div.index = div.index.tz_localize(None)

    def val(s, c):
        if s is None:
            return None
        try:
            return clean(s[c])
        except Exception:
            return None

    years = []
    for c in sorted(inc.columns):
        fy = pd.Timestamp(c)
        e, a = val(equity, c), val(assets, c)
        y = {"year": fy.strftime("%Y/%m"), "sales": val(sales, c), "op": val(op, c), "eps": val(eps, c),
             "ni": val(ni, c), "equity": round(e / a * 100, 2) if e and a and a > 0 else None,
             "opcf": val(opcf, c), "cash": val(cash, c), "debt": val(debt, c), "dps": None, "price": None}
        if div is not None and len(div):
            # 権利落ち日が決算期末の前後にずれても同じ年度に入るよう±20日の余裕
            win = div[(div.index > fy - pd.DateOffset(years=1) + pd.Timedelta(days=20)) &
                      (div.index <= fy + pd.Timedelta(days=20))]
            y["dps"] = round(float(win.sum()), 2) if len(win) else None
        # Yahooは最古の年度を全項目空で返すことが多い。財務が1つもない年度は捨てる
        if all(y[k] is None for k in ("sales", "op", "eps", "ni", "equity", "opcf", "cash")):
            continue
        years.append(y)
    if not years:
        return None
    cache.write_text(json.dumps({"fetched": dt.date.today().isoformat(), "years": years}, ensure_ascii=False))
    return years


# ---------- 3b. 米国高配当ETF ----------
# 経費率は各運用会社の公表値の目安。購入前にSBI証券の銘柄ページで確認すること
ETFS = [("VYM", "バンガード 米国高配当株式ETF", 0.06),
        ("HDV", "iシェアーズ コア 米国高配当株ETF", 0.08),
        ("SPYD", "SPDR ポートフォリオS&P500高配当株式ETF", 0.07)]


def load_etfs():
    log("米国高配当ETFを取得")
    h = retry(lambda: yf.download([t for t, _, _ in ETFS], period="7y", group_by="ticker", actions=True,
                                  progress=False, auto_adjust=False), label="ETF")
    out = []
    if h is None or h.empty:
        return out
    this_year = dt.date.today().year
    cutoff = pd.Timestamp.now() - pd.Timedelta(days=365)
    for t, name, er in ETFS:
        try:
            d = h[t] if isinstance(h.columns, pd.MultiIndex) else h
            close = d["Close"].dropna()
            if close.empty:
                continue
            idx = d.index.tz_localize(None) if d.index.tz is not None else d.index
            div = pd.Series(d["Dividends"].fillna(0).values, index=idx)
            annual = div.groupby(div.index.year).sum()
            annual = annual[(annual.index < this_year) & (annual > 0)]          # 途中の年は除く
            dg = None
            if len(annual) >= 3:
                a = annual.iloc[-6:] if len(annual) >= 6 else annual
                dg = (a.iloc[-1] / a.iloc[0]) ** (1 / (len(a) - 1)) - 1
            out.append({"code": t, "name": name, "er": er, "price": round(float(close.iloc[-1]), 2),
                        "ttm": round(float(div[div.index >= cutoff].sum()), 4),
                        "annual": {str(k): round(float(v), 4) for k, v in annual.items()},
                        "dg": clean(round(dg * 100, 2)) if dg is not None else None})
        except Exception as e:
            log(f"{t} の処理に失敗: {e}")
    return out


# ---------- 4. 景気シグナル ----------
def load_macro():
    log("景気シグナルを取得")
    m = {}
    try:
        h = retry(lambda: yf.download(["1306.T", "^TNX", "JPY=X", "^VIX"], period="14mo",
                                      group_by="ticker", progress=False, auto_adjust=True), label="マクロ")
        if h is None or h.empty:
            return None
        tp = h["1306.T"]["Close"].dropna()
        if len(tp) > 210:
            ma200 = tp.rolling(200).mean().iloc[-1]
            ret = tp.pct_change().dropna()
            m["topix_vs_ma200"] = round((tp.iloc[-1] / ma200 - 1) * 100, 2)
            m["topix_6m"] = round((tp.iloc[-1] / tp.iloc[-126] - 1) * 100, 2)
            m["topix_vol"] = round(float(ret.iloc[-60:].std() * (252 ** 0.5) * 100), 1)
        for key, t in [("us10y", "^TNX"), ("usdjpy", "JPY=X"), ("vix", "^VIX")]:
            s = h[t]["Close"].dropna()
            if len(s) > 130:
                m[key] = round(float(s.iloc[-1]), 2)
                m[key + "_6m_chg"] = round(float(s.iloc[-1] - s.iloc[-126]), 2)
    except Exception as e:
        log("マクロ取得失敗", e)
    return {k: clean(v) for k, v in m.items()} or None


# ---------- 5. main ----------
def main():
    t0 = time.time()
    parts = []
    if "JP" in COUNTRIES:
        jp = load_universe()
        jp["yft"] = jp["code"] + ".T"
        jp["country"], jp["currency"], jp["min_y"] = "JP", "JPY", MIN_YIELD
        parts.append(jp)
    if "US" in COUNTRIES and US_STOCKS:
        us = load_us_universe()
        us["country"], us["currency"], us["min_y"] = "US", "USD", MIN_YIELD_US
        parts.append(us)
    uni = pd.concat(parts, ignore_index=True) if parts else pd.DataFrame()
    if uni.empty:
        raise SystemExit("対象銘柄がありません。COUNTRIES の設定を確認してください。")
    px = load_prices_and_ttm(uni["yft"].tolist())

    # 一次選別：実績利回りが基準未満の銘柄は財務を取りに行かない
    def passes(r):
        p = px.get(r["yft"])
        return bool(p and p["price"] > 0 and p["ttm"] / p["price"] * 100 >= r["min_y"])
    pre = uni[uni.apply(passes, axis=1)].reset_index(drop=True)
    log(f"一次選別 {len(pre)} / {len(uni)} 銘柄")

    out = []
    for i, row in pre.iterrows():
        if str(row.get("need_info", "")) == "True":          # 総当たりで見つけた銘柄だけ（空欄・NaNは対象外）
            row = fill_info(row)
            if row is None:
                continue
        years = fetch_financials(row["code"], row["yft"])
        time.sleep(0.6 + random.random() * 0.4)
        if not years:
            continue
        p = px[row["yft"]]
        last = years[-1]
        last["price"] = round(p["price"], 2)
        ttm_yield = p["ttm"] / p["price"] * 100
        prev = years[-2]["dps"] if len(years) >= 2 else None
        # 記念配当・データ異常の疑い（極端な利回り、前年比2.5倍超）
        suspect = ttm_yield > 10 or bool(prev and last["dps"] and last["dps"] > prev * 2.5)
        out.append({"code": row["code"], "name": row["name"], "sector": row["sector"], "market": row["market"],
                    "country": row["country"], "currency": row["currency"],
                    "ttm_dps": round(p["ttm"], 4), "suspect": suspect, "years": years})
        if i % 50 == 0:
            log(f"{i}/{len(pre)} 処理中（収録 {len(out)}）")

    out.sort(key=lambda s: -(s["ttm_dps"] / s["years"][-1]["price"]))
    counts = {c: sum(1 for s in out if s["country"] == c) for c in ["JP"] + (["US"] if US_STOCKS else [])}
    OUT.parent.mkdir(exist_ok=True)
    OUT.write_text(json.dumps({
        "updated": dt.datetime.now(JST).strftime("%Y-%m-%d %H:%M"),
        "source": "Yahoo Finance (yfinance) / JPX上場銘柄一覧" + (" / S&P500構成銘柄" if US_STOCKS else ""),
        "min_yield": MIN_YIELD, "min_yield_us": MIN_YIELD_US if US_STOCKS else None, "universe": len(uni), "count": len(out),
        "counts": counts, "universe_source": UNIVERSE_SOURCE, "macro": load_macro(), "etfs": load_etfs() if "US" in COUNTRIES else [], "stocks": out,
    }, ensure_ascii=False, allow_nan=False))
    log(f"完了: {len(out)} 銘柄を保存 {counts}（{(time.time() - t0) / 60:.0f}分）")


if __name__ == "__main__":
    main()
