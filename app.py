import math
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import yfinance as yf
import pandas as pd
import feedparser  # pip install feedparser

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
  allow_origins=[
    "http://localhost:5500",
    "https://quantum-alpha-radar.netlify.app",
],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ============================================================================
# STATIC REFERENCE DATA
# ============================================================================

# IMPORTANT: NSE revises F&O lot sizes roughly every 6 months (based on a
# minimum contract value). Keep this updated from the official NSE lot-size
# circular (search "F&O Lot Size" on nseindia.com). Any ticker NOT in this
# dict will be scanned for signals but will show "Unverified lot size"
# instead of a fabricated position size.
LOT_SIZES = {
    "^NSEI": 65, "^BSESN": 20, "RELIANCE.NS": 250, "HDFCBANK.NS": 550,
    "ICICIBANK.NS": 700, "SBIN.NS": 750, "INFY.NS": 400, "TCS.NS": 175,
    "BHARTIARTL.NS": 950, "LT.NS": 300
}

CORE_INDEX_SYMBOLS = ["^NSEI", "^BSESN"]  # always scanned/shown, compulsory

# Fallback NIFTY 50 universe used if the live Wikipedia scrape fails or
# returns a malformed table. Not a substitute for live constituents, just a
# safety net so the app never returns an empty scan.
NIFTY50_FALLBACK = [
    "RELIANCE.NS", "TCS.NS", "HDFCBANK.NS", "ICICIBANK.NS", "INFY.NS", "BHARTIARTL.NS",
    "ITC.NS", "LT.NS", "KOTAKBANK.NS", "AXISBANK.NS", "SBIN.NS", "BAJFINANCE.NS",
    "HINDUNILVR.NS", "ASIANPAINT.NS", "MARUTI.NS", "SUNPHARMA.NS", "TITAN.NS",
    "ULTRACEMCO.NS", "M&M.NS", "NESTLEIND.NS", "WIPRO.NS", "ADANIENT.NS", "TATASTEEL.NS",
    "POWERGRID.NS", "NTPC.NS", "JSWSTEEL.NS", "HCLTECH.NS", "TECHM.NS", "BAJAJFINSV.NS",
    "ONGC.NS", "COALINDIA.NS", "GRASIM.NS", "INDUSINDBK.NS", "DRREDDY.NS", "CIPLA.NS",
    "EICHERMOT.NS", "BRITANNIA.NS", "APOLLOHOSP.NS", "DIVISLAB.NS", "HEROMOTOCO.NS",
    "TMPV.NS", "TATACONSUM.NS", "BAJAJ-AUTO.NS", "ADANIPORTS.NS", "HINDALCO.NS",
    "SBILIFE.NS", "HDFCLIFE.NS", "BPCL.NS", "SHRIRAMFIN.NS", "LTIM.NS"
]

# 100% Comprehensive Mapping: ALL 10 Main Trading Sectors on NSE India with 10 Core Stocks each
SECTOR_MAP = {
    "NIFTY BANK": {
        "index": "^NSEBANK",
        "stocks": ["HDFCBANK.NS", "ICICIBANK.NS", "AXISBANK.NS", "KOTAKBANK.NS", "SBIN.NS", "PNB.NS", "BANKBARODA.NS", "INDUSINDBK.NS", "FEDERALBNK.NS", "AUBANK.NS"]
    },
    "NIFTY IT": {
        "index": "^CNXIT",
        "stocks": ["TCS.NS", "INFY.NS", "WIPRO.NS", "HCLTECH.NS", "TECHM.NS", "LTIM.NS", "PERSISTENT.NS", "COFORGE.NS", "MPHASIS.NS", "KPITTECH.NS"]
    },
    "NIFTY AUTO": {
        "index": "^CNXAUTO",
        "stocks": ["TMPV.NS", "MARUTI.NS", "M&M.NS", "BAJAJ-AUTO.NS", "HEROMOTOCO.NS", "EICHERMOT.NS", "TVSMOTOR.NS", "BOSCHLTD.NS", "MRF.NS", "BHARATFORG.NS"]
    },
    "NIFTY FMCG": {
        "index": "^CNXFMCG",
        "stocks": ["ITC.NS", "HINDUNILVR.NS", "NESTLEIND.NS", "BRITANNIA.NS", "TATACONSUM.NS", "VBL.NS", "GODREJCP.NS", "DABUR.NS", "MARICO.NS", "COLPAL.NS"]
    },
    "NIFTY METAL": {
        "index": "^CNXMETAL",
        "stocks": ["TATASTEEL.NS", "HINDALCO.NS", "JSWSTEEL.NS", "VEDL.NS", "NATIONALUM.NS", "COALINDIA.NS", "NMDC.NS", "SAIL.NS", "JINDALSTEL.NS", "APLAPOLLO.NS"]
    },
    "NIFTY PHARMA": {
        "index": "^CNXPHARMA",
        "stocks": ["SUNPHARMA.NS", "CIPLA.NS", "DRREDDY.NS", "DIVISLAB.NS", "LUPIN.NS", "AUROPHARMA.NS", "ZYDUSLIFE.NS", "BIOCON.NS", "ALKEM.NS", "TORNTPHARM.NS"]
    },
    "NIFTY ENERGY": {
        "index": "^CNXENERGY",
        "stocks": ["RELIANCE.NS", "NTPC.NS", "POWERGRID.NS", "ONGC.NS", "BPCL.NS", "IOC.NS", "GAIL.NS", "TATAPOWER.NS", "ADANIGREEN.NS", "RECLTD.NS"]
    },
    "NIFTY REALTY": {
        "index": "^CNXREALTY",
        "stocks": ["DLF.NS", "GODREJPROP.NS", "OBEROIRLTY.NS", "LODHA.NS", "PRESTIGE.NS", "PHOENIXLTD.NS", "SOBHA.NS", "BRIGADE.NS", "MAHLIFE.NS", "SUNTECK.NS"]
    },
    "NIFTY PSU BANK": {
        "index": "^CNXPSUBANK",
        "stocks": ["SBIN.NS", "BANKBARODA.NS", "PNB.NS", "CANBK.NS", "UNIONBANK.NS", "IOB.NS", "INDIANB.NS", "CENTRALBK.NS", "UCOBANK.NS", "MAHABANK.NS"]
    },
    "NIFTY INFRA": {
        "index": "^CNXINFRA",
        "stocks": ["LT.NS", "BHARTIARTL.NS", "ADANIPORTS.NS", "ULTRACEMCO.NS", "GRASIM.NS", "AMBUJACEM.NS", "INDIGO.NS", "GMRAIRPORT.NS", "IRB.NS", "HINDZINC.NS"]
    }
}

# ============================================================================
# SHARED HELPERS
# ============================================================================

def get_nifty50_tickers():
    """
    Live NIFTY 50 constituent list scraped from Wikipedia, matched by column
    name (not table position, which shifts whenever the page layout changes).
    Falls back to a hardcoded list on any failure so downstream endpoints
    never silently return empty results.
    """
    try:
        tables = pd.read_html('https://en.wikipedia.org/wiki/NIFTY_50')
        for t in tables:
            if 'Symbol' in t.columns:
                syms = [str(s).strip() + ".NS" for s in t['Symbol'].tolist() if isinstance(s, str) and s.strip()]
                if len(syms) >= 40:
                    return syms
    except Exception:
        pass
    return NIFTY50_FALLBACK


def _next_weekday(target_weekday, from_date):
    """target_weekday: Monday=0 ... Sunday=6. Returns the next date >= from_date matching that weekday."""
    days_ahead = (target_weekday - from_date.weekday()) % 7
    return from_date + timedelta(days=days_ahead)


def _last_weekday_of_month(target_weekday, year, month):
    if month == 12:
        first_of_next = datetime(year + 1, 1, 1)
    else:
        first_of_next = datetime(year, month + 1, 1)
    last_day = first_of_next - timedelta(days=1)
    offset = (last_day.weekday() - target_weekday) % 7
    return last_day - timedelta(days=offset)


def get_nearest_expiry(symbol):
    """
    Returns (expiry_datetime, days_to_expiry) using current (2026) NSE/BSE
    expiry-day rules:
      - NIFTY (^NSEI): weekly expiry every Tuesday, monthly = last Tuesday
      - SENSEX (^BSESN): weekly expiry every Thursday, monthly = last Thursday
      - Individual stocks: monthly only (NSE), last Tuesday of the month
    NSE/BSE have changed these rules more than once (most recently Sept 2025).
    Verify against the live NSE/BSE circular if this ever stops matching
    what you see on the exchange.
    """
    now = datetime.now()
    if symbol == "^NSEI":
        expiry = _next_weekday(1, now)  # Tuesday
    elif symbol == "^BSESN":
        expiry = _next_weekday(3, now)  # Thursday
    else:
        expiry = _last_weekday_of_month(1, now.year, now.month)  # last Tuesday
        if expiry.date() < now.date():
            nm, ny = (now.month % 12) + 1, now.year + (1 if now.month == 12 else 0)
            expiry = _last_weekday_of_month(1, ny, nm)
    days_to_expiry = max((expiry.date() - now.date()).days, 0)
    return expiry, days_to_expiry


def _norm_cdf(x):
    return 0.5 * (1 + math.erf(x / math.sqrt(2)))


def black_scholes_price(S, K, T, r, sigma, option_type="CE"):
    """Estimated fair premium. This is a MODEL price off historical
    volatility, not a live quote from the option chain — real premiums also
    reflect open interest, skew and order flow that yfinance doesn't expose."""
    T = max(T, 0.0009)      # floor ~ a few hours, avoids div-by-zero on expiry day
    sigma = max(sigma, 0.05)
    d1 = (math.log(S / K) + (r + 0.5 * sigma ** 2) * T) / (sigma * math.sqrt(T))
    d2 = d1 - sigma * math.sqrt(T)
    if option_type == "CE":
        price = S * _norm_cdf(d1) - K * math.exp(-r * T) * _norm_cdf(d2)
    else:
        price = K * math.exp(-r * T) * _norm_cdf(-d2) - S * _norm_cdf(-d1)
    return max(round(price, 2), 0.05)


def estimate_annualized_volatility(df, price_col='Close'):
    """Realized volatility from 5-minute bars, annualized. Used as a stand-in
    for implied volatility since yfinance has no reliable NSE option chain."""
    returns = df[price_col].pct_change().dropna()
    if len(returns) < 15:
        return 0.18
    bars_per_year = 75 * 252  # ~75 five-min bars/trading day * 252 trading days
    sigma = returns.std() * math.sqrt(bars_per_year)
    return min(max(sigma, 0.08), 1.2)  # clip to a sane 8%-120% band


# ============================================================================
# 1. OPTION SCALPING MATRIX (dynamic premiums, dynamic strike, real expiry)
# ============================================================================

def analyze_market_matrix(ticker_symbol):
    try:
        ticker = yf.Ticker(ticker_symbol)
        df = ticker.history(period="5d", interval="5m")
    except Exception:
        return {"symbol": ticker_symbol.replace(".NS", ""), "signal": "NO DATA"}

    if df.empty or len(df) < 30:
        return {"symbol": ticker_symbol.replace(".NS", ""), "signal": "NO DATA"}

    df['EMA_Fast'] = df['Close'].ewm(span=9, adjust=False).mean()
    df['EMA_Slow'] = df['Close'].ewm(span=21, adjust=False).mean()

    high_low = df['High'] - df['Low']
    high_cp = (df['High'] - df['Close'].shift()).abs()
    low_cp = (df['Low'] - df['Close'].shift()).abs()
    tr = pd.concat([high_low, high_cp, low_cp], axis=1).max(axis=1)
    df['ATR'] = tr.rolling(window=14).mean()

    delta = df['Close'].diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
    rs = gain / (loss + 1e-10)
    df['RSI'] = 100 - (100 / (1 + rs))
    df['Vol_MA'] = df['Volume'].rolling(window=20).mean()

    latest = df.iloc[-1]
    current_price = round(latest['Close'], 2)
    rsi = latest['RSI'] if not pd.isna(latest['RSI']) else 50.0
    volume_burst = bool(not pd.isna(latest['Vol_MA']) and latest['Volume'] > (latest['Vol_MA'] * 1.2))

    ema_diff_pct = ((latest['EMA_Fast'] - latest['EMA_Slow']) / current_price) * 100
    momentum_score = round(ema_diff_pct + (rsi - 50) / 10, 2)

    signal, option_type, strike_price = "NO SIGNAL", "", 0

    if (latest['EMA_Fast'] > latest['EMA_Slow']) and (50 < rsi < 68) and volume_burst:
        signal, option_type = "BUY / CALL", "CE"
    elif (latest['EMA_Fast'] < latest['EMA_Slow']) and (32 < rsi < 50) and volume_burst:
        signal, option_type = "SELL / PUT", "PE"

    expiry_date = None
    option_entry = option_sl = option_target = 0
    quantity_display = "-"

    if signal != "NO SIGNAL":
        if ticker_symbol == "^NSEI":
            strike_base = 50
        elif ticker_symbol == "^BSESN":
            strike_base = 100
        else:
            # Rough dynamic strike gap scaled to price. Real NSE strike
            # intervals are set per-stock by the exchange — cross-check
            # against the contract specification if this looks off.
            strike_base = max(round(current_price * 0.01 / 5) * 5, 5)
        strike_price = int(round(current_price / strike_base) * strike_base)

        expiry_date, days_to_expiry = get_nearest_expiry(ticker_symbol)
        T = days_to_expiry / 365
        sigma = estimate_annualized_volatility(df)
        r = 0.065  # approx risk-free rate, update periodically

        option_entry = black_scholes_price(current_price, strike_price, T, r, sigma, option_type)

        # Size SL/target off the option premium's own expected move, not the
        # underlying's ATR (options don't move points-for-point with spot).
        premium_move_proxy = max(option_entry * sigma * math.sqrt(1 / 365) * 2, option_entry * 0.08)
        option_sl_points = round(premium_move_proxy * 0.9, 2)
        option_target_points = round(premium_move_proxy * 1.8, 2)
        option_sl = max(round(option_entry - option_sl_points, 2), 0.05)
        option_target = round(option_entry + option_target_points, 2)

        lot_size = LOT_SIZES.get(ticker_symbol)
        if lot_size:
            allowed_qty = int(1000 / max(option_sl_points, 0.5))
            calculated_lots = max(1, int(allowed_qty / lot_size))
            final_qty = calculated_lots * lot_size
            if (final_qty * option_entry) > 50000:
                calculated_lots = max(1, int(50000 / (option_entry * lot_size)))
                final_qty = calculated_lots * lot_size
            quantity_display = f"{final_qty} shares ({calculated_lots} lot)"
        else:
            quantity_display = "Unverified lot size — check NSE"

    return {
        "symbol": ticker_symbol.replace(".NS", ""),
        "current_price": current_price,
        "signal": signal,
        "momentum_score": momentum_score,
        "option_name": f"{strike_price} {option_type}" if signal != "NO SIGNAL" else "-",
        "expiry": expiry_date.strftime("%d-%b-%Y") if expiry_date else "-",
        "option_entry": option_entry,
        "option_sl": option_sl,
        "option_target": option_target,
        "quantity_display": quantity_display,
    }


# ============================================================================
# 2. DYNAMIC POSITIONAL ENGINE
# ============================================================================

def analyze_positional_stock(ticker_symbol):
    try:
        ticker = yf.Ticker(ticker_symbol)
        df = ticker.history(period="1y", interval="1d")
        if df.empty or len(df) < 60:
            return None

        current_price = round(df['Close'].iloc[-1], 2)
        sma_50 = df['Close'].rolling(window=50).mean().iloc[-1]
        sma_200 = df['Close'].rolling(window=200).mean().iloc[-1] if len(df) >= 200 else sma_50

        earnings_growth = None  # unknown by default, doesn't penalize the stock
        try:
            qf = ticker.quarterly_financials
            if not qf.empty and "Net Income" in qf.index and qf.shape[1] >= 2:
                earnings_growth = qf.loc["Net Income"].iloc[0] > qf.loc["Net Income"].iloc[1]
        except Exception:
            pass

        atr_daily = (df['High'] - df['Low']).rolling(window=14).mean().iloc[-1]
        if pd.isna(atr_daily) or atr_daily <= 0:
            return None

        trend_up = current_price > sma_50
        long_term_up = current_price > sma_200

        if trend_up and earnings_growth is not False:
            verdict = "ACCUMULATE"
        elif trend_up:
            verdict = "ACCUMULATE (earnings data unavailable)"
        elif long_term_up:
            verdict = "WATCH"
        else:
            return None  # genuinely weak setup right now

        buying_range = f"₹{round(current_price * 0.99, 1)} - ₹{round(current_price * 1.01, 1)}"
        return {
            "symbol": ticker_symbol.replace(".NS", ""), "current_price": current_price,
            "verdict": verdict, "horizon": "1 Month", "buying_range": buying_range,
            "stop_loss": round(current_price - (2.2 * atr_daily), 2),
            "target": round(current_price + (4.5 * atr_daily), 2)
        }
    except Exception:
        return None


# ============================================================================
# 3. ACCELERATED SECTOR MATRIX ENGINE
# ============================================================================

def calculate_all_sectors():
    sector_results = []
    for sector_name, meta in SECTOR_MAP.items():
        try:
            idx = yf.Ticker(meta["index"])
            idx_hist = idx.history(period="2d", interval="5m")
            if idx_hist.empty or len(idx_hist) < 2:
                continue

            idx_prev = idx_hist['Close'].iloc[0]
            idx_curr = idx_hist['Close'].iloc[-1]
            idx_change = round(((idx_curr - idx_prev) / idx_prev) * 100, 2)

            stock_data_list = []
            for stock_sym in meta["stocks"]:
                stk = yf.Ticker(stock_sym)
                stk_hist = stk.history(period="2d", interval="5m")
                if stk_hist.empty:
                    continue
                s_open = stk_hist['Close'].iloc[0]
                s_current = stk_hist['Close'].iloc[-1]
                s_change = round(((s_current - s_open) / s_open) * 100, 2)

                stock_data_list.append({
                    "ticker": stock_sym.replace(".NS", ""),
                    "price": round(s_current, 2),
                    "return_pct": s_change
                })

            stock_data_list = sorted(stock_data_list, key=lambda k: k['return_pct'], reverse=True)

            sector_results.append({
                "sector_name": sector_name,
                "index_performance": idx_change,
                "constituents": stock_data_list
            })
        except Exception:
            pass

    return sorted(sector_results, key=lambda k: k['index_performance'], reverse=True)


# ============================================================================
# 4. MARKET NEWS & SENTIMENT (keyword-based — a rough signal, not real NLP)
# ============================================================================

NEWS_FEEDS = [
    {"name": "Economic Times Markets", "url": "https://economictimes.indiatimes.com/markets/rssfeeds/1977021501.cms"},
    {"name": "Moneycontrol Markets", "url": "https://www.moneycontrol.com/rss/marketreports.xml"},
    {"name": "Business Standard Markets", "url": "https://www.business-standard.com/rss/markets-106.rss"},
    {"name": "Livemint Markets", "url": "https://www.livemint.com/rss/markets"},
]

POSITIVE_KEYWORDS = [
    "rally", "surge", "jump", "gain", "record high", "all-time high", "upgrade",
    "beats estimates", "beat estimates", "bullish", "outperform", "strong growth",
    "robust", "upbeat", "optimis", "buyback", "rate cut", "stimulus", "fii inflow",
    "fii buying", "recovery", "rebound", "soar",
]
NEGATIVE_KEYWORDS = [
    "crash", "plunge", "tumble", "selloff", "sell-off", "slump", "downgrade",
    "bearish", "recession", "war", "conflict", "tension", "inflation surge",
    "rate hike", "default", "fraud", "scam", "fii outflow", "fii selling", "weak",
    "miss estimates", "missed estimates", "layoff", "ban", "sanction", "crisis",
    "volatility spike", "panic", "slowdown",
]
IMPACT_TAGS = {
    "RBI Policy": ["rbi", "repo rate", "monetary policy", "reserve bank"],
    "Global Cues": ["fed", "federal reserve", "wall street", "dow jones", "nasdaq", "crude oil", "dollar index"],
    "Geopolitical": ["war", "tension", "sanction", "border", "geopolitic"],
    "Earnings": ["q1 results", "q2 results", "q3 results", "q4 results", "quarterly results", "earnings"],
    "FII/DII Flow": ["fii", "dii", "foreign investor", "institutional"],
    "Currency/Commodity": ["rupee", "dollar", "gold", "crude", "silver"],
}


def classify_sentiment(text):
    t = text.lower()
    pos_hits = sum(1 for k in POSITIVE_KEYWORDS if k in t)
    neg_hits = sum(1 for k in NEGATIVE_KEYWORDS if k in t)
    if pos_hits > neg_hits:
        return "Positive"
    if neg_hits > pos_hits:
        return "Negative"
    return "Neutral"


def tag_impact_category(text):
    t = text.lower()
    tags = [name for name, kws in IMPACT_TAGS.items() if any(k in t for k in kws)]
    return tags if tags else ["General Market"]


def fetch_market_news(limit_per_feed=8):
    items = []
    for feed in NEWS_FEEDS:
        try:
            parsed = feedparser.parse(feed["url"])
            for entry in parsed.entries[:limit_per_feed]:
                title = entry.get("title", "")
                summary = entry.get("summary", "")
                combined = f"{title} {summary}"
                items.append({
                    "source": feed["name"],
                    "title": title,
                    "link": entry.get("link", ""),
                    "published": entry.get("published", entry.get("updated", "")),
                    "sentiment": classify_sentiment(combined),
                    "impact_tags": tag_impact_category(combined),
                })
        except Exception:
            continue
    return items


# ============================================================================
# API ROUTES
# ============================================================================

@app.get("/api/signals")
def get_market_signals():
    universe = CORE_INDEX_SYMBOLS + get_nifty50_tickers()
    seen, ordered_universe = set(), []
    for s in universe:
        if s not in seen:
            seen.add(s)
            ordered_universe.append(s)

    results = []
    with ThreadPoolExecutor(max_workers=10) as executor:
        future_map = {executor.submit(analyze_market_matrix, sym): sym for sym in ordered_universe}
        for future in as_completed(future_map):
            symbol = future_map[future]
            try:
                res = future.result()
            except Exception:
                continue
            if not res or res.get("signal") == "NO DATA":
                continue
            is_core = symbol in CORE_INDEX_SYMBOLS
            # Compulsory indices always show, even with NO SIGNAL. Everything
            # else only shows if it currently has real momentum — no static list.
            if is_core or res.get("signal") != "NO SIGNAL":
                results.append(res)

    results.sort(key=lambda r: (r["symbol"] not in ("^NSEI", "^BSESN"), -abs(r.get("momentum_score") or 0)))
    return results


@app.get("/api/positional")
def get_positional_signals():
    tickers = get_nifty50_tickers()
    results = []
    with ThreadPoolExecutor(max_workers=10) as executor:
        future_map = {executor.submit(analyze_positional_stock, t): t for t in tickers}
        for future in as_completed(future_map):
            try:
                data = future.result()
            except Exception:
                data = None
            if data:
                results.append(data)
    results.sort(key=lambda r: 0 if r["verdict"].startswith("ACCUMULATE") else 1)
    return results


@app.get("/api/sectors")
def get_sector_performance_api():
    return calculate_all_sectors()


@app.get("/api/news")
def get_market_news():
    news = fetch_market_news()
    positive = [n for n in news if n["sentiment"] == "Positive"]
    negative = [n for n in news if n["sentiment"] == "Negative"]
    neutral = [n for n in news if n["sentiment"] == "Neutral"]
    return {
        "summary": {
            "positive_count": len(positive),
            "negative_count": len(negative),
            "neutral_count": len(neutral),
        },
        "positive": positive,
        "negative": negative,
        "neutral": neutral,
        "all": news,
    }


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=8000)
