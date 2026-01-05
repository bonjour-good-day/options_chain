import os
import pandas as pd
import yfinance as yf
from datetime import date, timedelta
from alpaca.trading.client import TradingClient
from alpaca.data.historical.option import OptionHistoricalDataClient
from alpaca.data.requests import OptionSnapshotRequest
from alpaca.trading.requests import GetOptionContractsRequest
from alpaca.trading.enums import AssetStatus

# Alpaca API keys
API_KEY = os.getenv("ALPACA_API_KEY")
SECRET_KEY = os.getenv("ALPACA_SECRET_KEY")

# Initialize Alpaca clients
trading_client = TradingClient(API_KEY, SECRET_KEY, paper=True)
option_client = OptionHistoricalDataClient(API_KEY, SECRET_KEY)

def get_current_price_yf(ticker: str) -> float:
    """Fetch the latest market price from Yahoo Finance."""
    try:
        ticker_obj = yf.Ticker(ticker)
        price = ticker_obj.fast_info.last_price
        if price is None or price <= 0:
            price = ticker_obj.history(period="1d")["Close"].iloc[-1]
        return float(price)
    except Exception as e:
        print(f"⚠️ Failed to fetch current price for {ticker}: {e}")
        return 90.0  # fallback value

def get_options_chain_snapshot(ticker: str, exp_days: int = 30, strike_pct: float = 0.1) -> pd.DataFrame:
    """Return an options chain snapshot for a given ticker."""
    current_price = get_current_price_yf(ticker)
    print(f"💰 {ticker} current price: {current_price:.2f}")

    strike_low = str(current_price * (1 - strike_pct))
    strike_high = str(current_price * (1 + strike_pct))
    exp_max = date.today() + timedelta(days=exp_days)

    all_contracts = []
    req = GetOptionContractsRequest(
        underlying_symbols=[ticker],
        status=AssetStatus.ACTIVE,
        expiration_date_lte=exp_max,
        strike_price_gte=strike_low,
        strike_price_lte=strike_high
    )

    page_token = None
    while True:
        if page_token:
            req.page_token = page_token

        result = trading_client.get_option_contracts(req)
        if isinstance(result, tuple):
            contracts, page_token = result
        else:
            contracts = result
            page_token = None

        all_contracts.extend(contracts.option_contracts)
        if not page_token:
            break

    print(f"Found {len(all_contracts)} contracts for {ticker}")
    if not all_contracts:
        return pd.DataFrame()

    symbols = [c.symbol for c in all_contracts[:50]]  # Limit for free tier
    all_snaps = {}

    for i in range(0, len(symbols), 20):
        batch = symbols[i:i + 20]
        try:
            snaps = option_client.get_option_snapshot(OptionSnapshotRequest(symbol_or_symbols=batch))
            all_snaps.update(snaps)
        except Exception as e:
            print(f"⚠️ Batch {i//20} failed for {ticker}: {e}")

    data = []
    now = pd.Timestamp.now().tz_localize(None)
    today = date.today()

    for contract in all_contracts:
        snap = all_snaps.get(contract.symbol)
        bid = ask = last_price = volume = open_interest = iv = 0.0

        if snap:
            if getattr(snap, 'latest_quote', None):
                bid = snap.latest_quote.bid_price or 0.0
                ask = snap.latest_quote.ask_price or 0.0
            if getattr(snap, 'latest_trade', None):
                last_price = snap.latest_trade.price or 0.0
            volume = getattr(contract, 'volume', 0) or getattr(snap, 'volume', 0)
            open_interest = getattr(contract, 'open_interest', 0) or getattr(snap, 'open_interest', 0)
            iv = getattr(snap, 'implied_volatility', 0.0)

        data.append({
            'ticker': ticker,
            'option_code': contract.symbol,
            'strike': float(contract.strike_price),
            'exp_date': contract.expiration_date,
            'type': contract.type.value.upper(),
            'bid': bid,
            'ask': ask,
            'last_price': last_price,
            'mid_price': (bid + ask) / 2 if bid > 0 or ask > 0 else 0.0,
            'volume': int(volume),
            'open_interest': int(open_interest),
            'iv': iv,
            'timestamp': now,
            'snapshot_date': today
        })

    return pd.DataFrame(data, columns=[
        'ticker', 'option_code', 'strike', 'exp_date', 'type',
        'bid', 'ask', 'last_price', 'mid_price', 'volume',
        'open_interest', 'iv', 'timestamp', 'snapshot_date'
    ])

def fetch_multiple_tickers(ticker_list, exp_days=45, strike_pct=0.3):
    """Iterate through tickers and save options chains to CSV."""
    for ticker in ticker_list:
        print(f"\n🔄 Fetching {ticker} options chain...")
        df = get_options_chain_snapshot(ticker, exp_days=exp_days, strike_pct=strike_pct)
        if df.empty:
            print(f"❌ No data for {ticker}.")
            continue

        filename = f"{ticker}_options_{pd.Timestamp.now().strftime('%Y%m%d_%H%M')}.csv"
        df.to_csv(filename, index=False)
        print(f"✅ Saved {len(df)} contracts to {filename}")

if __name__ == "__main__":
    tickers = ["CALM", "RKLB", "NFLX", "UBER", "ONDS", "V"]
    fetch_multiple_tickers(tickers)
