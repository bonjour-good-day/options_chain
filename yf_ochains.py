"""
Yahoo Finance Options Chain Data Fetcher (100% FREE) - MULTI TICKER VERSION
====================================================================
- Supports multiple tickers
- Saves each ticker into its own CSV
- Filename format: TICKER_YYYYMMDD_HHMMSS.csv
- All CSVs saved to: /xxx/yfoc

Installation:
pip install yfinance pandas duckdb
"""

import yfinance as yf
import pandas as pd
from datetime import datetime, timedelta
import time
import os


class YahooOptionsChainFetcher:
    def __init__(self):
        """Initialize Yahoo Finance options fetcher - No credentials needed!"""
        pass

    def get_option_chain(self, ticker, max_expiry_days=45):
        """
        Fetch complete options chain for a ticker
        """
        try:
            stock = yf.Ticker(ticker)

            expirations = stock.options
            if not expirations:
                print(f"No options data available for {ticker}")
                return pd.DataFrame()

            if max_expiry_days is not None:
                cutoff_date = datetime.now() + timedelta(days=max_expiry_days)
                expirations = [
                    d for d in expirations
                    if datetime.strptime(d, "%Y-%m-%d") <= cutoff_date
                ]

            print(f"Found {len(expirations)} expiration dates for {ticker}")

            all_options = []

            for exp_date in expirations:
                try:
                    print(f"  -> Fetching expiration {exp_date}")

                    opt = stock.option_chain(exp_date)

                    calls = opt.calls.copy()
                    calls["type"] = "CALL"
                    calls["exp_date"] = exp_date

                    puts = opt.puts.copy()
                    puts["type"] = "PUT"
                    puts["exp_date"] = exp_date

                    options = pd.concat([calls, puts], ignore_index=True)

                    options["ticker"] = ticker
                    options["timestamp"] = datetime.now()
                    options["snapshot_date"] = datetime.now().date()

                    options = options.rename(columns={
                        "contractSymbol": "option_code",
                        "lastPrice": "last_price",
                        "openInterest": "open_interest",
                        "impliedVolatility": "iv"
                    })

                    options["mid_price"] = (options["bid"] + options["ask"]) / 2

                    columns = [
                        "ticker", "option_code", "strike", "exp_date", "type",
                        "bid", "ask", "last_price", "mid_price",
                        "volume", "open_interest", "iv",
                        "timestamp", "snapshot_date"
                    ]

                    available_cols = [c for c in columns if c in options.columns]
                    options = options[available_cols]

                    all_options.append(options)

                    time.sleep(0.2)

                except Exception as e:
                    print(f"    ❌ Error fetching {exp_date}: {e}")
                    continue

            if not all_options:
                return pd.DataFrame()

            df = pd.concat(all_options, ignore_index=True)

            df = df.dropna(subset=["strike"])
            df["iv"] = df["iv"].fillna(0)
            df["volume"] = df["volume"].fillna(0).astype(int)
            df["open_interest"] = df["open_interest"].fillna(0).astype(int)

            print(f"✅ {ticker}: {len(df)} options fetched "
                  f"(Calls: {len(df[df['type'] == 'CALL'])}, "
                  f"Puts: {len(df[df['type'] == 'PUT'])})")

            return df

        except Exception as e:
            print(f"❌ Error fetching options chain for {ticker}: {e}")
            return pd.DataFrame()

    def save_to_csv(self, df, filename, output_dir):
        """Save options data to CSV in specified directory"""
        os.makedirs(output_dir, exist_ok=True)
        full_path = os.path.join(output_dir, filename)
        df.to_csv(full_path, index=False)
        print(f"💾 Saved {len(df)} records to {full_path}")


# =========================================================
# CONFIGURATION – DEFINE YOUR TICKERS HERE
# =========================================================

TICKERS = [
    "CALM",
    "ONDS",
    "RKLB",
    "UBER",
    "NFLX",
    "V"
]

MAX_EXPIRY_DAYS = 45   # change to None for all expiries

OUTPUT_DIR = "/Users/monpabi/dev/financial_news/data_scraper/options/yfoc"


# =========================================================
# MAIN EXECUTION
# =========================================================

def run_multi_ticker_scrape():
    fetcher = YahooOptionsChainFetcher()

    for ticker in TICKERS:
        print("\n" + "=" * 70)
        print(f"Fetching options chain for {ticker}")
        print("=" * 70)

        df = fetcher.get_option_chain(ticker, max_expiry_days=MAX_EXPIRY_DAYS)

        if not df.empty:
            run_ts = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"{ticker}_{run_ts}.csv"
            fetcher.save_to_csv(df, filename, OUTPUT_DIR)
        else:
            print(f"⚠️ No data retrieved for {ticker}")

        # Rate limit
        time.sleep(1)


if __name__ == "__main__":
    print("🚀 Options Chain Multi-Ticker Scraper Started")
    print(f"📁 Output directory: {OUTPUT_DIR}")
    run_multi_ticker_scrape()
    print("\n✅ All tickers processed")
