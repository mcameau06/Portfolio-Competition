import yfinance as yf 
import pandas as pd
from typing import List
from concurrent.futures import ThreadPoolExecutor, as_completed
# dowload tickers

screened_stocks_df = pd.read_csv("screened_stocks.csv")


all_tickers = screened_stocks_df['Symbol'].to_list()
all_tickers = [yf.Ticker(ticker) for ticker in all_tickers]

def get_metrics(ticker:yf.Ticker):
    info = ticker.info
    # value
    price_to_earnings = info.get('forwardPE')
    price_to_book = info.get('priceToBook')
    if price_to_earnings == 0:
        earnings_yield = 0
    earnings_yield = 1 / price_to_earnings
    
    # momentum and volatility
    momentum_3m, momentum_6m, volatility_daily, volatility_annual = get_momentum(ticker)

    #quality
    return_on_equity = info.get('returnOnEquity')
    debt_to_equity = info.get('debtToEquity')

    #size
    market_cap = info.get('marketCap')

   

    return [price_to_earnings,price_to_book,earnings_yield,return_on_equity,debt_to_equity,
    market_cap,momentum_3m,momentum_6m,volatility_daily,volatility_annual]

def get_momentum(ticker:yf.Ticker) -> List[float] :
    hist = ticker.history(period="7mo")
    if len(hist) < 126:
        raise ValueError("Not enough historical data for 6M momentum")

    price_today = hist['Close'].iloc[-1]
    price_3m_ago = hist['Close'].iloc[-63] # ~63 trading days in 3 months
    price_6m_ago = hist['Close'].iloc[-126]
    
    return_3m = (price_today - price_3m_ago)/price_3m_ago

    return_6m = (price_today - price_6m_ago)/price_6m_ago

    volatility_daily = hist['Close'].pct_change().std()

    # annualized volatility
    volatility_annual = volatility_daily * (252 ** 0.5)

    return [return_3m, return_6m, volatility_daily,volatility_annual]
def process_ticker(ticker: yf.Ticker):
    try:
        info = ticker.info
        metrics = get_metrics(ticker)

        ticker_data = {
            'Symbol': info.get('symbol'),
            'Name': info.get('longName'),
            'Sector': info.get('sector'),
            'Price to Earnings': metrics[0],
            'Price to Book': metrics[1],
            'Earnings Yield': metrics[2],
            'Return on Equity': metrics[3],
            'Debt to Equity': metrics[4],
            'Market Cap': metrics[5],
            'Momentum 3m': metrics[6],
            'Momentum 6m': metrics[7],
            'Volatility Daily': metrics[8],
            'Volatility Annual': metrics[9],
        }
        print(f"Downloaded metrics for {info.get('symbol')}")
        return ticker_data
    except Exception as e:
        # Optional: log and skip failures
        print(f"Failed for {ticker.ticker}: {e}")
        return None
if __name__ == "__main__":
    print("Downloading metrics for all tickers")

    all_metrics = []
    
    with ThreadPoolExecutor(max_workers=2 ) as executor:
        futures = [executor.submit(process_ticker, t) for t in all_tickers]

        for future in as_completed(futures):
            result = future.result()
            if result is not None:
                all_metrics.append(result)
    df = pd.DataFrame(all_metrics)
    df.to_csv('metrics.csv', index=False)
    print("Metrics saved to metrics.csv")