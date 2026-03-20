import yfinance as yf 
import pandas as pd
from typing import List
from concurrent.futures import ThreadPoolExecutor, as_completed

def get_metrics(ticker:yf.Ticker):
    info = ticker.info
    # value
    price_to_earnings = info.get('forwardPE')
    price_to_book = info.get('priceToBook')
    if not price_to_earnings:
        earnings_yield = 0
    else:
        earnings_yield = 1 / price_to_earnings
    
    # momentum and volatility
    momentum_1m, momentum_3m, volatility_daily, volatility_annual = get_momentum(ticker)

    #quality
    return_on_equity = info.get('returnOnEquity')
    debt_to_equity = info.get('debtToEquity')

    #size
    market_cap = info.get('marketCap')

   

    return [price_to_earnings,price_to_book,earnings_yield,return_on_equity,debt_to_equity,
    market_cap,momentum_1m,momentum_3m,volatility_daily,volatility_annual]

def get_momentum(ticker:yf.Ticker) -> List[float]:
    
    hist = ticker.history(period="6mo")
    if len(hist) < 63:
        raise ValueError("Not enough historical data for 6M momentum")

    price_today = hist['Close'].iloc[-1]
    price_1m_ago = hist['Close'].iloc[-21] # ~about 21 trading days in 1 month
    price_3m_ago = hist['Close'].iloc[-63]
    
    return_1m = (price_today - price_1m_ago)/price_1m_ago

    return_3m = (price_today - price_3m_ago)/price_3m_ago

    volatility_daily = hist['Close'].pct_change().std()

    
    volatility_annual = volatility_daily * (252 ** 0.5)

    return [return_1m, return_3m, volatility_daily,volatility_annual]

def process_ticker(ticker: yf.Ticker):
    try:
        info = ticker.info
        metrics = get_metrics(ticker)

        ticker_data = {
            'Symbol': info.get('symbol'),
            'Name': info.get('longName'),
            'Sector': info.get('sector'),
            'Price_to_Earnings': metrics[0],
            'Price_to_Book': metrics[1],
            'Earnings_Yield': metrics[2],
            'Return_on_Equity': metrics[3],
            'Debt_to_Equity': metrics[4],
            'Market_Cap': metrics[5],
            'Momentum_1m': metrics[6],
            'Momentum_3m': metrics[7],
            'Volatility_Daily': metrics[8],
            'Volatility_Annual': metrics[9],
        }
        print(f"Downloaded metrics for {info.get('symbol')}")
        return ticker_data
    except Exception as e:
        # Optional: log and skip failures
        print(f"Failed for {ticker.ticker}: {e}")
        return None

def get_tickers(tickers_file_path):
    # converts the tickers in the Symbol column of the data frame into a list

    screened_stocks_df = pd.read_csv(tickers_file_path)
    all_tickers = screened_stocks_df['Symbol'].to_list()
    all_tickers = [yf.Ticker(ticker) for ticker in all_tickers]

    return all_tickers

def create_metrics_df(tickers_file_path:str) -> pd.DataFrame :
    # creates of data frame with the list of tickers 

    all_tickers = get_tickers(tickers_file_path)
    
    all_metrics = []

    print("Downloading metrics for all tickers")

    with ThreadPoolExecutor(max_workers=4 ) as executor:
        futures = [executor.submit(process_ticker, t) for t in all_tickers]

        for future in as_completed(futures):
            result = future.result()
            if result is not None:
                all_metrics.append(result)

    df = pd.DataFrame(all_metrics)
    df = df.sort_values(by=['Sector'])
    
    print("Finished Loading Metrics")

    return df
    

