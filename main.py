import pandas as pd
import requests
from bs4 import BeautifulSoup
import yfinance
import pytz
from datetime import datetime
from io import StringIO
from utils import load_pickle, save_pickle, Alpha
from threading import Thread

def get_sp500_tickers():
    res = requests.get("https://en.wikipedia.org/wiki/List_of_S%26P_500_companies")
    soup = BeautifulSoup(res.content, 'html.parser')
    table = soup.find_all('table')[0]
    df = pd.read_html(StringIO(str(table)))[0] 
    tickers = list(df['Symbol'])
    return tickers

def get_history(ticker, period_start, period_end, granularity="1d", tries=0):
    try:
        df = yfinance.Ticker(ticker).history(
            start=period_start,
            end=period_end,
            interval=granularity,
            auto_adjust=True
        ).reset_index()
    except Exception as err:
        if tries < 5:
            return get_history(ticker, period_start, period_end, granularity, tries +1)
        return pd.DataFrame()

    df = df.rename(columns={
        "Date": "datetime",
        "Open": "open",
        "High": "high",
        "Low": "low",
        "Close": "close",
        "Volume": "volume",
    })

    if df.empty:
        return pd.DataFrame()

    df["datetime"] = df["datetime"].dt.tz_convert(pytz.utc)
    df = df.drop(columns=["Dividends", "Stock Splits"])
    df = df.set_index("datetime", drop=True)
    return df

def get_histories(tickers, period_start, period_end, granularity="1d"):
    dfs = [None] * len(tickers)
    def _helper(i):
        dfs[i] = get_history(tickers[i], period_start, period_end, granularity)
    threads = [Thread(target=_helper, args=(i,)) for i in range(len(tickers))]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    tickers = [tickers[i] for i in range(len(tickers)) if not dfs[i].empty]
    dfs = [df for df in dfs if not df.empty]
    return tickers, dfs

def get_ticker_dfs(start, end):
    try:
        tickers, ticker_dfs = load_pickle("dataset.obj")
    except Exception as err:
        tickers = get_sp500_tickers()
        tickers, dfs = get_histories(tickers, start, end, granularity="1d")
        ticker_dfs = {ticker: df for ticker, df in zip(tickers, dfs)}
        save_pickle("dataset.obj", (tickers, ticker_dfs))
    return tickers, ticker_dfs


period_start = datetime(2010, 1, 1, tzinfo=pytz.utc)
period_end = datetime.now(pytz.utc)
tickers, ticker_dfs = get_ticker_dfs(start=period_start, end=period_end)
alpha = Alpha(insts=tickers, dfs=ticker_dfs,start=period_start,end=period_end)
alpha.run_simulation()
