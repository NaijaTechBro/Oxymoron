import pytz
import requests
import threading
import pandas as pd
import numpy as np
import yfinance  # type: ignore
import threading
from datetime import datetime
from bs4 import BeautifulSoup
from io import StringIO

# Get all tickers (web scrapping)
def get_sp500_tickers():
    res = requests.get("https://en.wikipedia.org/wiki/List_of_S%26P_500_companies") #"https://ngxgroup.com/exchange/trade/equities/listed-companies/
    soup = BeautifulSoup(res.content, 'html.parser')
    table = soup.find_all('table')[0]
    df = pd.read_html(StringIO(str(table)))
    tickers = list(df[0].Symbol)
    return tickers

# get formatted history
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
            return get_history(ticker, period_start, period_end, granularity, tries+1)
        return pd.DataFrame
    
    df = df.rename(columns={
        "Date":"datetime",
        "Open":"open",
        "High":"high",
        "Low":"low",
        "Close":"close",
        "Volume":"volume"
    })
    if df.empty:
        return pd.DataFrame()
    
    df["datetime"] = df["datetime"].dt.tz_convert(pytz.utc)
    df = df.drop(columns=["Dividends", "Stock Splits"])
    df = df.set_index("datetime", drop=True)
    return df

# get histories for all tickers
def get_histories(tickers, period_starts, period_ends, granularity="1d"):
    dfs = [None]*len(tickers)
    def _helper(i):
        print(tickers[i])
        df= get_history(
            tickers[i],
            period_starts[i],
            period_ends[i], 
            granularity=granularity
        )
        dfs[i] = df
        # threading in other to get the len list of all the tickers
    threads = [threading.Thread(target=_helper, args=(i,)) for i in range(len(tickers))]
    [thread.start() for thread in threads]
    [thread.join() for thread in threads]
    tickers = [tickers[i] for  i in range(len(tickers)) if not dfs[i].empty]
    dfs = [df for df in dfs if not df.empty]
    return tickers, dfs

# get tickers in the data frame after consuming data from yfinance (save in a dataset object)
def get_ticker_dfs(start, end):
    from utils import load_pickle, save_pickle
    try:
        tickers, ticker_dfs = load_pickle("dataset.obj")
    except Exception as err:
        tickers = get_sp500_tickers()
        starts = [start]*len(tickers)
        ends = [end]*len(tickers)
        tickers, dfs = get_histories(tickers, starts, ends, granularity="1d")
        ticker_dfs = {ticker:df for ticker,df in zip(tickers,dfs)}
        save_pickle("dataset.obj", (tickers, ticker_dfs))
    return tickers, ticker_dfs
    # Alpha Backtest Usecase
from utils import Alpha
from utils import load_pickle
period_start = datetime(2010,1,1, tzinfo=pytz.utc)
period_end = datetime.now(pytz.utc)
tickers, ticker_dfs = get_ticker_dfs(start = period_start, end = period_end)
testfor = 20
tickers =tickers[:testfor]

from alpha1 import Alpha1
from alpha2 import Alpha2
from alpha3 import Alpha3

alpha1 = Alpha1(insts=tickers, dfs=ticker_dfs, start=period_start, end=period_end)
alpha2 = Alpha2(insts=tickers, dfs=ticker_dfs, start=period_start, end=period_end)
alpha3 = Alpha3(insts=tickers, dfs=ticker_dfs, start=period_start, end=period_end)

df1 = alpha1.run_simulation()

exit()
# df2 = alpha2.run_simulation()
# df3 = alpha3.run_simulation()
df1, df2, df3  = load_pickle("simulations.obj") #(df1, df2, df3))

import matplotlib.pyplot as plt # type: ignore

plt.plot(df1.capital)
plt.plot(df2.capital)
plt.plot(df3.capital)
plt.show()
plt.close()

nzr = lambda df: df.capital_ret.loc[df.capital_ret != 0].fillna(0)
def plot_vol(r):
    vol = r.rolling(25).std() * np.sqrt(253)
    plt.plot(vol)
    plt.show()
    plt.close()
    
plot_vol(nzr(df1))
plot_vol(nzr(df2))
plot_vol(nzr(df3))

print(nzr(df1).std()*np.sqrt(253), nzr(df2).std()*np.sqrt(253), nzr(df3).std()*np.sqrt(253))

    