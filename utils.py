import lzma
import dill as pickle
import pandas as pd

def load_pickle(path):
    with lzma.open(path, "rb") as fp:
        file = pickle.load(fp)
    return file

def save_pickle(path, obj):
    with lzma.open(path, "wb") as fp:
        pickle.dump(obj,fp)

class Alpha():

    def __init__(self, insts, dfs, start, end):
        self.insts = insts
        self.dfs = dfs
        self.start = start
        self.end = end

    def init_portfolio_settings(self, trade_range):
        portfolio_df = pd.DataFrame(index=trade_range)\
            .reset_index()\
            .rename(columns={"index":"datetime"})
        portfolio_df.loc[0, "capital"] = 10000
        return portfolio_df

    def compute_meta_info(self, trade_range):
        for inst in self.insts:
            df = pd.DataFrame(index=trade_range)
            self.dfs[inst] = df.join(self.dfs[inst]).ffill().bfill()
            self.dfs[inst]["ret"] = -1 + self.dfs[inst]["close"]/self.dfs[inst]["close"].shift(1)
            print(df)
            print(self.dfs[inst])
            input("see")

    def run_simulation(self):
        print("running backtest")
        date_range = pd.date_range(start=self.start, end=self.end, freq="D")
        self.compute_meta_info(trade_range=date_range)
        portfolio_df = self.init_portfolio_settings(trade_range=date_range)
        for i in portfolio_df.index:
            date = portfolio_df.loc[i, "datetime"]

            if i != 0:
                # compute pnl
                pass

            alpha_scores = ()
            #  compute alpha signals

            # compute positions and other information
        # print(date_range)