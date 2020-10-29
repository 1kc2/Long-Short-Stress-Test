import os
from datetime import datetime
import pandas as pd
from pandas import DataFrame
import numpy as np
from db_utils import get_engine


class Portfolio:
    """
    """
    def __init__(self, weights_path, history, conn_str=None):
        """
        :param weights_path: 
        :param history: 
        :param conn_str: SQLAlchemy connection str 
        """
        # _ db will be a filesystem basaed db if no params are given
        self.db = get_engine()
        self.weights = self.load_weights()
        self.constituents = self.load_manifest()
        self.start_date = datetime(2017,4,21)
        self.end_date = datetime(2017,4,24)
        self.prices = {}
        self.constituent_returns = {}
        self.adj_returns = {}

    def load_weights(self):
        print("Loading portfolio weights")
        self.weights = pd.read_csv(self.root_ticker_path + "weights")
        self.weights = self.weights.ticker

    def load_manifest(self):
        """
        """
        print("Loading ticker manifest")
        with open(os.path.join(self.root_ticker_path, "src/tickers"), "r") as ticker_file:
          all_tickers = ticker_file.readlines()
        all_tickers = [i.strip() for i in all_tickers]
        return all_tickers
    
    def get_weight(self, ticker):
        """
        """
        w = self.weights[self.weights.ticker == ticker]["weights"]
        return 0  if w.shape[0] == 0 else w.values[0]
    
    def load_price(self, ticker):
        print("Loading prices for: {}".format(ticker))
        try:
            p = pd.read_excel(os.path.join(self.prices_path, ticker + ".xlsx"))
            p.index = p.Date
        except:
            print("cannot load prices for {}".format(ticker))
            return []
        self.prices[ticker] = p
        return p
            
    def load_prices(self):
        for ticker in self.constituents:
            self.load_price(ticker)
    
    def calc_ticker_return(self, ticker):
        print("Calculating returns for {}".format(ticker))
        prices = self.prices[ticker]
        returns = prices["Adj Close"] / prices["Adj Close"].shift(1) - 1
        returns = returns.dropna()
        self.constituent_returns[ticker] = returns
    
    def calc_returns(self):
        for ticker in self.constituents:
            self.calc_ticker_return(ticker)
        
    def calc_portfolio_returns(self):
        # _ get returns for each constituents and returns
        for ticker in self.constituents:
            ret = self.constituent_returns[ticker]
            weight = self.get_weight(ticker)
            adj_ret = ret * weight
            self.adj_returns[ticker] = adj_ret
        
        # _ generate a giant frame
        return_df = DataFrame(self.adj_returns)

        # _ sum returns for the day
        daily_returns = return_df.apply(sum, axis=1)
        self.portfolio_returns = daily_returns

    def calc_benchmark_returns(self):
        price = self.load_price("SP500")
        returns = price["Adj Close"] / price["Adj Close"].shift(1) - 1
        returns = returns.dropna()
        self.benchmark_returns = returns
    
    def calc_cov_matrix(self, asset_ret, bench_ret):
        covmat = np.cov(asset_ret, bench_ret)
        return covmat

    def calc_beta(self):
        pf_ret = self.portfolio_returns
        bench_ret = self.benchmark_returns
        df = DataFrame({"asset_ret": pf_ret, "bench_ret": bench_ret}).dropna()
        covmat = self.calc_cov_matrix(df["asset_ret"], df["bench_ret"])
        beta = covmat[0,1]/covmat[1,1]
        self.portfolio_beta = beta
        return beta
        

p = Portfolio()
p.load_prices()
p.calc_returns()
p.calc_portfolio_returns()
p.calc_benchmark_returns()
p.calc_beta()



class StressTest:
    def __init__(self, pf, scenarios=[-15,-10,-5,5,10,15]):
        self.pf = pf
        self.scenarios = scenarios
        
    def calc_stress(self):
        scenario_returns = {}
        for s in self.scenarios:
            pf_beta = self.pf.portfolio_beta
            stress_return = s * pf_beta
            scenario_returns[s] = stress_return
        self.stress_returns = scenario_returns

    def visualize(self):
        pass
    
    
s = StressTest(p)
s.calc_stress()

"""
    Visualizations
"""

from pandas import Series
import matplotlib.pyplot as plt


# _ positions
f, (ax1, ax2) = plt.subplots(1, 2)
p.weights[p.weights.weights>0]["weights"].sort_values().plot(kind="pie", figsize=(35,17.5), autopct='%.2f', ax=ax1, colors=sns.light_palette("green"), fontsize=20)
ax1.set_title("Longs", fontsize=40)
(p.weights[p.weights.weights<0]*-1)["weights"].sort_values().plot(kind="pie", figsize=(35,17.5), autopct='(%.2f)', ax=ax2, colors=sns.color_palette("green"), fontsize=20)
ax2.set_title("Shorts", fontsize=40)


# _ cumulative returns
pf_cum_ret = p.portfolio_returns.cumsum().dropna()
bench_start = pf_cum_ret.head(1).index
bench_cum_ret = p.benchmark_returns['2016-11-16':].cumsum()
f, ax = plt.subplots()
DataFrame({"portfolio": pf_cum_ret, "SP500": bench_cum_ret}).dropna().plot(figsize=(20,10), ax=ax)
ax.set_title("Portfolio vs Benchmark Returns (cumulative)")
ax.set_ylabel("Returns")


# _ stress scenarios
f, ax = plt.subplots()
Series(s.stress_returns).plot(kind="bar", figsize=(20,10), ax=ax)
ax.set_title('Stress Scenario returns for Mock Portfolio (pf beta: {:.2f} vs SP500)'.format(p.portfolio_beta))
ax.set_xlabel("Scenarios (pct up/down)")
ax.set_ylabel("Returns")


# _ correlations
pf_daily_ret = p.portfolio_returns.dropna().ffill()
bench_daily_ret = p.benchmark_returns['2016-11-16':]
cor_df = DataFrame({"portfolio": pf_daily_ret, "SP500": bench_daily_ret}).ffill()
cor_df.rolling(window=30).corr(cor_df["portfolio"], cor_df["SP500"])
f, ax = plt.subplots()
pd.rolling_corr(cor_df["portfolio"], cor_df["SP500"], window=30).dropna().plot(ax=ax, figsize=(20,10))
ax.set_title("30-day rolling correlation portfolio vs SP500")
ax.set_ylabel("Correlation")