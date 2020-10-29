"""
Long/Short Equity Portfolio
"""

import pandas as pd
from db_utils import get_postgres_engine, get_temptable, drop_temp_table
from download import load_prices
from attribution import calc_daily_constituent_returns,\
                        calc_daily_portfolio_returns
from weights import insert_weights


class Benchmark:
    def __init__(self, ticker="^GSPC"):
        self.ticker = ticker
        self.db = get_postgres_engine()
        self.load()

    def load(self):
        load_prices(self.db, [self.ticker])
        calc_daily_constituent_returns([self.ticker], self.db)


class Portfolio:
    """
    """
    def __init__(self, portfolio_name, benchmark_name="^GSPC", weights_path="./weights.csv"):
        self.db = get_postgres_engine()
        self.weights_path = weights_path
        self.portfolio_name = portfolio_name
        self.benchmark = Benchmark(benchmark_name)

    def load_weights(self):
        # _ load weights from csv
        weights = pd.read_csv(self.weights_path)
        weights.loc[:, "portfolio_name"] = self.portfolio_name

        # _ load weights into db
        tmptbl = get_temptable()
        weights.to_sql(tmptbl, self.db)
        insert_weights(self.db, tmptbl, self.portfolio_name)
        drop_temp_table(self.db, tmptbl)

        # _ store weights on class for convenience
        weights.index = weights["ticker"]
        self.weights = weights
        self.constituents = weights.ticker.tolist()

    def download_prices(self):
        # _ load prices into db
        load_prices(self.db, self.constituents)

    def calc_returns(self):
        calc_daily_constituent_returns(self.constituents, self.db)
        calc_daily_portfolio_returns(self.portfolio_name, self.db)

    def run(self):
        self.load_weights()
        self.download_prices()
        self.calc_returns()
