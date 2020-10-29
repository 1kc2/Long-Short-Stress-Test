import numpy as np
import pandas as pd
from pandas import DataFrame

import visuals as vis
from attribution import get_portfolio_returns, get_ticker_returns
from db_utils import get_postgres_engine
from stress_test_rpt import render_report

STRESS_TEST_TEMPLATE = "stress_test_rpt.html"
STRESS_TEST_RPT_TARGET = "../reports/targets/stress_test_rpt_output.html"

class StressTest:
    """
    """
    def __init__(self, portfolio, template_name=STRESS_TEST_TEMPLATE
                 , target_path=STRESS_TEST_RPT_TARGET):
        self.db = get_postgres_engine()
        self.pf = portfolio
        self.benchmark = portfolio.benchmark
        self.mkt_up_down = range(-15,20,5)
        self.scenario_ret = {}
        self.template_name = template_name
        self.target_path = target_path

    def get_pf_ret(self):
        # _ get portfolio returns
        pf_ret = get_portfolio_returns(self.pf.portfolio_name, self.db)
        self.pf_ret = pf_ret.price_ret

    def get_bench_ret(self):
        # _ get benchmark returns
        self.bench_ret = get_ticker_returns(self.benchmark.ticker, self.db)

    def merge_frames(self):
        agg_df = pd.concat([self.pf_ret, self.bench_ret], axis=1)
        agg_df.columns = ["pf_ret", "bench_ret"]
        agg_df.dropna(inplace=True)
        self.agg_df = agg_df
        return agg_df

    def run_test(self):

        self.get_pf_ret()
        self.get_bench_ret()
        agg_df = self.merge_frames()


        # _ get covariance
        cov_matrix = np.cov(agg_df["pf_ret"], agg_df["bench_ret"])
        self.cov_matrix = cov_matrix

        # _ get beta
        beta = cov_matrix[0,1] / cov_matrix[1,1]
        self.beta = beta

        for event in self.mkt_up_down:
            scenario_ret = event * beta
            self.scenario_ret[event] = [scenario_ret]

        self.scenario_df = DataFrame(self.scenario_ret).T

    def gen_correlations(self, window_size=30):
        # rolling correlations
        self.rolling_corr = self.agg_df["pf_ret"].dropna() \
            .rolling(window=window_size) \
            .corr(self.agg_df["bench_ret"])

    def gen_visuals(self):
        longs_path, shorts_path = vis.pos_breakdown(self.pf.weights)
        self.longs_breakdown_image_url = longs_path
        self.shorts_breakdown_image_url = shorts_path

        self.cumulative_returns_image_url = vis.cumulative_returns(self.agg_df)
        self.scenario_returns_image_url = vis.scenario_returns(self.scenario_df, self.beta, self.benchmark.ticker)
        self.correlation_chart_image_url = vis.correlation_chart(self.rolling_corr)

        self.template_opts = {
            "visuals": {
                "longs_breakdown_image_url": self.longs_breakdown_image_url,
                "shorts_breakdown_image_url": self.shorts_breakdown_image_url,
                "cumulative_returns_image_url": self.cumulative_returns_image_url,
                "scenario_returns_image_url": self.scenario_returns_image_url,
                "correlation_chart_image_url": self.correlation_chart_image_url
            }
        }
    def render_report(self):
        render_report(self.template_name, self.target_path, self.template_opts)
        print("Report is available at: {}".format(self.target_path))

    def run(self):
        self.run_test()
        self.gen_correlations()
        self.gen_visuals()
        self.render_report()


def calc_cov_matrix(asset_ret, bench_ret):
    return np.cov(asset_ret, bench_ret)


def calc_asset_beta(asset_ret, bench_ret):
    cov_matrix = calc_cov_matrix(asset_ret, bench_ret)
    beta = cov_matrix[0, 1] / cov_matrix[1, 1]
    return beta