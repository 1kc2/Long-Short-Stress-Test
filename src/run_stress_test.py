from argparse import ArgumentParser
from portfolio import Portfolio
from risk import StressTest

def parse_args():
    """
    Cmd-line argument parser
    :return: args
    """
    parser = ArgumentParser()
    parser.add_argument("--portfolio_name", default="TEST_PF")
    parser.add_argument("--weights_file", default="./weights.csv")
    parser.add_argument("--report_template_name", default="stress_test_rpt.html")
    parser.add_argument("--report_target_path", default="../reports/targets/report.html")
    parser.add_argument("--override", action="store_true")
    args = parser.parse_args()
    return args

def run_stress_test():

    args = parse_args()

    weights_path = args.weights_file
    portfolio_name = args.portfolio_name

    pf = Portfolio(portfolio_name, weights_path=weights_path)

    pf.load_weights()
    pf.download_prices()
    pf.calc_returns()

    template_path = args.report_template_name
    target_path = args.report_target_path

    rpt = StressTest(pf, template_path, target_path)
    rpt.run()

if __name__ == "__main__":
    run_stress_test()