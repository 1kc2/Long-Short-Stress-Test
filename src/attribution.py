from db_utils import read_select, get_temptable,\
                     insert_temp_ret_table, drop_temp_table
from weights import get_portfolio_weights, get_single_ticker_weight
from pandas import DataFrame
import pandas as pd

def load_constituent_prices(ticker, db):
    q = """
        SELECT *
        FROM eq_prices
        WHERE ticker = {_ticker}
    """
    p = {"_ticker": ticker}
    prices = read_select(db, q, p, in_df=True)
    return prices


def calc_daily_return(ticker, db):
    existing_returns = get_ticker_returns(ticker, db)
    if existing_returns.shape[0] > 0:
        print("returns already existing for {}".format(ticker))
        return
    # _ get prices
    prices = load_constituent_prices(ticker, db)
    if prices.shape[0] == 0:
        raise RuntimeError("no prices found for {}".format(ticker))

    # _ calculate returns
    prices.index = prices["price_date"]
    returns = prices["price"] / prices["price"].shift(1) - 1
    returns.dropna(inplace=True)

    # _ prepare returns df
    insert_df = DataFrame(returns)
    insert_df = insert_df.reset_index()
    insert_df.columns = ["return_date", "price_ret"]
    insert_df["price_ret"] = insert_df["price_ret"].astype(float)
    insert_df.loc[:, "ticker"] = ticker

    # _ insert returns and clean up
    temptbl = get_temptable()
    try:
        insert_df.to_sql(temptbl, db)
        insert_temp_ret_table(db, temptbl, "daily_constituent_returns")
    except:
        print("Error loading returns for {}".format(ticker))
    drop_temp_table(db, temptbl)


def calc_daily_constituent_returns(tickers, db):
    for ticker in tickers:
        calc_daily_return(ticker, db)


def calc_daily_portfolio_returns(portfolio_name, db):

    existing_returns = get_portfolio_returns(portfolio_name, db)
    if existing_returns.shape[0] > 0:
        print("returns already exists for {}".format(portfolio_name))
        return

    # _ get constituent weights
    weights = get_portfolio_weights(portfolio_name, db)

    # _ get constituent returns
    # _ build a giant frame and merge it
    constituents = weights.ticker.tolist()
    adj_returns = {}
    for ticker in constituents:
        # _ calculate return contribution for each constituent
        _ticker_return = get_ticker_returns(ticker, db)
        _ticker_weight = get_single_ticker_weight(portfolio_name, ticker, db)
        if (_ticker_return is not None and _ticker_weight is not None):
            _adj_ret = _ticker_return * _ticker_weight
        adj_returns[ticker] = _adj_ret

    # _ clean-up frame
    portfolio_returns = DataFrame(adj_returns)
    portfolio_returns.fillna(0, inplace=True)

    # _ aggregate on the portfolio
    portfolio_returns_agg = portfolio_returns.sum(axis=1)
    portfolio_returns_agg = portfolio_returns_agg.reset_index()
    portfolio_returns_agg.columns = ["return_date", "price_ret"]
    portfolio_returns_agg.loc[:, "portfolio_name"] = portfolio_name

    # _ store in db
    temptbl = get_temptable()
    try:
        portfolio_returns_agg.to_sql(temptbl, db)
        insert_temp_ret_table(db, temptbl, returns_tbl="portfolio_returns", is_pf=True)
    except:
        print("Error loading portfolio returns for {}".format(portfolio_name))
    drop_temp_table(db, temptbl, debug=True)


def get_ticker_returns(ticker, db):
    q = """
        SELECT price_ret, return_date
        FROM <TBL:daily_constituent_returns> 
        WHERE ticker = {_ticker}
    """
    p = {"_ticker": ticker}
    df = read_select(db, q, p)
    if df.shape[0] > 0:
        index = pd.DatetimeIndex(df["return_date"])
        df.index = index
        del df["return_date"]
        return df["price_ret"].astype(float)
    else:
        return df


def get_portfolio_returns(portfolio_name, db):
    q = """
        SELECT *
        FROM <TBL:portfolio_returns>
        where portfolio_name = {_portfolio_name}
    """
    p = {"_portfolio_name": portfolio_name}
    df = read_select(db, q, p)
    if df.shape[0] > 0:
        index = pd.DatetimeIndex(df["return_date"])
        df.index = index
        del df["return_date"]
        df["price_ret"] = df["price_ret"].astype(float)
        return df
    return df
