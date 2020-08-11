"""
    Weighting related functions
"""

from db_utils import execute_sql, read_select


def insert_weights(db, temptbl_name, portfolio_name, weights_tbl="portfolio_weights", debug=True):
    existing_weights = get_portfolio_weights(portfolio_name, db)
    if existing_weights.shape[0] > 0:
        print("Weights already exists for {}".format(portfolio_name))
        return
    q = """
        INSERT INTO <TBL:{_weights_tbl}> (portfolio_name, ticker, weight)
        SELECT portfolio_name, ticker, weight
        FROM <TBL:{_temptbl_name}>
    """
    p = {
        "_weights_tbl": weights_tbl,
        "_temptbl_name": temptbl_name
    }
    return execute_sql(db, q, p, debug)


def get_portfolio_weights(portfolio_name, db):
    q = """
        SELECT * 
        FROM <TBL:portfolio_weights>
        WHERE portfolio_name = {_portfolio_name}
    """
    p = {"_portfolio_name": portfolio_name}
    df = read_select(db, q, p)
    return df

# to be retired
def get_ticker_weight(portfolio_name, ticker, db):
    q = """
        SELECT ticker, weight
        FROM portfolio_weights
        WHERE portfolio_name = {_pf_name}
        AND ticker = {_ticker}
    """
    p = {
        "_pf_name": portfolio_name,
        "_ticker": ticker
    }
    read_select(db, q, p)


def get_single_ticker_weight(portfolio_name, ticker, db):
    q = """
        SELECT weight
        FROM <TBL:portfolio_weights>
        WHERE portfolio_name = {_pf_name}
        AND ticker = {_ticker}
    """
    p = {
        "_pf_name": portfolio_name,
        "_ticker": ticker
    }
    df = read_select(db, q, p)
    if df.shape[0] > 0:
        weight = df.weight.values[0]
        return float(weight)
    else:
        return None