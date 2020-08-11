"""
Download Yahoo prices
"""
import os
import pandas_datareader as pdr
from datetime import datetime
from dateutil.relativedelta import relativedelta

from db_utils import get_engine
from db_utils import get_temptable, drop_temp_table,\
                     insert_temp_price_table, read_select

TODAY = datetime.today()
TEN_YRS = relativedelta(years=10)
TEN_YRS_AGO = TODAY - TEN_YRS


def download_yahoo_prices(ticker, start_date=TEN_YRS_AGO, end_date=TODAY):
    """
    
    :param ticker: Ticker to download prices for 
    :param start_date: Historical price start dare
    :param end_date: Historical price end date
    :return: Price df or None
    """
    print("Downloading prices for {}".format(ticker))
    try:
        prices = pdr.get_data_yahoo(symbols=ticker,
                                    start=start_date,
                                    end=end_date)
    except:
        print("Error occured downloading prices for {}".format(ticker))
        return None
    prices = prices.reset_index()
    prices.loc[:, "price_date"] = prices["Date"]
    prices.loc[:, "price"] = prices["Adj Close"]
    prices.loc[:, "ticker"] = ticker
    prices.loc[:, "source"] = "yahoo"
    return prices


def insert_prices_to_db(db, price_df, ticker):
    """
    Insert into temp db
    Insert into price db
    Cleanup temp table
    :param db: SQLAlchemy engine 
    :param price_df: 
    :param ticker: str
    :param tablename: str
    :return: 
    """
    temptbl = get_temptable()
    price_db_cols = ["price_date", "ticker", "source", "price"]
    df_to_insert = price_df[price_db_cols]
    try:
        df_to_insert.to_sql(temptbl, db)
        insert_temp_price_table(db, temptbl, "eq_prices", debug=True)
    except:
        print("Error loading in priceds for {}".format(ticker))

    drop_temp_table(db, temptbl, debug=True)


def prices_exist(ticker, db):
    q = """
        SELECT *
        FROM eq_prices
        WHERE ticker = {_t}
    """
    p = {"_t": ticker}
    res = read_select(db, q, p)
    return res.shape[0] > 0

def load_prices(db, tickers):
    """
    Download prices and load to tbd
    :param db: 
    :param tickers: 
    :return: 
    """
    for ticker in tickers:
        # _ TODO: check prices for ticker and get new range if necessary
        if prices_exist(ticker, db):
            print("prices already exist for {}".format(ticker))
            continue
        else:
            cur_prices = download_yahoo_prices(ticker)
            if cur_prices is None:
                continue
            else:
                insert_prices_to_db(db, cur_prices, ticker)
