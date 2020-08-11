import os
import re
from pandas import DataFrame
from datetime import datetime
from sqlalchemy import create_engine

from utils import stringify

TEMP_TABLE_PATTERN = re.compile("^temp_")
TABLE_PAT = re.compile("(<TBL:.*>)")

def get_engine(db_name="eq_stress_test.db", username=None, pswd=None, port=None,
               db_type="sqlite", hostname="localhost"):
    """
    
    :param db_name: Database name to be used default: eq_stress_test.db for a sqlite3 db 
    :param username: Username (str)
    :param pswd: Password (str)
    :param port: Port (int) 
    :param db_type: Database server type
    :param hostname: Servername
    :return: SQLAlchemy DB Engine
    :raise NameError: when db_type or db_name is None
    """
    if db_type == "sqlite":
        conn_str = "<db_type>://<username>:<pswd>@<hostname>:<port>/<db_name>"
    else:
        conn_str = "<db_type>://<username>:<pswd>@<hostname>:<port>"

    if (username is None and port is None):
        conn_str = "<db_type>://<hostname>:<port>/<db_name>"
    else:
        conn_str = conn_str.replace("<username>", username).replace("<pswd>", pswd)

    if port is None:
        conn_str = "<db_type>://<hostname>/<db_name>"
    else:
        conn_str = conn_str.replace("<port>", str(port))

    if hostname is None:
        conn_str = conn_str.replace("<hostname>","")
    elif hostname == "localhost" and db_type == "sqlite":
        conn_str = conn_str.replace("<hostname>", "")
    else:
        conn_str = conn_str.replace("<hostname>", hostname)

    if db_type is None:
        raise NameError("db_type cannot be None")
    else:
        conn_str = conn_str.replace("<db_type>", db_type)

    if db_name is None and db_type == "sqlite":
        raise NameError("db_name cannot be None")
    else:
        conn_str = conn_str.replace("<db_name>", db_name)

    return create_engine(conn_str)


def get_postgres_engine():
    db_type = "postgres"
    db_name = os.environ["PGRES_DBNAME"]
    username = os.environ["PGRES_USER"]
    pswd = os.environ["PGRES_PSWD"]
    port = os.environ["PGRES_PORT"]
    host = os.environ["PGRES_HOST"]
    return get_engine(db_name, username, pswd, port, hostname=host, db_type=db_type)


def setup_db_tables(drop_pre_init=False):
    db_type, username, pswd, port, host, db_type = ("postgres", None, None, None, None)
    try:
        db_name = os.environ["PGRES_DBNAME"]
        username = os.environ["PGRES_USER"]
        pswd = os.environ["PGRES_PSWD"]
        port = os.environ["PGRES_PORT"]
        host = os.environ["PGRES_HOST"]
    except:
        print("Make sure to use setup env variables")
    db_engine = get_engine(db_name, username, pswd, port, db_type=db_type, hostname=host)

    if drop_pre_init:
        q = """
            DROP TABLE IF EXISTS eq_prices;
            DROP TABLE IF EXISTS daily_constituent_returns;
            DROP TABLE IF EXISTS portfolio_weights;
            DROP TABLE IF EXISTS portfolio_returns;
            DROP TABLE IF EXISTS portfolio_beta;
            DROP TABLE IF EXISTS stress_scenarios;
        """
        execute_sql(db, q, debug=True)

    q = """
        CREATE TABLE eq_prices (
            ticker varchar (10),
            price_date date,
            price numeric,
            source varchar (10)
        );
        
        CREATE TABLE daily_constituent_returns (
            ticker varchar (10),
            return_date date,
            price_ret numeric
        );
        
        CREATE TABLE portfolio_weights (
            portfolio_name varchar (10),
            ticker varchar(10),
            weight numeric
        );
        
        CREATE TABLE portfolio_returns (
            portfolio_name varchar (10),
            return_date date,
            price_ret numeric
        );
        
        CREATE TABLE portfolio_beta (
            portfolio_name varchar (10),
            date date,
            beta numeric
        );
    """
    execute_sql(db_engine, q)


def insert_temp_price_table(db, tbl_name, price_tbl, debug=True):
    q = """
        INSERT INTO <TBL:eq_prices> (ticker, price_date, price, source)
        SELECT ticker, price_date, price, source
        FROM <TBL:{_temp_tbl_name}>
    """
    p = {
        "_price_tbl": price_tbl,
        "_temp_tbl_name": tbl_name
    }

    execute_sql(db, q, p, debug)


def insert_temp_ret_table(db, temptbl_name, returns_tbl, is_pf=False, debug=True):
    if is_pf:
        q = """
            INSERT INTO <TBL:portfolio_returns> (return_date, price_ret, portfolio_name)
            SELECT return_date, price_ret, portfolio_name
            FROM <TBL:{_temptbl_name}>
        """
    else:
        q = """
            INSERT INTO <TBL:{_returns_tbl}> (ticker, return_date, price_ret)
            SELECT ticker, return_date, price_ret
            FROM <TBL:{_temptbl_name}>
        """
    p = {
        "_returns_tbl": returns_tbl,
        "_temptbl_name": temptbl_name
    }
    return execute_sql(db, q, p, debug)


def apply_params(query, params):
    """
    
    :param query: 
    :param params: 
    :return: 
    """
    for key, val in params.items():
        if type(val) == str:
            params[key] = stringify(val)
        elif type(val) == list:
            if type(val[0]) == int or type(val[0]) == float:
                params[key] = stringify(val, is_list=True, int_list=True)
            else:
                params[key] = stringify(val, is_list=True)
    statement = query.format(**params)
    return statement


def norm_q(query, params):
    groups = TABLE_PAT.findall(query)
    if len(groups) > 0:
        for g in groups:
            tablename = g.replace("<","").replace(">","").split(":")[1]
            # _ if the tablename is passed as a variable remove formatting
            if "{" in tablename:
                tablename = tablename.replace("{", "").replace("}", "")
                tablename_to_be_used = params[tablename]
                del params[tablename]
            else:
                tablename_to_be_used = tablename
            query = query.replace(g, tablename_to_be_used)
    return query


def execute_sql(db, query, params={}, debug=False):
    """
    
    :param db: 
    :param query: 
    :param params: 
    :param debug: 
    :return: 
    """
    statement = norm_q(query, params)
    statement = apply_params(statement, params)
    if debug:
        print(statement)
    return db.execute(statement)


def read_select(db, query, params={}, debug=False, in_df=True):
    res = execute_sql(db, query, params, debug)
    data = res.fetchall()
    col_names = res.keys()
    df = DataFrame.from_records(data, columns=col_names)
    return df


def drop_temp_table(db, tbl_name, debug=False):
    if not TEMP_TABLE_PATTERN.match(tbl_name):
        print("Not a temp table")
    else:
        q = "DROP TABLE IF EXISTS <TBL:{_tbl_name}>"
        p = {"_tbl_name": tbl_name}
        execute_sql(db, q, p, debug)


def get_temptable():
    """
    :return: temptable name
    """
    return "temp_" + datetime.now().strftime("%m%s")


def delete_existing_portfolio_returns(portfolio_name, db):
    q = """
        DELETE FROM <TBL:portfolio_returns>
        WHERE portfolio_name = {_pf_name}
    """
    p = {"_pf_name": portfolio_name}
    execute_sql(db, q, p)