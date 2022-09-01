"""

"""


import sqlite3
import pandas as _pd
from fuzzywuzzy import fuzz as _fuzz, process as _process


def fuzzy_match_df(col1, col2, threshold=90, fuzz_type='token_sort_ratio'):
    """Create intermediate pandas DataFrame that fuzzy matches 2 arrays of strings.

    Args:
        col1 (array-like): All values are compared.
        col2 (array-like): If not matched to col1 then not retrieved.
        threshold (int, optional): Set minimum matching threshold ratio. Defaults to 90.
        fuzz_type (str, optional): fuzzywuzzy.fuzz matching type. Defaults to 'token_sort_ratio'.

    Returns:
        pandas.DataFrame: Returns a DataFrame from which you can join matched objects together on.
    """

    # get unique values from columns passed to function
    col1 = _pd.Series(col1).unique()
    col2 = _pd.Series(col2).unique()

    matches = []

    for c1 in col1:
        match = _process.extractOne(c1, col2, scorer=getattr(
            _fuzz, fuzz_type), score_cutoff=threshold)  # gets the best match above threshold
        if match is None:
            match_tuple = (c1, None)
        else:
            match_tuple = (c1, match[0])

        matches.append(match_tuple)
    return _pd.DataFrame(matches, columns=['COL1', 'COL2_MATCH'])


def sql_to_df(db_path, sql_txt):
    """Query SQLite database/table and return results into pandas DataFrame

    Args:
        db_path (string): SQLite DB path.
        sql_txt (string): SQL statement in string format.

    Returns:
        pandas.DataFrame
    """

    with sqlite3.connect(db_path) as conn:
        df = _pd.read_sql_query(sql_txt, conn)
        return df
    conn.close()


def df_to_sqlite(df, db_path, table_name, index=False, if_exists='replace', chunksize=5000):
    """Create SQLite table from pandas DataFrame

    Args:
        df (pd.DataFrame): DataFrame to convert to SQLite table.
        db_path (string): SQLite database path to store table.
        table_name (string): Name for SQLite table.
        index (bool, optional): Index created as column?. Defaults to False.
        if_exists (str, optional): Handle method if exists. Defaults to 'replace'.
        chunksize (int, optional): Chunksize for reading into table. Defaults to 5000.
    """

    df.to_sql(name=table_name, con=f"sqlite:///{db_path}",
              index=index, if_exists=if_exists, chunksize=chunksize)


def sqlite_tables(db_path):
    """Show all custom tables in specified SQLite .db.

    Args:
        db_path (string): SQLite .db path.

    Returns:
        pandas.DataFrame
    """

    sql_txt = r"SELECT name AS TABLE_NAME FROM sqlite_schema WHERE type='table' AND name NOT LIKE 'sqlite_%'"
    with sqlite3.connect(db_path) as conn:
        df = _pd.read_sql_query(sql_txt, conn)
        return df
    conn.close()


def sqlite_drop_table(db_path, table_name):
    """Drop specified table in SQLite db.

    Args:
        db_path (string): SQlite .db path.
        table_name (string): Table name to drop.
    """

    sql_txt = f'DROP TABLE IF EXISTS {table_name}'
    with sqlite3.connect(db_path) as conn:
        conn.execute(sql_txt)
    conn.close()
