"""
This module is used to work with SQLite .db
"""


import sqlite3 as _sqlite3
import pandas as _pd
import dask.dataframe as _dd 

def sql_to_df(db_path, sql_txt):
    """Query SQLite database/table and return results into pandas DataFrame

    Args:
        db_path (string): SQLite DB path.
        sql_txt (string): SQL statement in string format.

    Returns:
        pandas.DataFrame
    """

    with _sqlite3.connect(db_path) as conn:
        df = _pd.read_sql_query(sql_txt, conn)
        return df
    conn.close()


def df_to_sqlite(df, db_path, table_name, index=False, if_exists='replace', chunksize=5000):
    """Create SQLite table from pandas DataFrame

    Args:
        df (pd.DataFrame): DataFrame to convert to SQLite table.
        db_path (string): path/to/sqlite.db
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
        db_path (string): path/to/sqlite.db

    Returns:
        pandas.DataFrame
    """

    sql_txt = r"SELECT name AS TABLE_NAME FROM sqlite_schema WHERE type='table' AND name NOT LIKE 'sqlite_%'"
    with _sqlite3.connect(db_path) as conn:
        df = _pd.read_sql_query(sql_txt, conn)
        return df
    conn.close()


def sqlite_drop_table(db_path, table_name):
    """Drop specified table in SQLite db.

    Args:
        db_path (string): path/to/sqlite.db
        table_name (string): Table name to drop.
    """
    
    sql_txt = f"DROP TABLE IF EXISTS {table_name}"
    with _sqlite3.connect(db_path) as conn:
        conn.execute(sql_txt)
        conn.commit()
    conn.close()


def sqlite_create_table(db_path, table_name, col_dtypes):
    """Create specified table in SQLite db.

    Args:
        db_path (string): path/to/sqlite.db
        table_name (string): Table name to create.
        col_dtypes (string): 'col1 dtype, col2 dtype...coln dtype'
    """

    sql_txt = f"CREATE TABLE IF NOT EXISTS {table_name}({col_dtypes})"
    with _sqlite3.connect(db_path) as conn:
        conn.execute(sql_txt)
        conn.commit()
    conn.close()


def sqlite_insert_row(conn, table_name, col_names_tuple, row_vals_tuple):
    """Inserts a row into database/table

    Args:
        conn (sqlite connection object): sqlite connection obj
        table_name (string): Table to insert row into
        col_names_tuple (tuple): Tuple of col names
        row_vals_tuple (tuple): Tuple of col values
    """

    placeholder_list = f"({['?' for _ in col_names_tuple]})".replace('[', '').replace(']', '').replace("'?'", "?")

    sql_txt = f"INSERT INTO {table_name} {str(col_names_tuple)} VALUES {placeholder_list}"
    conn.execute(sql_txt, row_vals_tuple)
    conn.commit()


def sqlite_conn_cursor(db_path):
    """Create a sqlite connection & cursor obj

    Args:
        db_path (string): path/to/sqlite.db
    """

    conn = _sqlite3.connect(db_path)
    c = conn.cursor()

    return (conn, c)


def sqlite_delete_rows(db_path, table_name, where):
    """Delete rows from table.

    Args:
        db_path (string): path/to/sqlite.db
        table_name (_type_): Table to delete rows from
        where (_type_): Criteria for deletion.
    """
    sql_txt = f"DELETE FROM {table_name} WHERE {where};"
    with _sqlite3.connect(db_path) as conn:
        conn.execute(sql_txt)
        conn.commit()
    conn.close()
