"""
This module is used to work with Google Sheets in Google Drive
"""

import gspread as _gspread
import gspread_dataframe as _gdf
from google.colab import auth as _auth
from google.auth import default as _default


def df_to_gsheet(df, gsheet_name='DFSheet'):
    gc = gc_object()
    gc.create(gsheet_name) 
    sheet = gc.open(gsheet_name).sheet1
    _gdf.set_with_dataframe(sheet, df)



def gsheet_to_df(df, gsheet_name, sheet_name=None):
    gc = gc_object()
    try:
        sheet = gc.open(gsheet_name).worksheet(sheet_name)
    except:
        sheet = gc.open(gsheet_name).sheet1

    df = _gdf.get_as_dataframe(sheet)

    return df



def gc_object():
    _auth.authenticate_user()
    creds, _ = _default()

    gc = _gspread.authorize(creds)

    return gc