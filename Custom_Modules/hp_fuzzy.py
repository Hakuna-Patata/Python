"""hp_fuzzy
Functions for fuzzy matching
"""

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