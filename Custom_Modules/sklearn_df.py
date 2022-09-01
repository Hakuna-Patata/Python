"""
This module is used to combine SKlearn with Pandas
"""

from functools import reduce as _reduce
import pandas as _pd
from pandas import DataFrame as _DF
import numpy as _np
from itertools import chain as _chain
from joblib import Parallel as _Parallel, delayed as _delayed
from sklearn.compose import ColumnTransformer as _ColumnTransformer
from sklearn.preprocessing import (
    FunctionTransformer as _FunctionTransformer
    , OneHotEncoder as _OneHotEncoder
)
from sklearn.base import (
    TransformerMixin as _TransformerMixin
    , BaseEstimator as _BaseEstimator
)
from sklearn.pipeline import (
    FeatureUnion as _FeatureUnion
    , _fit_transform_one
    , _transform_one
)
from scipy import sparse as _sparse
from typing import List as _List, Any as _Any


class DFFunctionTransformer(_TransformerMixin, _BaseEstimator):
    """Class for applying any sklearn transformer as step in pipeline.
    """
    def __init__(self, *args, **kwargs):
        self.FT = _FunctionTransformer(*args, **kwargs)

    def fit(self, X, y=None):
        return self

    def transform(self, X):
        X_xfrm = self.FT.transform(X) 
        X_xfrm = _DF(X_xfrm, index=X.index, columns=X.columns)
        return X_xfrm


class DFFeatureUnion(_TransformerMixin, _BaseEstimator):
    # FeatureUnion but for pandas DataFrames

    def __init__(self, transformer_list):
        self.transformer_list = transformer_list

    def fit(self, X, y=None):
        for (name, t) in self.transformer_list:
            t.fit(X, y)
        return self

    def transform(self, X):
        # assumes X is a DataFrame
        Xts = [t.transform(X) for _, t in self.transformer_list]
        Xunion = _reduce(lambda X1, X2: _pd.merge(X1, X2, left_index=True, right_index=True), Xts)
        return Xunion

class DFColumnExtractor(_TransformerMixin, _BaseEstimator):
    """Class for extracting column in sklearn pipeline.
    """
    def __init__(self, cols):
        self.cols = cols

    def fit(self, X, y=None):
        return self

    def transform(self, X):
        Xcols = X[self.cols]
        return Xcols


class DFColumnDropper(_TransformerMixin, _BaseEstimator):
    """Class for dropping column step in sklearn pipeline.
    """
    def __init__(self, cols):
        self.cols = cols

    def fit(self, X, y=None):
        return self

    def transform(self, X):
        return X.drop(self.cols, axis=1)


class DFDummyTransformer(_TransformerMixin, _BaseEstimator):
    """Class for One-Hot Encoding step in sklearn pipeline.
    """
    def __init__(self, **kwargs):
        self.ohe = _OneHotEncoder(handle_unknown='ignore', sparse=False, **kwargs)

    def fit(self, X, y=None):
        self.ohe.fit(X)
        self.cols = self.ohe.get_feature_names_out()
        return self

    def transform(self, X):
        xfrm_array = self.ohe.transform(X)
        xfrm_df = _DF(xfrm_array, columns=self.cols, index=X.index)
        return xfrm_df


class DFStringTransformer(_TransformerMixin, _BaseEstimator):
    """Class for string conversion step in sklearn pipeline.
    """
    def fit(self, X, y=None):
        return self

    def transform(self, X):
        X_str = X.applymap(str)
        return X_str


class DFImputer(_TransformerMixin, _BaseEstimator):
    """Class for imputing step in sklearn pipeline.
    """
    def __init__(self, imputer):
        self.imputer = imputer
        self.stats_ = None

    def fit(self, X, y=None):
        self.imputer.fit(X)
        self.stats_ = _pd.Series(self.imputer.statistics_, index=X.columns)
        return self

    def transform(self, X):
        X_imputed_array = self.imputer.transform(X)
        X_imputed_df = _DF(X_imputed_array, index=X.index, columns=X.columns)
        return X_imputed_df


class DFScaler(_TransformerMixin, _BaseEstimator):
    """Class for scaling step in sklearn pipeline.
    """
    def __init__(self, scaler):
        self.scaler = scaler

    def fit(self, X, y=None):
        self.scaler.fit(X)
        return self 

    def transform(self, X):
        X_scaled_data = self.scaler.transform(X)
        X_scaled_df = _DF(X_scaled_data, index=X.index, columns=X.columns)
        return X_scaled_df


class DFDropNaN(_TransformerMixin, _BaseEstimator):
    """Class for NaN dropping step in sklearn pipeline.
    """
    def __init__(self, **kwargs):
        self.kwargs = kwargs

    def fit(self, X, y=None):
        return self 

    def transform(self, X):
        return X.dropna(**self.kwargs)
        