# -*- coding: utf-8 -*-
"""Based on diffenent types to classified DataFrame
"""
#author:Chimantian
#version:1.0

from pandas import DataFrame
import pandas as pd

class ClassifiedDataFrameChi:
    """
    Attribute
    ----------
    cat_dict: dict shape of (key: value)
        key: object of the selected properties
        value: list of the columns index of the DataFrame
        dictionary of selected properties
    """
    def __init__(self):
        pass

    def fit(self, X, y=None):
        """
        :param
        ----------
        X: DataFrame shape of (n_samples, n_catagories)
        input Data

        :parameters
        ----------
        y:numpy array shape of (n_catagoris, )
        selected properties
        specifies the column properties to select.
        If None, all DataFrame properties are selected.

        :return:
        """
        self.cat_dict = self._get_cat(X, y)

    def transform(self, X):
        """
        :param
        -----------
        X:DataFrame of (n_samples, n_catagories)
           input Data
        :return:
        -----------
        [re_df, re_cat]:numpy shape of (2, )
            re_df: DataFrame shape of (n_samples. col)
                col:columns of selected types
            re_cat:type of value in re_df
        """
        re_df = []
        re_cat = []
        cat_dict_key = list(self.cat_dict.keys())
        for i in range(len(cat_dict_key)):

            re_df.append(df.iloc[:, [j for j in self.cat_dict[cat_dict_key[i]]]])
            re_cat.append(cat_dict_key[i])
        return [re_df, re_cat]


    def _get_cat(self, df, catagory=None):
        """
        :param
        ----------
        df: DataFrame of (n_samples, n_catagories)
        input Data

        :parameters
        ----------
        catagory:numpy array shape of (n_catagoris, )
        selected properties
        specifies the column properties to select.
        If None, all DataFrame properties are selected.

        :return:
        ----------
        cat_dict:dit shape of (key:value)
           key: the catagories of the input data
           value: the columns index of DataFrame selecting attribute key
        """
        if catagory:
            cat = set(catagory)
        else:
            cat = set(df.dtypes)
        cat_dict = {key: [] for key in cat}
        df_cat = df.dtypes
        for i in range(df_cat.shape[0]):
            cat_dict[df_cat[i]].append(i)
        return cat_dict






