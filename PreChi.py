"""
function: Based on sklearn, The enhanced pipeline and featureUion classes implement partial parallel functionality
"""
#author:Chimantian

import numpy as np
from sklearn.pipeline import FeatureUnion, _transform_one, _fit_transform_one, _fit_one
from joblib import Parallel, delayed
from scipy import sparse

class FeatureUnionChi(FeatureUnion):
    """Concatenates results of multiple transformer object

    In addition to the class FeatureUnion, it implement partial input data parallel processing

    """

    def __init__(self,
                 idx_list,
                 transformer_list, n_jobs=None,
                 transformer_weights=None, verbose=False,
                 ):
        FeatureUnion.__init__(transformer_list=transformer_list, n_jobs=n_jobs,
                              transformer_weights=transformer_weights, verbose=verbose)
        self.idx_list = idx_list

    def fit(self, X, y=None, **fit_param):
        transformer_idx_list = map(lambda trans, idx:(trans[0], trans[1], idx), self.transformer_list, self.idx_list)

        transformers = Parallel(n_jobs=self.n_jobs)(delayed(_fit_one)
        (
            transformer, X[:, idx], y) for name, transformer, idx in transformer_idx_list
        )

        if not transformers:
            return self
        self._update_transformer_list(transformers)
        return self

    def transform(self,  X):
        transformer_idx_list = map(lambda trans, idx:(trans[0], trans[1], idx), self.transformer_list, self.idx_list)
        Xs = Parallel(n_jobs=self.n_jobs)(
            delayed(_transform_one)(trans, X[:, idx], None)
            for name, trans, idx in transformer_idx_list
        )
        if not Xs:
            return np.zeros((X.shape[0], 0))
        if any(sparse.issparse(f) for f in Xs):
            Xs = sparse.hstack(Xs).tocsr()
        else:
            Xs = np.hstack(Xs)
        return Xs

    def fit_transform(self, X, y=None, **fit_params):
        transformer_idx_list = map(lambda trans, idx:(trans[0], trans[1], idx), self.transformer_list, self.idx_list)
        results = Parallel(n_jobs=self.n_jobs)(
            delayed(_fit_transform_one)(trans, X[:, idx], None)
            for name, trans, idx in transformer_idx_list
        )



