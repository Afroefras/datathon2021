from string import ascii_uppercase
from pandas import DataFrame, to_datetime

def date_vars(df, date_col:str) -> DataFrame:
    df[date_col] = to_datetime(df[date_col])
    df[f'{date_col}_year'] = df[date_col].dt.year.map(str)
    df[f'{date_col}_quarter'] = df[date_col].dt.quarter.map(lambda x: str(x).zfill(2))
    df[f'{date_col}_month'] = df[date_col].dt.month.map(lambda x: str(x).zfill(2))
    df[f'{date_col}_yearquarter'] = df[f'{date_col}_year']+' - '+df[f'{date_col}_quarter']
    df[f'{date_col}_yearmonth'] = df[f'{date_col}_year']+' - '+df[f'{date_col}_month']
    return df

from sklearn.cluster import KMeans
from sklearn.pipeline import Pipeline
from sklearn.mixture import GaussianMixture
from sklearn.preprocessing import RobustScaler

def make_clusters(df, cluster_cols, n_clusters=5, kmeans=False):
    scaler_obj = RobustScaler()
    cluster_obj = KMeans(n_clusters, random_state=22) if kmeans else GaussianMixture(n_clusters, random_state=22)
    pipe_obj = Pipeline(steps=[('scaler', scaler_obj), ('cluster', cluster_obj)])
    cluster_dict = dict(zip(range(n_clusters), ascii_uppercase[:n_clusters]))
    df['cluster'] = pipe_obj.fit_predict(df[cluster_cols])
    df['cluster'] = df['cluster'].map(cluster_dict)
    return df['cluster'], pipe_obj

def profiles(df, cluster_col='cluster'):
    prof = {}
    num_cols = df.head(1).describe().columns.tolist()
    prof['numeric'] = df.pivot_table(index=cluster_col, values=num_cols)
    cat_cols = [x for x in df.columns if x not in num_cols]
    df['n'] = 1
    for col in cat_cols:
        prof[col] = df.pivot_table(index=col, columns=cluster_col, aggfunc={'n':sum})
    return prof