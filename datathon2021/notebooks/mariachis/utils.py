from pandas import DataFrame, to_datetime

def date_vars(df, date_col:str) -> DataFrame:
    df[date_col] = to_datetime(df[date_col])
    df[f'{date_col}_year'] = df[date_col].dt.year.map(str)
    df[f'{date_col}_quarter'] = df[date_col].dt.quarter.map(lambda x: str(x).zfill(2))
    df[f'{date_col}_month'] = df[date_col].dt.month.map(lambda x: str(x).zfill(2))
    df[f'{date_col}_yearquarter'] = df[f'{date_col}_year']+' - '+df[f'{date_col}_quarter']
    df[f'{date_col}_yearmonth'] = df[f'{date_col}_year']+' - '+df[f'{date_col}_month']
    return df

###############################################################################################################

from pandas import read_csv
from geopandas import GeoDataFrame, points_from_xy

def create_polygon(filepath:str, dissolve_by=None, crs_code="EPSG:3395", lat_col='lat', lng_col='lng', just_geodf=False) -> DataFrame:
        df = read_csv(filepath)
        gdf = GeoDataFrame(df, crs=crs_code, geometry=points_from_xy(df[lat_col], df[lng_col]))
        if just_geodf: return gdf
        df = gdf.dissolve(by=dissolve_by)
        df['geometry'] = df['geometry'].buffer(0.05)
        df.reset_index(inplace=True)
        return df

###############################################################################################################

from pandas import Series
from string import ascii_uppercase
from sklearn.cluster import KMeans
from sklearn.pipeline import Pipeline
from sklearn.mixture import GaussianMixture
from sklearn.preprocessing import RobustScaler

def make_clusters(df, cluster_cols:list, n_clusters=5, kmeans=False) -> tuple([Series,Pipeline]):
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

###############################################################################################################

from emoji import demojize
from re import sub, UNICODE
from unicodedata import normalize
from nltk.corpus import stopwords

def clean_text(text:str, language='spanish', pattern="[^a-zA-Z0-9\s]", add_stopw=[],
                lower=False, rem_stopw=False, unique=False, emoji=False) -> str:
    if emoji: text = demojize(text)
    cleaned_text = normalize('NFD',str(text).replace('\n',' \n ')).encode('ascii', 'ignore')
    cleaned_text = sub(pattern,' ',cleaned_text.decode('utf-8'),flags=UNICODE)
    cleaned_text = [word for word in (cleaned_text.lower().split() if lower else cleaned_text.split())]
    if rem_stopw: cleaned_text = [word for word in cleaned_text if word not in 
                                  stopwords.words(language)+add_stopw]
    return ' '.join((set(cleaned_text) if unique else cleaned_text))