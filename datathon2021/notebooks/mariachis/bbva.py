from pathlib import Path
from pandas import read_csv, DataFrame
from geopandas import GeoDataFrame, points_from_xy
from mariachis.utils import date_vars, make_clusters

class BbvaClass:
    def __init__(self, base_dir:str, cp_file:str, folder_name:str) -> None:
        self.base_dir = Path(base_dir)
        self.cp_filepath = self.base_dir.joinpath(cp_file)
        self.folder_path = self.base_dir.joinpath(folder_name)
        self.avg_am_files = list(self.folder_path.glob('avg_am*.csv'))
        self.cards_files = list(self.folder_path.glob('cards*.csv'))

    def __len__(self) -> tuple:
        return (len(self.avg_am_files),len(self.cards_files))

    def __str__(self) -> str:
        len_files = self.__len__()
        return f'Folder path:\t{self.folder_path}\nWith {len_files[0]} files about average amount and {len_files[-1]} about cards'

    def read_country(self, path_list, country_col='country', country='ES') -> DataFrame:
        df = DataFrame()
        for file_path in path_list:
            df_sub = read_csv(file_path)
            df_sub = df_sub[df_sub[country_col]==country].copy()
            df = df.append(df_sub, ignore_index=True)
        return df

    def merge_bbva(self) -> DataFrame:
        avg_am = self.read_country(self.avg_am_files)
        cards = self.read_country(self.cards_files)
        df = avg_am.merge(cards)
        return df

    def make_pivot(self, date_col='day', omit_zero=['avg_amount','cards'], **pivot_kwargs) -> DataFrame:
        df = self.merge_bbva()
        for col in omit_zero: df = df[df[col]!=0].copy()
        df = date_vars(df, date_col)
        df = df.pivot_table(**pivot_kwargs)
        df.columns = ['_'.join(str(y) for y in x) for x in df.columns]
        df = df[sorted(df.columns)].copy()
        return df

    def create_polygon(self, crs_code="EPSG:3395", lat_col='lat', lng_col='lng', zipcode_col='zipcode') -> DataFrame:
        df = read_csv(self.cp_filepath)
        gdf = GeoDataFrame(df, crs=crs_code, geometry=points_from_xy(df[lat_col], df[lng_col]))
        df = gdf.dissolve(by=zipcode_col)
        df['geometry'] = df['geometry'].buffer(0.05)
        df.reset_index(inplace=True)
        return df

    def full_pipeline(self, cluster_kwargs={}, polygon_kwargs={}, **make_pivot_kwargs):
        zipcode = self.create_polygon(**polygon_kwargs)
        df = self.make_pivot(**make_pivot_kwargs)
        df['cluster'], pipe_obj = make_clusters(df, df.columns, **cluster_kwargs)
        result = df.reset_index().merge(zipcode)
        return result, pipe_obj

    def export_bbva(self, df, export_name='Finished_BBVA.csv'):
        df.to_csv(self.base_dir.joinpath(export_name), index=False)
        print(f'Exported succesfully!\nFile:\t{export_name}\nPath:\t{self.base_dir}')