from pathlib import Path
from pandas import read_csv, DataFrame
from mariachis.utils import date_vars, create_polygon, make_clusters

class Bbva:
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

    def comunity_catalog(self, df, zipcode_col='zipcode', comunity_cols=['comunity_code','comunity']) -> DataFrame:
        cat = read_csv(self.cp_filepath)
        df = df.merge(cat[[zipcode_col]+comunity_cols])
        return df

    def full_pipeline(self, comunity_level=False, date_col='day', omit_zero=['avg_amount','cards'], merge_zipcode=False, cluster_kwargs={}, polygon_kwargs={}, **pivot_kwargs) -> DataFrame:
        zipcode = create_polygon(self.cp_filepath, **polygon_kwargs)
        df = self.merge_bbva()
        for col in omit_zero: df = df[df[col]!=0].copy()
        if comunity_level: df = self.comunity_catalog(df)
        df = date_vars(df, date_col)
        df = df.pivot_table(**pivot_kwargs)
        df.columns = ['_'.join(str(y) for y in x) for x in df.columns]
        df = df[sorted(df.columns)].copy()
        df['cluster'], pipe_obj = make_clusters(df, df.columns, **cluster_kwargs)
        if merge_zipcode: result = df.reset_index().merge(zipcode)
        else: result = df.reset_index()
        return result, pipe_obj

    def export_bbva(self, df, export_name='Finished_BBVA.csv'):
        df.to_csv(self.base_dir.joinpath(export_name), index=False)
        print(f'Exported succesfully!\nFile:\t{export_name}\nPath:\t{self.base_dir}')


from geopandas import sjoin

class Recursos:
    def __init__(self, base_dir, folder_name, file_name, cp_file) -> None:
        self.base_dir = Path(base_dir)
        self.file_path = self.base_dir.joinpath(folder_name,file_name)
        self.cp_path = self.base_dir.joinpath(cp_file)

    def __str__(self) -> str:
        return f'File: {self.file_path}'

    def intersect_polygon(self, dissolve_by='zipcode', id_col='nombre') -> DataFrame:
        pol = create_polygon(self.cp_path, dissolve_by=dissolve_by)
        df = create_polygon(self.file_path, lat_col='latitud', lng_col='longitud', just_geodf=True)
        result = sjoin(df, pol, how="inner", op='intersects').drop_duplicates([id_col,'latitud','longitud']).reset_index(drop=True)
        return result

    def full_pipeline(self, group_col=['comunity_code','comunity','place'], type_col='tipo', **intersect_kwargs) -> DataFrame:
        df = self.intersect_polygon(**intersect_kwargs)
        df['n'] = 1
        df = df.pivot_table(index=group_col, columns=type_col, aggfunc={'n':'sum'}, fill_value=0)
        df.columns = [x[-1] for x in df.columns]
        return df.reset_index()