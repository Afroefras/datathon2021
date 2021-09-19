from re import findall
from pathlib import Path
from geopandas import sjoin
from pandas import read_csv, DataFrame
from mariachis.utils import date_vars, create_polygon, make_clusters, clean_text

class BaseClass:
    def __init__(self, base_dir:str, folder_name:str, file_name:str, cp_file:str) -> None:
        self.base_dir = Path(base_dir)
        self.file_name = file_name
        self.file_path = self.base_dir.joinpath(folder_name,file_name)
        self.cp_path = self.base_dir.joinpath(cp_file)

    def __str__(self) -> str:
        return f'File: {self.file_path}'

    def export_result(self, df, export_name=None):
        if export_name==None: export_name=f'Finished_{self.file_name}'
        df.to_csv(self.base_dir.joinpath(export_name), index=False)
        print(f'Exported succesfully!\nFile:\t{export_name}\nPath:\t{self.base_dir}')

###############################################################################################################

class Pubs(BaseClass):
    def __init__(self, base_dir:str, folder_name:str, file_name:str, cp_file:str) -> None:
        super().__init__(base_dir, folder_name, file_name, cp_file)

    def __str__(self) -> str:
        return super().__str__()

    def merge_cp_pubs(self, left_on='Municipio', right_on='comunity', value_col='Valor') -> DataFrame:
        cat = read_csv(self.cp_path)
        df = read_csv(self.file_path)
        df[f'clean_{left_on}'] = df[left_on].map(lambda x: clean_text(x, lower=True, rem_stopw=True))
        cat[f'clean_{right_on}'] = cat[right_on].map(lambda x: clean_text(x, lower=True, rem_stopw=True))
        df = df.merge(cat, left_on=f'clean_{left_on}', right_on=f'clean_{right_on}').dropna(subset=[value_col])
        return df

    def full_pipeline_pubs(self, cluster_kwargs={}, **pivot_kwargs) -> DataFrame:
        df = self.merge_cp_pubs()
        df = df.pivot_table(**pivot_kwargs)
        df.columns = ['_'.join(str(y) for y in x) for x in df.columns]
        df = df[sorted(df.columns)].copy()
        df['cluster'], pipe_obj = make_clusters(df, df.columns, **cluster_kwargs)
        return df.reset_index(), pipe_obj

###############################################################################################################

class Recursos(BaseClass):
    def __init__(self, base_dir:str, folder_name:str, file_name:str, cp_file:str) -> None:
        super().__init__(base_dir, folder_name, file_name, cp_file)

    def __str__(self) -> str:
        return super().__str__()

    def intersect_polygon_recursos(self, dissolve_by='zipcode', id_col='nombre') -> DataFrame:
        pol = create_polygon(self.cp_path, dissolve_by=dissolve_by)
        df = create_polygon(self.file_path, lat_col='latitud', lng_col='longitud', just_geodf=True)
        result = sjoin(df, pol, how="inner", op='intersects').drop_duplicates([id_col,'latitud','longitud']).reset_index(drop=True)
        return result

    def full_pipeline_recursos(self, group_col=['comunity_code','comunity','place'], type_col='tipo', **intersect_kwargs) -> DataFrame:
        df = self.intersect_polygon_recursos(**intersect_kwargs)
        df['n'] = 1
        df = df.pivot_table(index=group_col, columns=type_col, aggfunc={'n':'sum'}, fill_value=0)
        df.columns = [x[-1] for x in df.columns]
        return df.reset_index()
        
###############################################################################################################

class Bbva(BaseClass):
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

    def full_pipeline_bbva(self, comunity_level=False, date_col='day', omit_zero=['avg_amount','cards'], merge_zipcode=False, cluster_kwargs={}, polygon_kwargs={}, **pivot_kwargs) -> DataFrame:
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

###############################################################################################################
        
class Weather(BaseClass):
    def __init__(self, base_dir:str, cp_file:str, folder_name:str, from_year:int, to_year:int, filename_prefix='maestro_cm05_') -> None:
        self.base_dir = Path(base_dir)
        self.cp_path = self.base_dir.joinpath(cp_file)
        self.folder_path = self.base_dir.joinpath(folder_name)
        self.all_files = self.folder_path.glob('*.csv')
        self.from_year = from_year
        self.to_year = to_year
        self.weather_files = []
        for x in self.all_files:
            file_name = str(x).split('/')[-1]
            years_range = '|'.join([str(x) for x in range(self.from_year,self.to_year+1)])
            pattern_found = findall(filename_prefix+years_range, file_name)
            if len(pattern_found)>0: self.weather_files.append(x)

    def __len__(self) -> tuple:
        return len(self.weather_files)

    def __str__(self) -> str:
        return f'Folder path:\t{self.folder_path}\nWith {self.__len__()} files about daily weather between {self.from_year} and {self.to_year}'

    def read_weather(self) -> DataFrame:
        df = DataFrame()
        for file_path in self.weather_files:
            df_sub = read_csv(file_path).dropna()
            df = df.append(df_sub, ignore_index=True)
        return df

    def intersect_polygon_recursos(self, fullfile_path, dissolve_by='zipcode', id_col='nombre') -> DataFrame:
        pol = create_polygon(self.cp_path, dissolve_by=dissolve_by)
        df = create_polygon(fullfile_path, lat_col='latitud', lng_col='longitud', just_geodf=True)
        result = sjoin(df, pol, how="inner", op='intersects').drop_duplicates([id_col,'latitud','longitud']).reset_index(drop=True)
        return result

    def full_pipeline_weather(self, cluster_kwargs={}, polygon_kwargs={}, **pivot_kwargs):
        df = self.read_weather()
        groupfile_path = self.base_dir.joinpath('group_weather.csv')
        df.to_csv(groupfile_path, index=False)
        df = create_polygon(groupfile_path, lat_col='latitud', lng_col='longitud', just_geodf=True)
        pol = create_polygon(self.cp_path, dissolve_by='zipcode')
        df = sjoin(df, pol, how="inner", op='intersects').drop_duplicates(pivot_kwargs['index']+pivot_kwargs['columns']).reset_index(drop=True)
        df = df.pivot_table(**pivot_kwargs)
        df.columns = ['_'.join(str(y) for y in x) for x in df.columns]
        df = df[sorted(df.columns)].copy()
        df['cluster'], pipe_obj = make_clusters(df, df.columns, **cluster_kwargs)
        df.reset_index(inplace=True)
        return df, pipe_obj