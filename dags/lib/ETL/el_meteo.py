class loader(
):
        
    '''
    This class has the purpose of extract and loading the source data into the PG instance. 3 main outputs 

        - RAW_PROVINCE: merges together PROVINCE and REGIONS for computing optimization
        - RAW_CITIES: all cities in source file
        - RAW_HOURLY_METEO: all the hourly meteo records in sources  
    '''
    def __init__(self, dir, conn = 'meteo_db'):
        self.dir = dir
        self.conn = conn

    def walk_files(self):
        '''
            finds all files in a specific path
        '''
        import os
        f = []
        for (dirpath, dirnames, filenames) in os.walk(self.dir):
            for filename in filenames:
                # Join the directory path with the filename to get the full file path
                full_file_path = os.path.join(dirpath, filename)
                f.append(full_file_path)
        return(f)

    def extract_meteo(self):
        '''
            extract and make the data compatible to SQL-like systems
        '''
        import pandas as pd
        import gzip
        import json
        from datetime import datetime as dt

        files = self.walk_files()
        df = pd.DataFrame()
        
        for file in files:
            with gzip.open(file, 'r') as l:
                bytes = l.read() 
                json_data = json.loads(bytes.decode('utf-8'))
            data = pd.DataFrame(json_data[0]['hourly']['data'])
            data['latitude'] = json_data[0]['latitude']
            data['longitude'] = json_data[0]['longitude']
            data['city'] = file.split('/')[-1].replace('.jsonl.gz','').lower()
            data['elevation'] = json_data[0]['elevation']
            data['time_converted'] = data['time'].apply(lambda x: dt.fromtimestamp(x))
            data = data[['summary', 'precipIntensity', 'precipAccumulation', 'precipType', 'temperature', 'apparentTemperature', 'dewPoint', 'pressure', 'windSpeed', 'windGust', 'windBearing', 'cloudCover', 'snowAccumulation', 'latitude', 'longitude', 'city', 'elevation', 'time_converted']]
            df = pd.concat([df,data])
        
        return(df)
    
    def find_region_for_province(self, province_geom, df_regions, col_name='region_name'):
        '''
            finds whether a specific polygon province is within a polygon region 
        '''
        from shapely.wkt import loads
        import pandas as pd
        
        if isinstance(df_regions, pd.DataFrame):
            for _, region in df_regions.iterrows():
                if loads(province_geom).within(loads(region['region_boundaries'])):
                    return region[col_name]
        else:
            raise ValueError("df_regions should be a pandas DataFrame")
        
        return None
    
    def find_province_for_city(self,row, province_df):
        '''
            finds whether a GIS point is within a polygon province
        '''
        from shapely.wkt import loads
        import pandas as pd
        if isinstance(province_df, pd.DataFrame):
            for _, provincia in province_df.iterrows():
                if loads(provincia['province_boundaries']).contains(row):
                    return provincia['province_name']
            return None  
        else:
            raise ValueError("province_df should be a pandas DataFrame")    
        
    def transform_cities(self, cities):
        '''
            transform instance for cities source file
        '''
        from shapely.geometry import Point

        cities['lat'] = cities['lat'].str.replace(',', '.').astype(float)
        cities['lon'] = cities['lon'].str.replace(',', '.').astype(float)
        cities['geom_point'] = cities.apply(lambda row: Point(row['lon'],row['lat']),axis = 1)
    
        return cities        
    
    def transform_province(self):
        '''
            transform instance for merging together region and province source files, through GIS configurations
        '''

        import pandas as pd
        import os

        provinces = pd.read_json(os.path.join(self.dir, 'provinces.jsonl'), lines=True)
        regions = pd.read_json(os.path.join(self.dir, 'regions.jsonl'), lines=True)

        cities = pd.read_csv(os.path.join(self.dir, 'cities.csv'), sep=';')
        cities = self.transform_cities(cities=cities)

        provinces['region'] = provinces['province_boundaries'].apply(lambda x: self.find_region_for_province(province_geom= x, df_regions= regions, col_name= 'region_name'))
        provinces['region_istat'] = provinces['province_boundaries'].apply(lambda x: self.find_region_for_province(province_geom= x, df_regions= regions, col_name= 'region_istat'))

        sigle = cities[cities['flag_capoluogo'] == 'SI'][['sigla_provincia','geom_point']]
        sigle['province_name'] = sigle['geom_point'].apply(lambda x: self.find_province_for_city(x,provinces))
        
        prov = provinces.merge(sigle, on='province_name')[['sigla_provincia','province_name','province_istat_code','region','region_istat','province_boundaries']].drop_duplicates()
        return prov
    
    def load(self, file):
        '''
            semi-dynamic runner for loading the data into the PG instance. The Load function can be seen as a RUN instance in a BaseOperator
        '''
        from sqlalchemy import create_engine
        from airflow.hooks.base_hook import BaseHook


        conn = BaseHook.get_connection(self.conn)
        username = conn.login
        password = conn.password
        host = conn.host
        port = conn.port
        database = conn.schema

        engine = create_engine(f"postgresql://{username}:{password}@{host}:{port}/{database}")

        if file == 'hourly':
            df = self.extract_meteo()

            df.to_sql('raw_hourly_meteo',con=engine,if_exists='replace',index=False)

        elif file == 'province':
            import pandas as pd
            import os

            df = self.transform_province()    
            df.to_sql(f'raw_province',con=engine,if_exists='replace',index=False)
        
        elif file == 'city':
            import pandas as pd
            import os       
            from shapely.wkt import loads

            df = pd.read_csv(os.path.join(self.dir,'cities.csv'), sep=';')
            df = self.transform_cities(cities=df)
            df.drop('geom_point',1,inplace=True)

            df.to_sql(f'raw_city',con=engine,if_exists='replace',index=False)
