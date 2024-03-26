import pandas as pd
import ast
import pytz
import os
import json
import yaml
import glob
from arcgis.geocoding import reverse_geocode
from arcgis.geometry import Geometry
from arcgis.gis import GIS
from dateutil import tz
from datetime import datetime
from datetime import timedelta
from IPython.display import display

pd.set_option('display.max_columns', None)

class BasePipeline:
    def __init__(self, config, base_file_path):
        self.config = config
        self.base_file_path = base_file_path
        self.geomap = {}
        self._data = pd.DataFrame({})
    
    def construct_file_path(self):
        #TODO: add type to prefix 
        file_prefix = 'per_outage' if self.config['type'] == 'o' else 'per_county'
        file_path = f"{self.base_file_path}/{self.config['state']}/layout_{self.config['layout']}/{file_prefix}_{self.config['name']}.csv"
        return file_path.replace('//', '/')

    def load_data(self):
        # TODO: use us zipcode database
        try:
            file_path = self.construct_file_path()
            self._data = pd.read_csv(file_path)
            with open('zip_to_county_name.json', 'r') as json_file:
                self.geomap['zip_to_county_name'] = json.load(json_file)
            with open('zip_to_county_fips.json', 'r') as json_file:
                self.geomap['zip_to_county_fips'] = json.load(json_file)
            with open('zip_to_state_name.json', 'r') as json_file:
                self.geomap['zip_to_state_name'] = json.load(json_file)
        except Exception as e:
            print(f"An error occurred during file loading: {e}")
            
    def transform(self):
        raise NotImplementedError

    def standardize(self):
        # Base transformation method
        self.load_data()
        self.transform()
        grouped = self._data.groupby('outage_id').apply(self._compute_metrics).reset_index().round(2)
        self._data = pd.merge(grouped, self._data, on=['outage_id', 'timestamp'], how='inner')
        
        self._data['state'] = self.config['state']
        if self.config['state'] != 'ca':
            self._data['utility_provider'] = self.config['name'] 
            self._data['county'] = self._data['zipcode'].map(self.geomap) 
        
        self._data = self._data[[
            'utility_provider', 'state', 'county', 'zipcode',
            'outage_id', 'start_time', 'end_time', 'lat', 'lng', 
            'duration', 'duration_max', 'duration_mean', 'customer_affected_mean', 'total_customer_outage_time', 'total_customer_outage_time_max', 'total_customer_outage_time_mean'
        ]]
        
        return self._data
    
    def output_data(self, standard_data):
        # TODO: Output unified data
        pass
    
    def get_dataframe(self):
        return self._data
    
    def _compute_metrics(self, group):
        """
        Generic method to compute standardized metrics, used for being apply in DataFrame.groupby method, 
        given dataframe being transformed with standardized column names
        """
        duration = (group['end_time'] - group['start_time']).dt.total_seconds() / 60
        duration_max = duration + 15
        duration_mean = (duration + duration_max) / 2
        customer_affected_mean = group['customer_affected'].mean()
        
        total_customer_outage_time = 15 * (group['customer_affected'].sum() - group['customer_affected'].iloc[0]) + (group['timestamp'].iloc[0] - group['start_time'].iloc[0]).total_seconds() / 60 * group['customer_affected'].iloc[0]
        total_customer_outage_time_max = total_customer_outage_time + 15 * group['customer_affected'].iloc[-1]
        total_customer_outage_time_mean = (total_customer_outage_time + total_customer_outage_time_max) / 2

        return pd.Series({
            'timestamp': group['end_time'].iloc[-1],
            'duration': duration.iloc[-1],
            'duration_max': duration_max.iloc[-1],
            'duration_mean': duration_mean.iloc[-1],
            'customer_affected_mean': customer_affected_mean,
            'total_customer_outage_time': total_customer_outage_time,
            'total_customer_outage_time_max': total_customer_outage_time_max,
            'total_customer_outage_time_mean': total_customer_outage_time_mean
        })
        
    def check_vars(self):
        # TODO: Check other useful variables
        pass
    
class GA1TX8(BasePipeline):
    def transform(self):
        try:
            # Convert timestamps
            eastern = tz.gettz('US/Eastern')
            utc = tz.gettz('UTC')
            self._data['timestamp'] = pd.to_datetime(self._data['timestamp'], utc=True).dt.tz_convert(eastern)
            self._data['outageStartTime'] = pd.to_datetime(self._data['outageStartTime'], utc=True).dt.tz_convert(eastern)
            self._data['end_time'] = self._data.groupby('outageRecID')['timestamp'].transform('max')
            
            # extract lat and long
            self._data['outagePoint'] = self._data['outagePoint'].apply(lambda x: json.loads(x.replace("'", '"')))
            self._data[['lat', 'lng']] = self._data['outagePoint'].apply(lambda x: pd.Series([x['lat'], x['lng']]))
            # TODO: add zipcode NaN checking
            self._data = self._data.rename(columns={
                'outageRecID':'outage_id',
                'outageStartTime': 'start_time',
                'customersOutNow':'customer_affected',
                'zip':'zipcode'
            })
        except Exception as e:
            print(f"An error occurred during transformation: {e}")

class GA2TX17(BasePipeline):
    def transform(self):
        """
        Formatting issues with startTime as some times have decimal seconds
        Extracting long and lat from location string
        """
        dataframe = self._data
        def reformat_starttime(startTime): 
            # format: 2023-03-15T21:51:54-04:00
            reformatted = startTime
            if pd.notna(startTime) and "." in startTime: 
                first_period_index = startTime.find('.')
                first_hyphen_after_period_index = startTime.find('-', first_period_index)
                reformatted = startTime[:first_period_index] + startTime[first_hyphen_after_period_index:]
            return reformatted
        
        try:
            eastern = tz.gettz('US/Eastern')
            utc = tz.gettz('UTC')
            dataframe['OutageStartTime'] = dataframe['OutageStartTime'].apply(reformat_starttime)
            dataframe['timestamp'] = pd.to_datetime(dataframe['timestamp'], utc=True).dt.tz_convert(eastern)
            dataframe['OutageStartTime'] = pd.to_datetime(dataframe['OutageStartTime'], format='mixed', utc=True).dt.tz_convert(eastern)
            dataframe['OutageEndTime'] =pd.to_datetime(dataframe['OutageEndTime'], format='mixed', utc=True).dt.tz_convert(eastern)

            dataframe['OutageLocation'] = dataframe['OutageLocation'].apply(lambda x: json.loads(x.replace("'", '"')))
            dataframe[['lat', 'long']] = dataframe['OutageLocation'].apply(lambda x: pd.Series([x['Y'], x['X']]))
            
            dataframe.rename(columns={
                'OutageRecID':'outage_id',
                'OutageStartTime': 'start_time',
                'CustomersOutNow':'customer_affected',
                'EMC': 'utility_provider',
                'zip': 'zipcode'
            }, inplace=True)
        except Exception as e:
            print(f"An error occurred during transformation: {e}")

    def standardize(self): 
        self.load_data()
        self.transform()
        grouped = self._data.groupby('outage_id').apply(self._compute_metrics).reset_index().round(2)
        self._data = grouped

    
    gis = GIS("http://www.arcgis.com", "JK9035", "60129@GR0W3R5") # signing in to get access to arcGIS api
    def _compute_metrics(self, group):
        """
        Raw data has no zipcodes so must calculate them using the long, lat using arcGIS api
        """
        def get_zipcode(long, lat):
            location = reverse_geocode((Geometry({"x":float(long), "y":float(lat), "spatialReference":{"wkid": 4326}})))
            return location['address']['Postal']
        
        start_time = group['start_time'].min()
        duration_diff = group['timestamp'].max() - group['timestamp'].min()
        end_time = start_time + duration_diff
        lat = group['lat'].iloc[-1]
        long = group['long'].iloc[-1]
        zipcode = get_zipcode(long, lat)
        county_name = self.geomap['zip_to_county_name'][zipcode] if (pd.notna(zipcode) and zipcode != '') else pd.NA
        county_fips = self.geomap['zip_to_county_fips'][zipcode] if (pd.notna(zipcode) and zipcode != '') else pd.NA
        state = self.geomap['zip_to_state_name'][zipcode] if (pd.notna(zipcode) and zipcode != '') else pd.NA
        utility_provider = group['utility_provider'].iloc[-1]
        duration_max = duration_diff + timedelta(minutes=15)
        duration_mean = (duration_diff + duration_max) / 2
        customer_affected_mean = group['customer_affected'].mean()
        total_customer_outage_time = customer_affected_mean * duration_diff

        return pd.Series({
            'start_time': start_time,
            'end_time': end_time,
            'lat': lat,
            'long': long,
            'zipcode': zipcode,
            'county_name': county_name,
            'county_fips': county_fips,
            'state': state,
            'utility_provider': utility_provider,
            'duration_max': duration_max,
            'duration_mean': duration_mean,
            'customer_affected_mean': customer_affected_mean,
            'total_customer_outage_time': total_customer_outage_time
        })
    
class GA3TX16(BasePipeline):
    def transform(self):
        try:
            # Convert timestamps
            eastern = tz.gettz('US/Eastern')
            self._data['timestamp'] = pd.to_datetime(self._data['timestamp'], utc=True).dt.tz_convert(eastern)
            self._data['OutageTime'] = pd.to_datetime(self._data['OutageTime'], utc=True).dt.tz_convert(eastern)
            self._data['end_time'] = self._data.groupby('CaseNumber')['timestamp'].transform('max')
            
             # TODO: get zipcode from lat and long
            self._data['zipcode'] = "000000"
            
            self._data = self._data.rename(columns={
                'CaseNumber':'outage_id',
                'OutageTime': 'start_time',
                'CutomersAffected':'customer_affected',
                'X':'lat',
                'Y': 'lng'
            })
        except Exception as e:
            print(f"An error occurred during transformation: {e}")
    
class GA4TX5(BasePipeline):
    def standardize(self, outage_data):
        # Specific transformation for GA4TX5
        pass
    
class GA5(BasePipeline):
    def standardize(self, outage_data):
        # Specific transformation for GA5
        pass
    
class GA7(BasePipeline):
    def standardize(self, outage_data):
        # Specific transformation for GA4TX5
        pass
    
class GA9TX11(BasePipeline):
    def standardize(self, outage_data):
        # Specific transformation for GA9TX11
        pass
    
class GA10(BasePipeline):
    def standardize(self, outage_data):
        # Specific transformation for GA10
        pass
    
class GA11TX12(BasePipeline):    
    def __init__(self, config, base_file_path):
        super().__init__(config, base_file_path)
        with open('us_mapping.json', 'r') as json_file:
            self._usmap = json.load(json_file)
    
    def transform(self):
        """
        Formatting issues with start_date and updateTime
        start_date format: 03/15 05:28 pm
        timestamp format: 01-18-2024 15:25:06 (For Walton, Tri-State, Oconee, and Mitchell, there are null timestamps in March 2023)
        updateTime format: Mar 15, 5 09, pm

        Remove outages with multiple start dates due to not there being many
        """
        
        def _reformat_start_date(row):
            month_day, time, ampm = row['start_date'].split(' ')
            s_month, s_day = month_day.split('/')
            year = None
            # Determining year using timestamp as start_date does not include year
            if pd.notna(row['timestamp']): 
                timestamp_components = row['timestamp'].split(' ')
                ts_date_comp = timestamp_components[0].split('-')
                t_month, t_day = ts_date_comp[0], ts_date_comp[1]
                t_year = pd.to_numeric(ts_date_comp[2])
                if t_month == '01' and s_month == '12':
                    year = str(int(t_year) - 1)
                else:
                    year = t_year 
            else:
            # for Walton, Tri-State, Oconee, and Mitchell, the na timestamps are in march 2023
                year = '2023'

            hour, minute = time.split(':')

            if 'am' in ampm.lower() and hour == '12':
                hour = '00' 
            if 'pm' in ampm.lower():
                hour = str(int(hour) + 12) if int(hour) < 12 else hour

            # Add leading zeros if necessary
            hour = hour.zfill(2)
            minute = minute.zfill(2)

            reformatted_date = f'{s_month}-{s_day}-{year} {hour}:{minute}:00'

            return reformatted_date


        def _reformat_update(row):
            month_day, time, ampm = row['updateTime'].split(',') 
            u_month, u_day = month_day.split(' ')
            month_dict = { 'Jan': '01', 'Feb': '02', 'Mar': '03', 'Apr': '04', 'May': '05', 'Jun': '06', 
                        'Jul': '07', 'Aug': '08', 'Sep': '09', 'Oct': '10', 'Nov': '11', 'Dec': '12' }
            u_month = month_dict[u_month]
            
            year = None
            if pd.notna(row['timestamp']):
                timestamp_components = row['timestamp'].split(' ')
                ts_date_comp = timestamp_components[0].split('-')
                t_month, t_day = ts_date_comp[0], ts_date_comp[1]
                t_year = pd.to_numeric(ts_date_comp[2])
                if t_month == '01' and u_month == '12':
                    year = str(int(t_year) - 1)
                else:
                    year = t_year 
            else:
                year = '2023'

            hour, minute = time.split()
            if 'am' in ampm.lower() and hour == '12':
                hour = '00' 
            if 'pm' in ampm.lower():
                hour = str(int(hour) + 12) if int(hour) < 12 else hour
            hour = hour.zfill(2)
            minute = minute.zfill(2)

            reformatted_date = f'{u_month}-{u_day}-{year} {hour}:{minute}:00'
            return reformatted_date

        try:
            self._data['start_date'] = self._data.apply(_reformat_start_date, axis=1) 
            self._data['start_date'] = pd.to_datetime(self._data['start_date']) 
            self._data['updateTime'] = self._data.apply(_reformat_update, axis=1)
            self._data['updateTime'] = pd.to_datetime(self._data['updateTime'])
            self._data['duration'] = pd.to_timedelta(self._data['duration'])
            self._data['timestamp'] = pd.to_datetime(self._data['timestamp']) 

            # Renaming column names to match with superclass standardize
            self._data = self._data.rename(columns={
                'incident_id':'outage_id',
                'zip_code':'zipcode'
            })
            
            # Eliminating outage_id's with multiple start dates
            df = self._data
            grouped = df.groupby('outage_id')['start_date'].nunique()
            multi_start_outages = grouped[grouped > 1].index.tolist()
            self._data = df[~df['outage_id'].isin(multi_start_outages)]
        except Exception as e:
            print(f"An error occurred during transformation: {e}")

    def standardize(self):
        self.load_data()
        self.transform()
        grouped = self._data.groupby('outage_id').apply(self._compute_metrics).reset_index().round(2)
        self._data = grouped

    def _compute_metrics(self, group): 
        """
        Overwriting superclass _compute_metrics as duration is given in this layout is more accurate
        """
        duration_diff = group['duration'].max()
        duration_max = duration_diff + timedelta(minutes=15) # because 15 minute update intervals
        duration_mean = (duration_diff + duration_max) / 2
        start_time = group['start_date'].iloc[0]
        end_time = start_time + duration_diff
        customer_affected_mean = group['consumers_affected'].mean()
        total_customer_outage_time = customer_affected_mean * duration_diff
        zipcode = group['zipcode'].iloc[-1]
        zipcode_values = None
        
        null_zipcode = [None, None, None] 
        try:
            zipcode_values = self.map[zipcode] 
        except KeyError:
            try:
                zipcode_values = self._usmap[zipcode]
            except KeyError:    
                zipcode_values = null_zipcode

        return pd.Series({
            'start_time': start_time,
            'end_time': end_time,
            'lat': group['lat'].iloc[-1],
            'long': group['lon'].iloc[-1],
            'zipcode': zipcode,
            'county_name': zipcode_values[0], 
            'county_fips': zipcode_values[1],
            'utility_provider': self.config['name'],
            'state': zipcode_values[2],
            'duration_diff': duration_diff,
            'duration_max': duration_max,
            'duration_mean': duration_mean,
            'customer_affected_mean': customer_affected_mean,
            'total_customer_outage_time': total_customer_outage_time
        })
    
class CA1(BasePipeline):
    def load_data(self):
        try:
            dir_path = f"{self.base_file_path}/{self.config['state']}/layout_{self.config['layout']}/"
            csv_files = glob.glob(os.path.join(dir_path, "*.csv"))
            df_list = [pd.read_csv(file) for file in csv_files]
            self._data = pd.concat(df_list, ignore_index=True)
            
            with open('zip_to_county_name.json', 'r') as json_file:
                self.geomap['zip_to_county_name'] = json.load(json_file)
            with open('zip_to_county_fips.json', 'r') as json_file:
                self.geomap['zip_to_county_fips'] = json.load(json_file)
        except Exception as e:
            print(f"An error occurred during file loading: {e}")
    
    def transform(self):
        try:
            # Convert timestamps
            eastern = tz.gettz('US/Eastern')
            utc = tz.gettz('UTC')
            self._data['StartDate'] = pd.to_datetime(self._data['StartDate'], utc=True).dt.tz_convert(eastern)
            self._data['timestamp'] = pd.to_datetime(self._data['timestamp'], utc=True).dt.tz_convert(eastern)
            
            # Since there's no direct 'end_time' in the new dataset, assuming 'EstimatedRestoreDate' serves a similar purpose
            self._data['end_time'] = self._data.groupby('OBJECTID')['timestamp'].transform('max')
            
            # TODO: get zipcode from lat and long
            self._data['zipcode'] = "000000"
            
            self._data = self._data.rename(columns={
                'x': 'lat',
                'y': 'lng',
                'OBJECTID': 'outage_id',
                'StartDate': 'start_time',
                'ImpactedCustomers': 'customer_affected',
                'UtilityCompany': 'utility_provider',
                'County': 'county'
            })
        except Exception as e:
            print(f"An error occurred during transformation: {e}")

class CA2(BasePipeline):
    def standardize(self, outage_data):
        # Specific transformation for CA2
        pass
    
class TX1(BasePipeline):
    def standardize(self, outage_data):
        # Specific transformation for TX1
        pass
    
class TX4(BasePipeline):
    def standardize(self, outage_data):
        # Specific transformation for TX4
        pass
    
class TX6(BasePipeline):
    def standardize(self, outage_data):
        # Specific transformation for TX6
        pass

class TX10(BasePipeline):
    def standardize(self, outage_data):
        # Specific transformation for TX10
        pass

class TX18(BasePipeline):
    def standardize(self, outage_data):
        # Specific transformation for TX18
        pass