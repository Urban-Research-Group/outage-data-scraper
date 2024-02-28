import pandas as pd
import ast
import pytz
import os
import json
import yaml
import glob
from dateutil import tz
from datetime import datetime

class BasePipeline:
    def __init__(self, config, base_file_path):
        self.config = config
        self.base_file_path = base_file_path
        self.geomap = {}
        self._data = pd.DataFrame({})
    
    def construct_file_path(self):
        #TODO: add type to prefix mapping
        file_prefix = 'per_outage' if self.config['type'] == 'o' else 'per_county'
        file_path = f"{self.base_file_path}/{self.config['state']}/layout_{self.config['layout']}/{file_prefix}_{self.config['name']}.csv"
        return file_path.replace('//', '/')

    def load_data(self):
        try:
            file_path = self.construct_file_path()
            print(file_path)
            self._data = pd.read_csv(file_path)
            with open('zip_to_county_name.json', 'r') as json_file:
                self.geomap['zip_to_county_name'] = json.load(json_file)
            with open('zip_to_county_fips.json', 'r') as json_file:
                self.geomap['zip_to_county_fips'] = json.load(json_file)
        except Exception as e:
            print(f"An error occurred during file loading: {e}")
            
    def transform(self):
        raise NotImplementedError

    def standardize(self):
        """
        Generic method to compute and output standardized metrics
        """
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
    def standardize(self, outage_data):
        # Specific transformation for GA2TX17
        pass
    
class GA3TX16(BasePipeline):
    def standardize(self, outage_data):
        # Specific transformation for GA3TX16
        pass
    
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
    def standardize(self, outage_data):
        # Specific transformation for GA11TX12
        pass
    
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