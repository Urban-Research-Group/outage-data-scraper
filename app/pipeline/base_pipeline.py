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
        self.type_to_prefix = {'o': 'per_outage', 'c': 'per_county', 'z': 'per_zipcode'} 
        self._data = pd.DataFrame({})
        self.geomap = {}
        self._load_data()
        self._load_geo_mapping()
        
    def _load_geo_mapping(self):
        try:
            with open('zip_to_county_name.json', 'r') as json_file:
                self.geomap['zip_to_county_name'] = json.load(json_file)
            with open('zip_to_county_fips.json', 'r') as json_file:
                self.geomap['zip_to_county_fips'] = json.load(json_file)
            with open('zip_to_state_name.json', 'r') as json_file:
                self.geomap['zip_to_state_name'] = json.load(json_file)
        except Exception as e:
            print(f"An error occurred during geo map loading: {e}") 
        
    def _construct_file_path(self):
        file_prefix = self.type_to_prefix[self.config['type']]
        file_path = f"{self.base_file_path}/{self.config['state']}/layout_{self.config['layout']}/{file_prefix}_{self.config['name']}.csv"
        return file_path.replace('//', '/')

    def _load_data(self):
        try:
            file_path = self._construct_file_path()
            self._data = pd.read_csv(file_path)
        except Exception as e:
            print(f"An error occurred during file loading: {e}")
            
    def transform(self, **kwargs):
        raise NotImplementedError
    
    # TODO: remove 
    def standardize(self):
        """
        obslete
        """
        self._load_data()
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
    
    def to_incident_level(self, identifers=['outage_id'], method='id_grouping'):
        """
        identifer: default identifier name "outage_id", or list like ['IncidentId', 'lat', 'lng', 'subgroup']
        method: "id_grouping" or "timegap_seperation" 
        """
        df = self.transform(identifiers=identifers, method=method)
        grouped = df.groupby('id').apply(self._agg_vars).reset_index().round(2)
        
        return grouped
    
    def to_geoarea_level(self, geo_level='zipcode', time_interval='hourly'):
        """
        geo_level: 'zipcode', 'county', 'state'
        time_interval: 'hourly', 'daily', 'monthly'
        """    
        # TODO: complet geo_level and time_interval support
        eastern = tz.gettz('US/Eastern')
        self._data['timestamp'] = pd.to_datetime(self._data['timestamp'], utc=True).dt.tz_convert(eastern)
        self._data['year'] = self._data['timestamp'].dt.year
        self._data['month'] = self._data['timestamp'].dt.month
        self._data['day'] = self._data['timestamp'].dt.day
        self._data['hour'] = self._data['timestamp'].dt.hour
        
        df = self.transform(geo_level=geo_level, time_interval=time_interval)
        
        # element wise metrics computation
        df['duration_weight'] = 15
        df['outage_freq_x_cust_a'] = df['customer_affected'] * df['outage_count']
        df['cust_a_x_duration'] = df['customer_affected'] * df['duration_weight']
        
        # TODO: complet geo_level and time_interval support 
        keys = ['EMC', 'year', 'month', 'day', 'hour', geo_level]
        grouped = df.groupby(keys).agg({
            'customer_affected': 'mean',
            'customer_served': 'mean',
            'percent_customer_affected': 'mean',
            'outage_count': 'max',
            'duration_weight': 'sum',
            'outage_freq_x_cust_a': 'sum',
            'cust_a_x_duration': 'sum'
            }).reset_index()
        
        #TODO: fill non outage hours with 0
        
        return grouped
    
    def _agg_vars(self, group):
        first_timestamp = group['timestamp'].iloc[0]
        last_timestamp = group['timestamp'].iloc[-1]
        duration_diff = (last_timestamp - first_timestamp).total_seconds() / 60
        duration_15 = 15 * len(group)
        group['duration_weight'] = (group['timestamp'].diff().dt.total_seconds() / 60).round(0).fillna(15)
        cust_affected_x_duration = (group['customer_affected'] * group['duration_weight']).sum()
        cust_a_mean = cust_affected_x_duration / group['duration_weight'].sum()
        
        return pd.Series({
            'first_timestamp': first_timestamp,
            'last_timestamp': last_timestamp,
            'duration_diff': duration_diff,
            'duration_15': duration_15,
            'customer_affected_mean': cust_a_mean,
            'cust_affected_x_duration': cust_affected_x_duration
        })
    
    # TODO: remove 
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
    
    def save(self, path=None):
        raise NotImplementedError
    
    def get_dataframe(self):
        return self._data
    
    def _add_metadata(self):
        """
        #TODO: add state, provider variables
        """
        raise NotImplementedError
        
    def check_vars(self):
        # TODO: Check other useful variables
        pass