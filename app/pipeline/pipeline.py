import pandas as pd
import ast
import pytz
import os
import json
import yaml
import glob
from dateutil import tz
from datetime import datetime
from base_pipeline import BasePipeline
    
class GA1TX8(BasePipeline):
    def transform(self, **kwargs):
        identifiers = kwargs.get('identifiers')
        method = kwargs.get('method')
        geo_level = kwargs.get('geo_level')
        time_interval = kwargs.get('time_interval')
        df = kwargs.get('dataframe', self._data.copy())
        
        df = df.rename(columns={
            'outageRecID':'outage_id',
            'outageStartTime': 'start_time',
            'customersOutNow':'customer_affected',
            'zip':'zipcode'
        })
        
        # Convert timestamps
        eastern = tz.gettz('US/Eastern')
        df['timestamp'] = pd.to_datetime(df['timestamp'], utc=True).dt.tz_convert(eastern)
        df['start_time'] = pd.to_datetime(df['start_time'], utc=True).dt.tz_convert(eastern)
        
        # extract lat and long
        df['outagePoint'] = df['outagePoint'].apply(lambda x: json.loads(x.replace("'", '"')))
        df[['latitude', 'longitude']] = df['outagePoint'].apply(lambda x: pd.Series([x['lat'], x['lng']]))
        # TODO: add zipcode NaN checking
        
        if identifiers:
            if method == 'timegap_seperation': 
                # subgroup by timegap rule, must derive subgroup step by step
                df['gap'] = (df.groupby(identifiers)['timestamp'].diff().dt.total_seconds() / 60).round(0).fillna(0)
                df['is_gap'] = (df.groupby(identifiers)['timestamp'].diff() > pd.Timedelta(minutes=20))
                df['subgroup'] = df.groupby(identifiers)['is_gap'].cumsum()
                identifiers.append('subgroup')
                
            df['id'] = df[identifiers].apply(tuple, axis=1)
        
        # for zipcode level transformation 
        if geo_level and time_interval:
            # TODO: complet geo_level and time_interval support
            keys = ['zipcode', 'year', 'month', 'day', 'hour'] 
            df['customer_served'] = 0
            df['percent_customer_affected'] = 0
            
            # Aggregate count of unique outage_id
            count_df = df.groupby(keys)['outage_id'].nunique().reset_index()
            count_df.rename(columns={'outage_id': 'outage_count'}, inplace=True)

            # Merge aggregated DataFrame with the original DataFrame
            df = pd.merge(df, count_df, on=keys, how='left')
        
        return df
    
    # TODO: refactor such that overriden method only include extra variables
    def _agg_vars(self, group):
        first_timestamp = group['timestamp'].iloc[0]
        last_timestamp = group['timestamp'].iloc[-1]
        duration_diff = (last_timestamp - first_timestamp).total_seconds() / 60
        duration_15 = 15 * len(group)
        group['duration_weight'] = (group['timestamp'].diff().dt.total_seconds() / 60).round(0).fillna(15)
        cust_affected_x_duration = (group['customer_affected'] * group['duration_weight']).sum()
        cust_a_mean = cust_affected_x_duration / group['duration_weight'].sum()
        
        return pd.Series({
            'latitude': group['latitude'].unique(),
            'longitude': group['longitude'].unique(),
            'zipcode': group['zipcode'].unique(),
            'start_time': group['start_time'].unique(),
            'first_timestamp': first_timestamp,
            'last_timestamp': last_timestamp,
            'duration_diff': duration_diff,
            'duration_15': duration_15,
            'customer_affected_mean': cust_a_mean,
            'cust_affected_x_duration': cust_affected_x_duration
        })

class GA2TX17(BasePipeline):
    def standardize(self, outage_data):
        # Specific transformation for GA2TX17
        pass
    
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
        def transform(self, **kwargs):
            df = kwargs.get('dataframe', self._data.copy())
            df = df.rename(columns={
                'name':'zipcode',
                'cust_a':'customer_affected',
                'cust_s': 'customer_served',
                'percent_cust_a':'percent_customer_affected',
                'n_out':'outage_count'
            })
            
            return df
    
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
    def _load_data(self):
        try:
            dir_path = f"{self.base_file_path}/{self.config['state']}/layout_{self.config['layout']}/"
            csv_files = glob.glob(os.path.join(dir_path, "*.csv"))
            df_list = [pd.read_csv(file) for file in csv_files]
            self._data = pd.concat(df_list, ignore_index=True)
        except Exception as e:
            print(f"An error occurred during file loading: {e}")
  
    def transform(self, **kwargs):
        identifiers = kwargs.get('identifiers')
        method = kwargs.get('method')
        geo_level = kwargs.get('geo_level')
        time_interval = kwargs.get('time_interval')
        df = kwargs.get('dataframe', self._data.copy())
        
        # standardize columns names
        df = df.rename(columns={
            'IncidentId': 'outage_id',
            'StartDate': 'start_time',
            'x': 'latitude',
            'y': 'longitude',
            'OutageType': 'is_planned',
            'Cause': 'cause',
            'ImpactedCustomers': 'customer_affected',
            'UtilityCompany': 'utility_provider',
            'County': 'county'
        })
        
        # Convert timestamps
        eastern = tz.gettz('US/Eastern')
        pacific = tz.gettz('US/Pacific')
        df['start_time'] = pd.to_datetime(df['start_time'], utc=True).dt.tz_convert(pacific)
        df['timestamp'] = pd.to_datetime(df['timestamp'], utc=True).dt.tz_convert(eastern)
        df = df.sort_values(by='timestamp')
        
        if identifiers:
            df['id'] = df[identifiers].apply(tuple, axis=1)
            if method == 'timegap_seperation': 
                # subgroup by timegap rule, must derive subgroup step by step
                df['gap'] = (df.groupby(identifiers)['timestamp'].diff().dt.total_seconds() / 60).round(0).fillna(0)
                df['is_gap'] = (df.groupby(identifiers)['timestamp'].diff() > pd.Timedelta(minutes=20))
                df['subgroup'] = df.groupby(identifiers)['is_gap'].cumsum()
        
        if geo_level and time_interval:
            # Extract year, month, day, hour from 'timestamp'
            df['year'] = df['timestamp'].dt.year
            df['month'] = df['timestamp'].dt.month
            df['day'] = df['timestamp'].dt.day
            df['hour'] = df['timestamp'].dt.hour
                
        # TODO: get zipcode from lat and long
        df['zipcode'] = "000000"
        
        return df
    
    def to_incident_level(self, **kwargs):
        """
        apply timegap_seperation rule to LAWP;
        apply id_grouping rule to SCE, PGE, SMUD, SDGE
        """
        lawp = self._data.query('UtilityCompany == "LAWP"')
        others = self._data.query('UtilityCompany in ["SCE", "SDGE", "PGE", "SMUD"]')
        
        # hardcoded ids for ca
        trans_lawp = self.transform(dataframe=lawp, identifiers=['utility_provider', 'outage_id', 'latitude', 'longitude'], method='timegap_seperation')
        std_lawp = trans_lawp.groupby(['utility_provider', 'outage_id', 'latitude', 'longitude']).apply(self._agg_vars).reset_index().round(2)
        
        trans_others = self.transform(dataframe=others, identifiers=['utility_provider', 'outage_id', 'latitude', 'longitude', 'start_time'], method='id_grouping')
        std_others = trans_others.groupby(['utility_provider', 'outage_id', 'latitude', 'longitude', 'start_time']).apply(self._agg_vars).reset_index().round(2)
        
        std_ca = pd.concat([std_lawp, std_others]).sort_values(by = 'first_timestamp')
        
        return std_ca

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
    def transform(self, **kwargs):
        identifiers = kwargs.get('identifiers')
        method = kwargs.get('method')
        geo_level = kwargs.get('geo_level')
        time_interval = kwargs.get('time_interval')
        df = kwargs.get('dataframe', self._data.copy())
        
        df = df.rename(columns={
            'id':'outage_id',
            'CUSTOMERSOUT':'customer_affected',
            'EMC':'utility_provider',
            'x':'longitude',
            'y':'latitude'
        })
        
        # Convert timestamps
        eastern = tz.gettz('US/Eastern')
        df['BEGINTIME'] = pd.to_datetime(df['BEGINTIME'], utc=True, errors='coerce').dt.tz_convert(eastern)
        df['BEGINTIME'] = pd.to_datetime(df['BEGINTIME'], utc=True).dt.tz_convert(eastern)
        df['timestamp'] = pd.to_datetime(self._data['timestamp'], utc=True, errors='coerce').dt.tz_convert(eastern)
        df['timestamp'] = pd.to_datetime(df['timestamp'], utc=True).dt.tz_convert(eastern)
        df['ESTIMATEDTIMERESTORATION'] = pd.to_datetime(self._data['ESTIMATEDTIMERESTORATION'], utc=True, errors='coerce').dt.tz_convert(eastern)
        df['ESTIMATEDTIMERESTORATION'] = pd.to_datetime(df['ESTIMATEDTIMERESTORATION'], utc=True).dt.tz_convert(eastern)

        df['start_time'] = df['BEGINTIME']
        # extract lat and long
        # df['outagePoint'] = df['outagePoint'].apply(lambda x: json.loads(x.replace("'", '"')))
        # df[['latitude', 'longitude']] = df['outagePoint'].apply(lambda x: pd.Series([x['lat'], x['lng']]))
        # TODO: add zipcode NaN checking
        
        if identifiers:
            if method == 'timegap_seperation': 
                # subgroup by timegap rule, must derive subgroup step by step
                df['gap'] = (df.groupby(identifiers)['timestamp'].diff().dt.total_seconds() / 60).round(0).fillna(0)
                df['is_gap'] = (df.groupby(identifiers)['timestamp'].diff() > pd.Timedelta(minutes=20))
                df['subgroup'] = df.groupby(identifiers)['is_gap'].cumsum()
                identifiers.append('subgroup')
                
            df['id'] = df[identifiers].apply(tuple, axis=1)
        
        # for zipcode level transformation 
        if geo_level and time_interval:
            # TODO: complet geo_level and time_interval support
            keys = ['zipcode', 'year', 'month', 'day', 'hour'] 
            df['customer_served'] = 0
            df['percent_customer_affected'] = 0
            
            # Aggregate count of unique outage_id
            count_df = df.groupby(keys)['outage_id'].nunique().reset_index()
            count_df.rename(columns={'outage_id': 'outage_count'}, inplace=True)

            # Merge aggregated DataFrame with the original DataFrame
            df = pd.merge(df, count_df, on=keys, how='left')
        return df
    
    # TODO: refactor such that overriden method only include extra variables
    def _agg_vars(self, group):
        first_timestamp = group['timestamp'].iloc[0]
        last_timestamp = group['timestamp'].iloc[-1]
        duration_diff = (last_timestamp - first_timestamp).total_seconds() / 60
        duration_15 = 15 * len(group)
        group['duration_weight'] = (group['timestamp'].diff().dt.total_seconds() / 60).round(0).fillna(15)
        cust_affected_x_duration = (group['customer_affected'] * group['duration_weight']).sum()
        cust_a_mean = cust_affected_x_duration / group['duration_weight'].sum()
        
        return pd.Series({
            'latitude': group['latitude'].unique(),
            'longitude': group['longitude'].unique(),
            # 'zipcode': group['zipcode'].unique(),
            'start_time': group['start_time'].unique(),
            'first_timestamp': first_timestamp,
            'last_timestamp': last_timestamp,
            'duration_diff': duration_diff,
            'duration_15': duration_15,
            'customer_affected_mean': cust_a_mean,
            'cust_affected_x_duration': cust_affected_x_duration
        })
    def standardize(self, outage_data):
        # Specific transformation for TX6
        pass

class TX10(BasePipeline):
    def transform(self, **kwargs):
        identifiers = kwargs.get('identifiers')
        method = kwargs.get('method')
        geo_level = kwargs.get('geo_level')
        time_interval = kwargs.get('time_interval')
        df = kwargs.get('dataframe', self._data.copy())
        
        df = df.rename(columns={
            'id':'outage_id',
            'custAffected':'customer_affected',
            'zip':'zipcode',
            'EMC':'utility_provider',
            'lon':'longitude',
            'lat':'latitude'
        })
        
        # Convert timestamps
        eastern = tz.gettz('US/Eastern')
        df['date'] = pd.to_datetime(df['date'].str[:-4], format='%B %d, %Y %I:%M %p', errors='coerce')
        df['date'] = df['date'].dt.tz_localize(None).dt.tz_localize('UTC').dt.tz_convert(eastern)
        df['timestamp'] = pd.to_datetime(df['timestamp'], utc=True, errors='coerce').dt.tz_convert(eastern)
        df['timestamp'] = pd.to_datetime(df['timestamp'], utc=True).dt.tz_convert(eastern)
        df['start_time'] = df['date']
        
        # extract lat and long
        # df['outagePoint'] = df['outagePoint'].apply(lambda x: json.loads(x.replace("'", '"')))
        # df[['latitude', 'longitude']] = df['outagePoint'].apply(lambda x: pd.Series([x['lat'], x['lng']]))
        # TODO: add zipcode NaN checking
        
        if identifiers:
            if method == 'timegap_seperation': 
                # subgroup by timegap rule, must derive subgroup step by step
                df['gap'] = (df.groupby(identifiers)['timestamp'].diff().dt.total_seconds() / 60).round(0).fillna(0)
                df['is_gap'] = (df.groupby(identifiers)['timestamp'].diff() > pd.Timedelta(minutes=20))
                df['subgroup'] = df.groupby(identifiers)['is_gap'].cumsum()
                identifiers.append('subgroup')
                
            df['id'] = df[identifiers].apply(tuple, axis=1)
        
        # for zipcode level transformation 
        if geo_level and time_interval:
            # TODO: complet geo_level and time_interval support
            keys = ['zipcode', 'year', 'month', 'day', 'hour'] 
            df['customer_served'] = 0
            df['percent_customer_affected'] = 0
            
            # Aggregate count of unique outage_id
            count_df = df.groupby(keys)['outage_id'].nunique().reset_index()
            count_df.rename(columns={'outage_id': 'outage_count'}, inplace=True)

            # Merge aggregated DataFrame with the original DataFrame
            df = pd.merge(df, count_df, on=keys, how='left')
        
        return df
    
    # TODO: refactor such that overriden method only include extra variables
    def _agg_vars(self, group):
        first_timestamp = group['timestamp'].iloc[0]
        last_timestamp = group['timestamp'].iloc[-1]
        duration_diff = (last_timestamp - first_timestamp).total_seconds() / 60
        duration_15 = 15 * len(group)
        group['duration_weight'] = (group['timestamp'].diff().dt.total_seconds() / 60).round(0).fillna(15)
        cust_affected_x_duration = (group['customer_affected'] * group['duration_weight']).sum()
        cust_a_mean = cust_affected_x_duration / group['duration_weight'].sum()
        
        return pd.Series({
            'latitude': group['latitude'].unique(),
            'longitude': group['longitude'].unique(),
            'zipcode': group['zipcode'].unique(),
            'start_time': group['start_time'].unique(),
            'first_timestamp': first_timestamp,
            'last_timestamp': last_timestamp,
            'duration_diff': duration_diff,
            'duration_15': duration_15,
            'customer_affected_mean': cust_a_mean,
            'cust_affected_x_duration': cust_affected_x_duration
        })
    def standardize(self, outage_data):
        # Specific transformation for TX10
        pass

class TX18(BasePipeline):
    def standardize(self, outage_data):
        # Specific transformation for TX18
        pass