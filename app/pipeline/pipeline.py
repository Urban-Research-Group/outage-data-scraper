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
    def transform(self, geo_level):
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
            
            if geo_level != 'incident':
                self._data['customer_served'] = 0
                self._data['percent_customer_affected'] = 0
                
                # Aggregate count of unique outageRecID
                count_df = self._data.groupby(['zipcode', 'year', 'month', 'day', 'hour'])['outage_id'].nunique().reset_index()
                count_df.rename(columns={'outage_id': 'outage_count'}, inplace=True)

                # Merge aggregated DataFrame with the original DataFrame
                self._data = pd.merge(self._data, count_df, on=['zipcode', 'year', 'month', 'day', 'hour'], how='left')
                
        except Exception as e:
            print(f"An error occurred during transformation: {e}")
    
    # TODO: override base class method by adding layout specific duration estimate, i.e. from different start_time, end_time candidate
    def _id_grouping(self, group):
        start_time = group['timestamp'].iloc[0]
        end_time = group['timestamp'].iloc[-1]
        duration_diff = (end_time - start_time).total_seconds() / 60
        duration_15 = 15 * len(group)
        group['duration_weight'] = (group['timestamp'].diff().dt.total_seconds() / 60).round(0).fillna(15)
        cust_affected_x_duration = (group['customer_affected'] * group['duration_weight']).sum()
        cust_a_mean = cust_affected_x_duration / group['duration_weight'].sum()
        
        return pd.Series({
            'latitude': group['lat'].unique(),
            'longitude': group['lng'].unique(),
            'zipcode': group['zipcode'].unique(),
            'start_time': start_time,
            'end_time': end_time,
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
        def transform(self, geo_level):
            self._data = self._data.rename(columns={
                'name':'zipcode',
                'cust_a':'customer_affected',
                'cust_s': 'customer_served',
                'percent_cust_a':'percent_customer_affected',
                'n_out':'outage_count'
            })
    
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
          
        #    with open('zip_to_county_name.json', 'r') as json_file:
        #        self.geomap['zip_to_county_name'] = json.load(json_file)
        #    with open('zip_to_county_fips.json', 'r') as json_file:
        #        self.geomap['zip_to_county_fips'] = json.load(json_file)
       except Exception as e:
           print(f"An error occurred during file loading: {e}")
  
   def transform(self):
       try:
           # TEMP FILTER
           self._data = self._data[self._data['UtilityCompany'] != 'LAWP']
           # self._data = self._data[(self._data['utility_provider'] != 'LAWP') & (self._data['OutageType'] != 'PLANNED')]
          
           # Convert timestamps
           eastern = tz.gettz('US/Eastern')
           pacific = tz.gettz('US/Pacific')
           utc = tz.gettz('UTC')
           self._data['StartDate'] = pd.to_datetime(self._data['StartDate'], utc=True).dt.tz_convert(pacific)
           self._data['timestamp'] = pd.to_datetime(self._data['timestamp'], utc=True).dt.tz_convert(eastern)
           self._data = self._data.sort_values(by='timestamp')
          
           # subgroup by 15 min rule, must derive subgroup step by step
           self._data['gap'] = (self._data.groupby(['IncidentId', 'x', 'y'])['timestamp'].diff().dt.total_seconds() / 60).round(0).fillna(0)
           self._data['is_gap'] = (self._data.groupby(['IncidentId', 'x', 'y'])['timestamp'].diff() > pd.Timedelta(minutes=20))
           self._data['subgroup'] = self._data.groupby(['IncidentId', 'x', 'y'])['is_gap'].cumsum()
          
           # outage_id candidate
           self._data['IncidentId+StartDate'] = self._data['IncidentId'].astype(str) + '_' + self._data['StartDate'].dt.strftime('%Y-%m-%d %H:%M:%S')
           self._data['coord'] = list(zip(self._data['x'], self._data['y']))
           self._data['IncidentId+Coord'] = self._data['IncidentId'].astype(str) + '_' + self._data['coord'].astype(str)
           self._data['StartDate+Coord'] = self._data['StartDate'].dt.strftime('%Y-%m-%d %H:%M:%S') + '_' + self._data['coord'].astype(str)
           self._data['IncidentId+StartDate+Coord'] = self._data['StartDate'].dt.strftime('%Y-%m-%d %H:%M:%S') + '_' + self._data['coord'].astype(str) + '_' + self._data['IncidentId'].astype(str)
          
           # TODO: get zipcode from lat and long
           self._data['zipcode'] = "000000"
          
           self._data = self._data.rename(columns={
               'x': 'lat',
               'y': 'lng',
               # id_candidate: 'outage_id',
               # 'StartDate': 'start_time',
               'ImpactedCustomers': 'customer_affected',
               'UtilityCompany': 'utility_provider',
               'County': 'county'
           })
       except Exception as e:
           print(f"An error occurred during transformation: {e}")
  
   def standardize(self, method='identifier', outage_id='IncidentId+StartDate+Coord'):
       '''
       method: 'identifier' or 'timegap'
       outage_id: id_candidate
       '''
       self._load_data()
       self.transform()
      
       if method == 'identifier':
           grouped = self._data.groupby(outage_id).apply(self._identifier_rule).reset_index().round(2)
           self._data = pd.merge(grouped, self._data, on=[outage_id, 'timestamp'], how='inner')
          
           self._data['state'] = self.config['state']
           self._data['is_planned'] = self._data['OutageType']
           self._data = self._data[[
               'utility_provider', 'state', 'county', 'zipcode',
               'outage_id', 'start_time', 'end_time', 'is_planned', 'cause', 'lat', 'lng',
               'duration', 'duration_max', 'duration_mean', 'customer_affected_mean', 'total_customer_outage_time', 'total_customer_outage_time_max', 'total_customer_outage_time_mean'
           ]]
       elif method == 'timegap':
           self._data = self._data.groupby(['IncidentId', 'lat', 'lng', 'subgroup']).apply(self._timegap_rule).reset_index()
              
       return self._data
  
   def _identifier_rule(self, group):
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
  
   def _timegap_rule(self, group):
       county = group['county'].unique()
       IncidentId = group['IncidentId'].unique()
       x = group['lat'].unique()
       y = group['lng'].unique()
       OutageType = group['OutageType'].unique()
       cause = group['Cause'].unique()
       n_startdate = group['StartDate'].nunique()
       n_OBJECTID = group['OBJECTID'].nunique()
      
       start_time = group['timestamp'].iloc[0]
       end_time = group['timestamp'].iloc[-1]
       duration_diff = (end_time - start_time).total_seconds() / 60
       duration_15 = 15 * len(group)
       duration_diff_minus_15 = duration_diff - duration_15
       cust_a_mean = group['customer_affected'].mean()

       return pd.Series({
           'county': county,
           'IncidenId': IncidentId,
           'lat': x,
           'lng':y,
           'OutageType': OutageType,
           'Cause': cause,
           'start_time': start_time,
           'end_time': end_time,
           'duration_diff': duration_diff,
           'duration_15': duration_15,
           'duration_diff_minus_15': duration_diff_minus_15,
           'cust_a_mean': cust_a_mean,
           'n_StartDate': n_startdate,
           'n_OBJECTID': n_OBJECTID
       })

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