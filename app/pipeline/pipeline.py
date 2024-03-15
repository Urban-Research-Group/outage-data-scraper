import pandas as pd
import ast
import pytz
import os
import json
import yaml
from dateutil import tz
from datetime import datetime
from datetime import timedelta
from IPython.display import display

pd.set_option('display.max_columns', None)

class BasePipeline:
    def __init__(self, config, base_file_path):
        self.config = config
        self.base_file_path = base_file_path
        self.map = {}
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
            print(file_path)
            self._data = pd.read_csv(file_path)
            with open(f"{self.config['state']}_.json", 'r') as json_file:
                self.map = json.load(json_file)
        except Exception as e:
            print(f"An error occurred during file loading: {e}")
            
    def transform(self):
        # Base transformation method
        raise NotImplementedError

    def standardize(self):
        # Base standardization
        self.load_data()
        self.transform()
        grouped = self._data.groupby('outage_id').apply(self._compute_metrics).reset_index().round(2)
        self._data = pd.merge(grouped, self._data, on=['outage_id', 'timestamp'], how='inner')
        
        self._data['utility_provider'] = self.config['name']
        self._data['state'] = self.config['state']
        self._data['county'] = self._data['zipcode'].map(self.map)
        
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
        
    def _check_other_vars(self):
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
    def __init__(self, config, base_file_path):
        super().__init__(config, base_file_path)
        with open('us_.json', 'r') as json_file:
            self._usmap = json.load(json_file)
    
    def transform(self): # if edited, must recreate pipeline to reset transformed flag
        #### HELPER METHOD
        def _reformat_start_date(row): # taking the row and reformating its 'start_date' based on timestamp
            # Split the date string into components
            # start_date format: 03/15 05:28 pm
            month_day, time, ampm = row['start_date'].split(' ') # taking the row's start_date and parsing it
            # Split the month and day and determine year
            s_month, s_day = month_day.split('/')
            year = None
            # Determining year using timestamp as start_date does not include year
            if pd.notna(row['timestamp']): # if the timestamp value for the given row is not null (hopefully it works)
            # timestamp format: 01-18-2024 15:25:06
                timestamp_components = row['timestamp'].split(' ')
                ts_date_comp = timestamp_components[0].split('-')
                t_month, t_day = ts_date_comp[0], ts_date_comp[1]
                t_year = pd.to_numeric(ts_date_comp[2])
                if t_month == '01' and s_month == '12':
                    # if the timestamp is in january but the start time is reported to be in december, make the update time have the previous year (year of Jan - 1)
                    year = str(int(t_year) - 1)
                else: # use timestamp year
                    year = t_year 
            else: # if no timestamp
            # for Walton, Tri-State, Oconee, and Mitchell, the na timestamps are in march 2023
                year = '2023'

            # Extract the hour and minute from the time
            hour, minute = time.split(':')

            # Convert hour to 24-hour format
            if 'am' in ampm.lower() and hour == '12': # if 12 am, then set to 00
                hour = '00' 
            if 'pm' in ampm.lower():
                hour = str(int(hour) + 12) if int(hour) < 12 else hour

            # Add leading zeros if necessary
            hour = hour.zfill(2)
            minute = minute.zfill(2)

            # Combine the components into the desired format
            reformatted_date = f'{s_month}-{s_day}-{year} {hour}:{minute}:00'

            return reformatted_date


        # Helper method
        def _reformat_update(row):
            # Format: Mar 15, 5 09, pm
            ## Splitting updateTime into components month, day, hour, min, am/pm
            month_day, time, ampm = row['updateTime'].split(',') # split into date; time; and pm/am 
            # Splitting into month, day
            u_month, u_day = month_day.split(' ')
            month_dict = { 'Jan': '01', 'Feb': '02', 'Mar': '03', 'Apr': '04', 'May': '05', 'Jun': '06', 
                        'Jul': '07', 'Aug': '08', 'Sep': '09', 'Oct': '10', 'Nov': '11', 'Dec': '12' }
            u_month = month_dict[u_month] # turning the 3 char month name to its two digit form
            
            # Determining year from timestamp as updateTime does not have year
            year = None
            if pd.notna(row['timestamp']): # if the timestamp value for the given row is not null (hopefully it works)
                # timestamp format: 01-18-2024 15:25:06
                timestamp_components = row['timestamp'].split(' ')
                ts_date_comp = timestamp_components[0].split('-')
                t_month, t_day = ts_date_comp[0], ts_date_comp[1]
                t_year = pd.to_numeric(ts_date_comp[2])

                if t_month == '01' and u_month == '12':
                    # if the timestamp is in january but the start time is reported to be in december, make the update time have the previous year (year of Jan - 1)
                    year = str(int(t_year) - 1)
                else: # use timestamp year
                    year = t_year 
            else: # if no timestamp
            # for Walton, Tri-State, Oconee, and Mitchell, the na timestamps are in march 2023
                year = '2023'

            # Extract the hour and minute from the update time
            hour, minute = time.split() # splits based on whtie space which is space in this case and removed leading space
            # Convert hour to 24-hour format
            if 'am' in ampm.lower() and hour == '12': # if 12 am, then set to 00
                hour = '00' 
            if 'pm' in ampm.lower():
                hour = str(int(hour) + 12) if int(hour) < 12 else hour # if pm and less than 12, add 12 hours  --> othertest it is 12 pm

            # Add leading zeros if necessary
            hour = hour.zfill(2)
            minute = minute.zfill(2)

            # Combine the components into the desired format
            reformatted_date = f'{u_month}-{u_day}-{year} {hour}:{minute}:00'

            return reformatted_date
        ## HELPER METHOD ####
        

        try:
            self._data['start_date'] = self._data.apply(_reformat_start_date, axis=1) # reformattin the string to be datetime string form
            self._data['start_date'] = pd.to_datetime(self._data['start_date']) # change to timestamp data format
            self._data['updateTime'] = self._data.apply(_reformat_update, axis=1)
            self._data['updateTime'] = pd.to_datetime(self._data['updateTime'])
            self._data['duration'] = pd.to_timedelta(self._data['duration'])
            self._data['timestamp'] = pd.to_datetime(self._data['timestamp']) # THIS HAS TO BE BEFORE applying reformat functions as splitting only works on timestamp in string form, not datetime

            # Renaming column names to match with superclass standardize
            self._data = self._data.rename(columns={
                'incident_id':'outage_id',
                'zip_code':'zipcode'
            })
            self._transformed = True
        except Exception as e:
            print(f"An error occurred during transformation: {e}")

    def standardize(self):
        # Specific transformation for GA11TX12
        # print(self.config)
        self.load_data()
        self.transform()
        grouped = self._data.groupby('outage_id').apply(self._compute_metrics).reset_index().round(2)
        self._data = grouped

    def _compute_metrics(self, group): # overwriting super class because the most accurate duration seems to be calculated from update times
        # group = groupby groupy of a unique outage_id
        ## HELPER METHOD TO VALIDATE DIFFERENT TIME
                
        duration_diff = group['duration'].max()
        duration_max = duration_diff + timedelta(minutes=15) # because 15 minute update intervals
        duration_mean = (duration_diff + duration_max) / 2
        end_time = group['updateTime'].max()
        start_time = end_time - duration_diff
        customer_affected_mean = group['consumers_affected'].mean()
        total_customer_outage_time = customer_affected_mean * duration_diff
        zipcode = group['zipcode'].iloc[-1]
        zipcode_values = None
        
        null_zipcode = [None, None, None] 
        try:
            zipcode_values = self.map[zipcode] # the tuple of values from zipcode map (county name, fip, state) 
        except KeyError:
            try:
                zipcode_values = self._usmap[zipcode]
            except KeyError:
                # print(f"Nonexistent zipcode in {self.config['name']}: {zipcode}")        
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
    def standardize(self, outage_data):
        # Specific transformation for CA1
        pass

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