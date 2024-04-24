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
    def transform(self, **kwargs):
        """
        Formatting issues with startTime as some times have decimal seconds
        Extracting long and lat from location string
        """
        identifiers = kwargs.get('identifiers')
        method = kwargs.get('method')
        geo_level = kwargs.get('geo_level')
        time_interval = kwargs.get('time_interval')
        df = kwargs.get('dataframe', self._data.copy())

        df = df.rename(columns={
                'OutageRecID':'outage_id',
                'OutageStartTime': 'start_time',
                'CustomersOutNow':'customer_affected',
                'EMC': 'utility_provider',
                'zip': 'zipcode'
            })
        
        eastern = tz.gettz('US/Eastern')
        utc = tz.gettz('UTC')

        def reformat_starttime(startTime): 
            # format: 2023-03-15T21:51:54-04:00
            reformatted = startTime
            if pd.notna(startTime) and "." in startTime: 
                first_period_index = startTime.find('.')
                first_hyphen_after_period_index = startTime.find('-', first_period_index)
                reformatted = startTime[:first_period_index] + startTime[first_hyphen_after_period_index:]
            return reformatted
        
        try:
            df['start_time'] = df['start_time'].apply(reformat_starttime)
            df['timestamp'] = pd.to_datetime(df['timestamp'], utc=True).dt.tz_convert(eastern)
            df['start_time'] = pd.to_datetime(df['start_time'], format='mixed', utc=True).dt.tz_convert(eastern)
            df['OutageEndTime'] = pd.to_datetime(df['OutageEndTime'], format='mixed', utc=True).dt.tz_convert(eastern)
            df['zipcode'] = pd.NA if ('zipcode' not in df.columns) else df['zipcode']
            df['OutageLocation'] = df['OutageLocation'].apply(lambda x: json.loads(x.replace("'", '"')))
            df[['lat', 'long']] = df['OutageLocation'].apply(lambda x: pd.Series([x['Y'], x['X']]))


        except Exception as e:
            print(f"An error occurred during transformation: {e}")

        return df

    def to_incident_level(self, identifers=['outage_id', 'start_time'], method='id_grouping'):
        """
        identifer: default identifier name "outage_id", or list like ['IncidentId', 'lat', 'lng', 'subgroup']
        method: "id_grouping" or "timegap_seperation" 
        """
        df = self.transform(identifiers=identifers, method=method)
        grouped = df.groupby(identifers).apply(self._agg_vars).reset_index().round(2)
        
        return grouped
    
    # gis = GIS("http://www.arcgis.com", "JK9035", "60129@GR0W3R5") # signing in to get access to arcGIS api; have to find 
    def _agg_vars(self, group):
        """
        Raw data has no zipcodes so must calculate them using the long, lat using arcGIS api. 
        To do: arcGIS api is paywalled, so will have to find another way of calculate zipcode (without get_zipcode)
        """
        # def get_zipcode(long, lat):
        #     location = reverse_geocode((Geometry({"x":float(long), "y":float(lat), "spatialReference":{"wkid": 4326}})))
        #     return location['address']['Postal']

        first_timestamp = group['timestamp'].min()
        last_timestamp = group['timestamp'].max()
        duration_diff = last_timestamp - first_timestamp
        duration_15 = 15 * len(group)
        group['duration_weight'] = (group['timestamp'].diff().dt.total_seconds() / 60).round(0).fillna(15)
        cust_affected_x_duration = (group['customer_affected'] * group['duration_weight']).sum()
        cust_a_mean = cust_affected_x_duration / group['duration_weight'].sum()

        start_time = group['start_time'].min()
        end_time = start_time + duration_diff
        lat = group['lat'].iloc[-1]
        long = group['long'].iloc[-1]

        zipcode_map = self.geomap['zip_to_county_name']        
        zipcode = group['zipcode'].iloc[-1] if (pd.notna(group['zipcode'].iloc[-1]) and group['zipcode'].iloc[-1] != 'unknown') else '00000' # '00000' dummy zipcode to replace get_zipcode(long, lat) 
        county_name = self.geomap['zip_to_county_name'][zipcode] if (pd.notna(zipcode) and zipcode != '' and zipcode in zipcode_map) else pd.NA
        county_fips = self.geomap['zip_to_county_fips'][zipcode] if (pd.notna(zipcode) and zipcode != '' and zipcode in zipcode_map) else pd.NA
        state = self.geomap['zip_to_state_name'][zipcode] if (pd.notna(zipcode) and zipcode != '' and zipcode in zipcode_map) else pd.NA

        utility_provider = group['utility_provider'].iloc[-1]
        duration_max = duration_diff + timedelta(minutes=15)
        duration_mean = (duration_diff + duration_max) / 2
        group['duration_weight'] = (group['timestamp'].diff().dt.total_seconds() / 60).round(0).fillna(15)
        cust_affected_x_duration = (group['customer_affected'] * group['duration_weight']).sum()
        cust_a_mean = cust_affected_x_duration / group['duration_weight'].sum()
        total_customer_outage_time = cust_a_mean * duration_diff

        return pd.Series({
            # 'start_time': start_time,
            'end_time': end_time, # only if there is an endtime column like etrTime
            'first_timestamp': first_timestamp,
            'last_timestamp': last_timestamp,
            'lat': group['lat'].unique(),
            'long': group['long'].unique(),
            'zipcode': group['zipcode'].unique(),
            'county_name': county_name,
            'county_fips': county_fips,
            'state': state,
            'utility_provider': utility_provider,
            'duration_diff': duration_diff,
            'duration_max': duration_max,
            'duration_mean': duration_mean,
            'duration_15': duration_15,
            'customer_affected_mean': cust_a_mean,
            'cust_affected_x_duration': cust_affected_x_duration
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
    def transform(self, **kwargs):
        identifiers = kwargs.get('identifiers')
        method = kwargs.get('method')
        geo_level = kwargs.get('geo_level')
        time_interval = kwargs.get('time_interval')
        df = kwargs.get('dataframe', self._data.copy())

        df = df.rename(columns={
            'id':'outage_id',
            'startTime': 'start_time',
            'numPeople':'customer_affected',
            'zip_code':'zipcode',
            'EMC': 'utility_provider',
            'zip_code': 'zipcode',
            'latitude': 'lat',
            'longitude': 'long',
            'county': 'city'
        })
        
        eastern = tz.gettz('US/Eastern')
        utc = tz.gettz('UTC')

        def reformat_time(time):
            # format: 2024-01-18 09:04:15 but can also be in milliseconds since the Unix epoch (January 1, 1970, 00:00:00 UTC)
            eastern = tz.gettz('US/Eastern')
            utc = tz.gettz('UTC')
            if isinstance(time, str) and time.isdigit():
                # Convert millisecond timestamp to datetime
                return pd.to_datetime(int(time), unit='ms', utc=True).tz_convert(eastern)
                
            # else the time is string format of datetime
            elif isinstance(time, str) and ":" in time:
                return pd.to_datetime(time, utc=True).tz_convert(eastern)
            
            elif isinstance(time, datetime): # is datetime object already
                return time
            
            else: # is null or extraneous values that should be null (like '-1000')
                return pd.NaT

        try:
            """
            Transforming the dataframe
            - Some of the time columns has millisecond format, error codes, and NaN so we need to separately reformat before pd.to_datetime
            - etrTime has times before 2023-01-01, so we will set them to NaT
            - "county" values look like city names so renaming accordingly
            """
            # Masks for extracting rows with millisecond format, errors, or NA
            start_time_ms = df['start_time'].apply(lambda x: (isinstance(x, str) and (x.isdigit() or ":" not in x)) or pd.NA)
            lastUptTime_ms = df['lastUpdatedTime'].apply(lambda x: (isinstance(x, str) and (x.isdigit() or ":" not in x)) or pd.NA)
            etrTime_ms_null = df['etrTime'].apply(lambda x: (isinstance(x, str) and (x.isdigit() or ":" not in x)) or pd.NA) # or pd.NA
            timeSt_null = df['timestamp'].isna()
            extraneous_mask = start_time_ms | lastUptTime_ms | etrTime_ms_null | timeSt_null

            extraneous_rows = df[extraneous_mask]

            extraneous_rows['start_time'] = extraneous_rows['start_time'].apply(reformat_time)
            extraneous_rows['lastUpdatedTime'] = extraneous_rows['lastUpdatedTime'].apply(reformat_time)
            extraneous_rows['etrTime'] = extraneous_rows['etrTime'].apply(reformat_time)
            extraneous_rows['timestamp'] = extraneous_rows['timestamp'].apply(reformat_time)

            eastern = tz.gettz('US/Eastern')
            utc = tz.gettz('UTC')

            df.loc[extraneous_rows.index, 'start_time'] = extraneous_rows['start_time']
            df.loc[extraneous_rows.index, 'lastUpdatedTime'] = extraneous_rows['lastUpdatedTime']
            df.loc[extraneous_rows.index, 'etrTime'] = extraneous_rows['etrTime']
            df.loc[extraneous_rows.index, 'timestamp'] = extraneous_rows['timestamp']
        
            df['start_time'] = pd.to_datetime(df['start_time'], utc=True).dt.tz_convert(eastern)
            df['lastUpdatedTime'] = pd.to_datetime(df['lastUpdatedTime'], utc=True).dt.tz_convert(eastern)
            df['etrTime'] =pd.to_datetime(df['etrTime'], utc=True).dt.tz_convert(eastern)
            df['timestamp'] = pd.to_datetime(df['timestamp'], utc=True).dt.tz_convert(eastern)

            time_col = ['start_time', 'lastUpdatedTime', 'etrTime', 'timestamp']
            minimum_datetime = pd.to_datetime('2023-01-01 23:59:59-05:00', utc=True).tz_convert('US/Eastern')
            for col in time_col:
                df.loc[self._data[col] < minimum_datetime, col] = pd.NaT

        except Exception as e:
            print(f"An error occurred during transformation: {e}")

        return df

    def to_incident_level(self, identifers=['outage_id', 'start_time'], method='id_grouping'):
        """
        identifer: default identifier name "outage_id", or list like ['IncidentId', 'lat', 'lng', 'subgroup']
        method: "id_grouping" or "timegap_seperation" 
        """
        df = self.transform(identifiers=identifers, method=method)
        grouped = df.groupby(identifers).apply(self._agg_vars).reset_index().round(2)
        
        return grouped
    
    # gis = GIS("http://www.arcgis.com", "JK9035", "60129@GR0W3R5") # signing in to get access to arcGIS api; have to find 
    def _agg_vars(self, group):
        """
        Calculate duration by timestamp difference. 
        To do: Add more duration metrics later. Find a way to calculate zipcode without arcGIS api (get_zipcode method)
        """
        # def get_zipcode(long, lat):
        #     location = reverse_geocode((Geometry({"x":float(long), "y":float(lat), "spatialReference":{"wkid": 4326}})))
        #     return location['address']['Postal']

        first_timestamp = group['timestamp'].iloc[0]
        last_timestamp = group['timestamp'].iloc[-1]
        duration_diff = (last_timestamp - first_timestamp).total_seconds() / 60
        
        start_time = group['start_time'].min() # pd.NaT # group.index.get_level_values('start_time')
        end_time = group['timestamp'].max()        
        duration_diff_e1 = (end_time - start_time).total_seconds() / 60 if pd.notna(end_time) and pd.notna(start_time) else group['timestamp'].max() - group['timestamp'].min()

        lat = group['lat'].iloc[-1]
        long = group['long'].iloc[-1]

        zipcode_map = self.geomap['zip_to_county_name']        
        zipcode = group['zipcode'].iloc[-1] if group['zipcode'].iloc[-1] != 'unknown' else '00000' # '00000' dummy zipcode to replace get_zipcode(long, lat) 
        county_name = self.geomap['zip_to_county_name'][zipcode] if (pd.notna(zipcode) and zipcode != '' and zipcode in zipcode_map) else pd.NA
        county_fips = self.geomap['zip_to_county_fips'][zipcode] if (pd.notna(zipcode) and zipcode != '' and zipcode in zipcode_map) else pd.NA
        state = self.geomap['zip_to_state_name'][zipcode] if (pd.notna(zipcode) and zipcode != '' and zipcode in zipcode_map) else pd.NA

        utility_provider = group['utility_provider'].iloc[-1]
        duration_max = duration_diff + 15 # in float format where 1 = 1 minute
        duration_mean = (duration_diff + duration_max) / 2
        duration_15 = 15 * len(group)
        group['duration_weight'] = (group['timestamp'].diff().dt.total_seconds() / 60).round(0).fillna(15)
        cust_affected_x_duration = (group['customer_affected'] * group['duration_weight']).sum()
        cust_a_mean = cust_affected_x_duration / group['duration_weight'].sum()

        return pd.Series({
            'end_time': end_time, # only if there is an endtime column like etrTime
            'first_timestamp': first_timestamp,
            'last_timestamp': last_timestamp,
            'lat': group['lat'].unique(),
            'long': group['long'].unique(),
            'zipcode': group['zipcode'].unique(),
            'county_name': county_name,
            'county_fips': county_fips,
            'state': state,
            'utility_provider': utility_provider,
            'duration_diff': duration_diff,
            'duration_max': duration_max,
            'duration_mean': duration_mean,
            'duration_15': duration_15,
            'customer_affected_mean': cust_a_mean,
            'cust_affected_x_duration': cust_affected_x_duration
        })
    
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
    def transform(self, **kwargs):
        """
        Formatting issues with start_date and updateTime
        start_date format: 03/15 05:28 pm
        timestamp format: 01-18-2024 15:25:06 (For Walton, Tri-State, Oconee, and Mitchell, there are null timestamps in March 2023)
        updateTime format: Mar 15, 5 09, pm

        Remove outages with multiple start dates due to not there being many
        """

        identifiers = kwargs.get('identifiers')
        method = kwargs.get('method')
        geo_level = kwargs.get('geo_level')
        time_interval = kwargs.get('time_interval')
        df = kwargs.get('dataframe', self._data.copy())

        df = df.rename(columns={
            'incident_id':'outage_id',
            'start_date': 'start_time',
            'zip_code':'zipcode',
            'consumers_affected': 'customer_affected'
        })
        
        eastern = tz.gettz('US/Eastern')
        utc = tz.gettz('UTC')

        def _reformat_start_date(row):
            month_day, time, ampm = row['start_time'].split(' ')
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

        eastern = tz.gettz('US/Eastern')
        utc = tz.gettz('UTC')
        try:
            df['start_time'] = df.apply(_reformat_start_date, axis=1) 
            df['start_time'] = pd.to_datetime(df['start_time'], utc=True).dt.tz_convert(eastern)
            df['updateTime'] = df.apply(_reformat_update, axis=1)
            df['updateTime'] = pd.to_datetime(df['updateTime'], utc=True).dt.tz_convert(eastern)
            df['duration'] = pd.to_timedelta(df['duration'])
            df['timestamp'] = pd.to_datetime(df['timestamp'], utc=True).dt.tz_convert(eastern)


        except Exception as e:
            print(f"An error occurred during transformation: {e}")

        return df

    def to_incident_level(self, identifers=['outage_id', 'start_time'], method='id_grouping'):
        """
        identifer: default identifier name "outage_id", or list like ['IncidentId', 'lat', 'lng', 'subgroup']
        method: "id_grouping" or "timegap_seperation" 
        """
        df = self.transform(identifiers=identifers, method=method)
        grouped = df.groupby(identifers).apply(self._agg_vars).reset_index().round(2)
        
        return grouped
    
    def _agg_vars(self, group):
        """
        Overwriting superclass _compute_metrics as duration is given in this layout is more accurate
        """
        first_timestamp = group['timestamp'].min()
        last_timestamp = group['timestamp'].max()

        duration_dur_max = group['duration'].max()
        duration_ts_diff = last_timestamp - first_timestamp
        
        duration_diff = duration_dur_max
        duration_15 = 15 * len(group)
        group['duration_weight'] = (group['timestamp'].diff().dt.total_seconds() / 60).round(0).fillna(15)
        cust_affected_x_duration = (group['customer_affected'] * group['duration_weight']).sum()
        cust_a_mean = cust_affected_x_duration / group['duration_weight'].sum()

        duration_max = duration_diff + timedelta(minutes=15) # because 15 minute update intervals
        duration_mean = (duration_diff + duration_max) / 2
        start_time = group['start_time'].min()
        end_time = start_time + duration_diff

        zipcode = group['zipcode'].iloc[-1]
        zipcode_map = self.geomap['zip_to_county_name']        
        county_name = self.geomap['zip_to_county_name'][zipcode] if (pd.notna(zipcode) and zipcode != '' and zipcode in zipcode_map) else pd.NA
        county_fips = self.geomap['zip_to_county_fips'][zipcode] if (pd.notna(zipcode) and zipcode != '' and zipcode in zipcode_map) else pd.NA
        state = self.geomap['zip_to_state_name'][zipcode] if (pd.notna(zipcode) and zipcode != '' and zipcode in zipcode_map) else pd.NA

        utility_provider = group['EMC'].iloc[-1]

        customer_affected_mean = group['customer_affected'].mean()
        total_customer_outage_time = customer_affected_mean * duration_diff

        return pd.Series({
            # 'start_time': start_time,
            'end_time': end_time,
            'first_timestamp': first_timestamp,
            'last_timestamp': last_timestamp,
            'lat': group['lat'].unique(),
            'long': group['lon'].unique(),
            'zipcode': group['zipcode'].unique(),
            'county_name': county_name, 
            'county_fips': county_fips,
            'state': state,
            'utility_provider': utility_provider,
            'duration_diff': duration_dur_max,
            'duration_max': duration_max,
            'duration_mean': duration_mean,
            'duration_15': duration_15,
            'customer_affected_mean': cust_a_mean,
            'cust_affected_x_duration': cust_affected_x_duration,
            'total_customer_outage_time': total_customer_outage_time
        })
    
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