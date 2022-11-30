import logging
from base import BaseScraper

from urllib.request import urlopen
import json
import pandas as pd
import geopy

class ScraperL2(BaseScraper):
    logger = logging.getLogger("ScraperL2")

    def __init__(self, url):
        super().__init__()
        self.url = url
        self.geo_locator = geopy.Nominatim(user_agent='1234')
    
    def extract_zipcode(self, lat, lon):
        addr = self.geo_locator.reverse((lat, lon)).raw['address']
        return addr.get('postcode', 'unknown')

    def parse(self):
        data = self.fetch()
        response_df =  pd.DataFrame(data)[['CustomersOutNow', 'CustomersRestored', 'CustomersServed', 'OutageLocation_lon',
                                'OutageLocation_lat', 'MapLocation', 'District', 'OutageName']]
        response_df.columns = ['customersAffected', 'customersRestored', 'customersServed', 'OutageLocation_lon',
                                'OutageLocation_lat', 'MapLocation', 'District', 'name']
        response_df['zip_code'] = response_df.apply(lambda row: self.extract_zipcode(row['OutageLocation_lat'], row['OutageLocation_lon']), axis = 1 )
        

        return response_df[['name', 'zip_code','customersAffected', 'customersRestored', 'customersServed' ]]


    def fetch(self):
        response = urlopen(self.url)
        data = response.read()
        response.close()

        data = json.loads(data.decode())['Outages']
        for outage in data:
            outage['OutageLocation_lon'] = outage['OutageLocation']['X']
            outage['OutageLocation_lat'] = outage['OutageLocation']['Y']

        return data

if __name__ == '__main__':
    scrapper = ScraperL2('http://outage.brmemc.com:8008/api/weboutageviewer/get_live_data?Start=&End=&Duration=0&CustomerResponsible=false&Historical=false&_=1667883437927')
    print(scrapper.fetch())


