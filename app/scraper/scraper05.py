import logging
from base import BaseScraper

import pandas as pd
from urllib.request import urlopen
import json
import geopy

class ScraperL5(BaseScraper):
    logger = logging.getLogger("ScraperL5")

    def __init__(self, url):
        super().__init__()
        self.url = url
        self.geo_locator = geopy.Nominatim(user_agent='1234')

    def extract_zipcode(self, lat, lon):
        addr = self.geo_locator.reverse((lat, lon)).raw['address']
        return addr.get('postcode', 'unknown')
            

    def parse(self):
        response = self.fetch()
        response_df = pd.DataFrame(response)[['county', 'numPeople', 'latitude', 'longitude', 'title']]
        response_df.columns = ['name', 'customersAffected', 'latitude', 'longitude', 'type']

        
        response_df['zip_code'] = response_df.apply(lambda row: self.extract_zipcode(row['latitude'], row['longitude']), axis = 1 )
        return response_df[['name', 'zip_code', 'customersAffected', 'latitude', 'longitude', 'type']]

    def fetch(self):
        response = urlopen(self.url)
        data_json = json.loads(response.read())
        return data_json

if __name__ == '__main__':
    scrapper = ScraperL5('https://utilisocial.io/datacapable/v2/p/colquitt/map/events?types=OUTAGE&polygons=&sortBy=CUSTOMERS_AFFECTED&orderBy=DESCENDING')
    print(scrapper.fetch())







