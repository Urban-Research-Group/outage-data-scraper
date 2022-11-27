import requests
import logging
from base import BaseScraper

import pandas as pd
from urllib.request import urlopen
import json

class ScraperL4(BaseScraper):
    logger = logging.getLogger("ScraperL4")

    def __init__(self, url):
        super().__init__()
        self.url = url

    def parse(self):
        response = self.fetch()
        data = (response['file_data']['areas'])
        response_df = pd.DataFrame(data)
        response_df = response_df[['name', 'cust_a', 'cust_s', 'n_out']]
        response_df['cust_a'] = response_df['cust_a'].apply(lambda x: x['val'])
        response_df.columns = ['name', 'customersAffected', 'customersServed', 'customersOutNow']
        return response_df

    def fetch(self):
        response = urlopen(self.url)
        data_json = json.loads(response.read())
        return data_json

if __name__ == '__main__':
    scrapper = ScraperL4('https://kubra.io/data/642d1d66-af22-44b8-b8e9-8118540b3a6f/public/reports/02e190b3-64f2-48b0-81ba-295186233695_report.json')
    print(scrapper.parse())

