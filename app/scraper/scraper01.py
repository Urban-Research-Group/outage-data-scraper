import requests
import logging
from base import BaseScraper

import pandas as pd
from urllib.request import urlopen
import json

class ScraperL1(BaseScraper):
    logger = logging.getLogger("ScraperL1")

    def __init__(self, url):
        super().__init__()
        self.url = url

    def parse(self):
        body, status = self.fetch(self.url)
        data = json.loads(body)
        df = pd.DataFrame(data[0]['boundaries'])
        return df

    # def fetch(self):
    #     response = urlopen(self.url)
    #     # print(response.read())
    #     data_json = json.loads(response.read())
    #     return data_json


if __name__ == '__main__':
    scrapper = ScraperL1('http://74.121.99.238:8081/data/boundaries.json')
    print(scrapper.parse())

    # scrapper = ScraperL1('http://outage.gradyemc.com:7576/data/boundaries.json')
    # print(scrapper.parse())


