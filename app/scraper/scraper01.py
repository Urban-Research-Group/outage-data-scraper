import requests
import logging
from app.scraper.base import BaseScraper

import pandas as pd
from urllib.request import urlopen
import json

class ScraperL1(BaseScraper):
    logger = logging.getLogger("ScraperL1")

    def __init__(self, urls):
        super().__init__()
        self.urls = urls

    def parse(self):
        """
        parse all urls to dataframe
        :return: dataframe
        """
        for url in self.urls:
            body, status = self.fetch(url)
            data = json.loads(body)

            # TODO: aggregate how?
            if 'boundaries' in url:
                print('boundaries', data)
            elif 'outageSummary' in url:
                print('summary', data)
            elif 'outage' in url:
                print('outage', data)

        #     df = pd.DataFrame(data[0]['boundaries'])
        # return df


