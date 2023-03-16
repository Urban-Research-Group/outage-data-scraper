import pandas as pd
import requests

from scrapers.ga_scraper import BaseScraper as BaseScraper
from scrapers.util import timenow

URL = "https://services.arcgis.com/BLN4oKB0N1YSgvY8/arcgis/rest/services/Power_Outages_(View)/FeatureServer/0/query"


class CAScraper(BaseScraper):
    def __init__(self, layout_id, url, emc):
        super().__init__(url, emc)

    def parse(self):
        data = self.fetch()
        # parsing
        for key, val in data.items():
            df = pd.DataFrame(val['features'])
            df = pd.concat([df.drop(["attributes"], axis=1), df["attributes"].apply(lambda x: pd.Series(x))], axis=1)
            df = pd.concat([df.drop(["geometry"], axis=1), df["geometry"].apply(lambda x: pd.Series(x))], axis=1)

            # df['timestamp'] = timenow()
            # disabled b/c takes too long
            # df['zip'] = df.apply(lambda x: self.extract_zipcode(x.y, x.x), axis=1)
            df[['StartDate', 'EstimatedRestoreDate']] = df[['StartDate', 'EstimatedRestoreDate']].apply(pd.to_datetime,
                                                                                                        unit='ms')
            data.update({key: df})
            print(data)

        return data

    def fetch(self):
        raw_data = {}
        # Query parameters
        params = {
            "where": "1=1",
            "outFields": "*",
            "outSR": "4326",
            "f": "json"
        }

        # Send a GET request to the API endpoint with the query parameters
        response = requests.get(self.url, params=params)

        # Check if the request was successful
        if response.status_code == 200:
            # Extract the JSON response content
            raw_data.update({'per_outage': response.json()})

            return raw_data
        else:
            print("Request failed. Status code:", response.status_code)