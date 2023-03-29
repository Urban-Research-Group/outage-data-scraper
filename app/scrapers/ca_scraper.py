import pandas as pd
import requests
import time

from .ga_scraper import BaseScraper, Scraper9
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from .util import timenow

URL = "https://services.arcgis.com/BLN4oKB0N1YSgvY8/arcgis/rest/services/Power_Outages_(View)/FeatureServer/0/query"


class ScraperINV(BaseScraper):
    def __init__(self, url, emc):
        super().__init__(url, emc)

    def parse(self):
        data = self.fetch()
        # parsing
        for key, val in data.items():
            df = pd.DataFrame(val['features'])
            df = pd.concat([df.drop(["attributes"], axis=1), df["attributes"].apply(lambda x: pd.Series(x))], axis=1)
            df = pd.concat([df.drop(["geometry"], axis=1), df["geometry"].apply(lambda x: pd.Series(x))], axis=1)

            df['timestamp'] = timenow()
            # disabled b/c takes too long
            # df['zip'] = df.apply(lambda x: self.extract_zipcode(x.y, x.x), axis=1)
            df[['StartDate', 'EstimatedRestoreDate']] = df[['StartDate', 'EstimatedRestoreDate']].apply(pd.to_datetime,
                                                                                                        unit='ms')
            data.update({key: df})

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


class ScraperCPA(Scraper9):
    def __init__(self, url, emc):
        super().__init__(url, emc)

    def fetch(self):
        print(f"fetching {self.emc} outages from {self.url}")
        # Send a request to the website and let it load
        self.driver.get(self.url)
        # time.sleep(10)

        wait = WebDriverWait(self.driver, 10)
        label = wait.until(EC.element_to_be_clickable((By.XPATH, '//*[@id="OMS.Customers Summary"]')))
        label.click()

        # Wait for the page to fully load
        time.sleep(10)


class ScraperCC(BaseScraper):
    def __init__(self, url, emc):
        super().__init__(url, emc)

    def parse(self):
        pass

    def fetch(self):
        # need to observe outage first?
        pass


class CAScraper:
    def __new__(cls, layout_id, url, emc):
        if layout_id == 'investor':
            obj = super().__new__(ScraperINV)
        elif layout_id == 'paloalto':
            obj = super().__new__(ScraperCPA)
        elif layout_id == 'colton':
            obj = super().__new__(ScraperCC)
        else:
            raise "Invalid layout ID: Enter layout ID in ['investor', 'cpa', 'cc']"

        obj.__init__(url, emc)
        return obj
