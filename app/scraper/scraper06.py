import logging
from base import BaseScraper

from seleniumwire import webdriver
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities
from seleniumwire.utils import decode as sw_decode
import time

import xmltodict
import pandas as pd
import geopy



class ScraperL6(BaseScraper):
    logger = logging.getLogger("ScraperL3")

    def __init__(self, url):
        super().__init__()
        self.url = url
        self.geo_locator = geopy.Nominatim(user_agent='1234')        
        self.init_webdriver()


    def init_webdriver(self):
        
        chrome_driver_path = 'chromedriver.exe'

        desired_capabilities = DesiredCapabilities.CHROME
        desired_capabilities["goog:loggingPrefs"] = {"performance": "ALL"}

        # Create the webdriver object and pass the arguments
        options = webdriver.ChromeOptions()

        # Chrome will start in Headless mode
        options.add_argument('headless')

        # Ignores any certificate errors if there is any
        options.add_argument("--ignore-certificate-errors")

        # Startup the chrome webdriver with executable path and
        # pass the chrome options and desired capabilities as
        # parameters.
        self.driver = webdriver.Chrome(executable_path=chrome_driver_path,
                                chrome_options=options,
                                desired_capabilities=desired_capabilities)

        # Send a request to the website and let it load
        self.driver.get(self.url)

        # Sleeps for 5 seconds
        time.sleep(5)

    def extract_zipcode(self, lat, lon):
        addr = self.geo_locator.reverse((lat, lon)).raw['address']
        return addr.get('postcode', 'unknown')


    def parse(self):
        data = self.fetch()

        rows = []
        for row in data['data']['reports']['report']['dataset']['t']:
            position = row['e'][5].split(',')
            rows.append([row['e'][0], row['e'][1], row['e'][2], float(position[0]), float(position[1])])

        df = pd.DataFrame( rows, columns = ['name', 'customersServed', 'customersAffected', 'latitutde', 'longitude'])
        df['zip_code'] = df.apply(lambda row: self.extract_zipcode(row['latitutde'], row['longitude']), axis = 1 )
        return df[['name', 'zip_code','customersAffected', 'customersServed' ]]

    def fetch(self):
        datas = []
        for request in self.driver.requests:
            if "data?_" in request.url:
                data = sw_decode(request.response.body, request.response.headers.get('Content-Encoding', 'identity'))
                data = data.decode("utf8")
                datas.append(data)

        data_response = ""
        for data in datas:
            if "County" in data:
                data_response = data
                break

        data_response = xmltodict.parse(data_response)
        return data_response

if __name__ == '__main__':
    scrapper = ScraperL6('https://outage.utility.org/')
    print(scrapper.parse())


