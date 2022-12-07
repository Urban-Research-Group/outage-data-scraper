import logging
from base import BaseScraper

from seleniumwire import webdriver
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities
from seleniumwire.utils import decode as sw_decode
import time

import json
import pandas as pd
import geopy


class ScraperL11(BaseScraper):
    logger = logging.getLogger("ScraperL7")

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
        data = (self.fetch())
        # print(data)
        df =  pd.DataFrame(data['rows']['subs'])
        return df

    def fetch(self):
        datas = []
        index = 0
        i = 0
        for request in self.driver.requests:
            if "ShellOut" in request.url:
                data = sw_decode(request.response.body, request.response.headers.get('Content-Encoding', 'identity'))
                data = data.decode("utf8")
                datas.append(data)

                if 'SubName' in data:
                    index = i
                
                i += 1
        
        print(index)
        data_response = datas[index]
        return json.loads(data_response)

if __name__ == '__main__':
    scrapper = ScraperL11('https://www.outageentry.com/Outages/outage.php?Client=tsemc&serviceIndex=1&openingPage=')
    print(scrapper.parse())

    scrapper = ScraperL11('https://www.outageentry.com/Outages/outage.php?Client=walton')
    print(scrapper.parse())

