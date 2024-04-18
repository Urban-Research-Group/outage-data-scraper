import logging
import json
import pandas as pd
import geopy
import xmltodict
import time
import requests

from bs4 import BeautifulSoup
from datetime import datetime
from urllib.request import urlopen, Request
from seleniumwire.utils import decode as sw_decode
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import Select
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from seleniumwire import webdriver

from .util import is_aws_env, make_request, timenow

# TODO: update for security
import ssl

ssl._create_default_https_context = ssl._create_unverified_context

from .ga_scraper import (
    BaseScraper,
    Scraper1 as GA_Scraper1,
    Scraper3 as GA_Scraper3
)
class MSScraper3(BaseScraper):
    def __init__(self, url, emc):
        super().__init__(url, emc)
        self.driver = self.init_webdriver()

    def parse(self):
        data = self.fetch()

        for level, pg in data.items():
            df = self._parse(pg)
            data.update({level: df})

        self.driver.close()
        self.driver.quit()

        return data



    def fetch(self):
        print(f"fetching {self.emc} outages from {self.url}")
        self.driver.get(self.url)
        time.sleep(10)

        dictionary_return = {
            "Number of Outages": [],
            "Affected Customers": [],
            "Still Out": [],

        }

        outage_elements = self.driver.find_elements(By.XPATH, "//div[contains(text(), 'Outages')]")
        affected_customers_elements = self.driver.find_elements(By.XPATH, "//div[contains(text(), 'Customers Affected')]")
        still_out_elements = self.driver.find_elements(By.XPATH, "//div[contains(text(), 'Still out')]")

        dictionary_return = {
            "Number of Outages": [element.text for element in outage_elements],
            "Affected Customers": [element.text for element in affected_customers_elements],
            "Still Out": [element.text for element in still_out_elements],
        }

        return dictionary_return



class MSScraper:
    def __new__(cls, layout_id, url, emc):
        if layout_id == 1:
            obj = super().__new__(GA_Scraper1)
        elif layout_id == 2:
            obj = super().__new__(GA_Scraper3)
        elif layout_id == 3:
            obj = super().__new__(MSScraper3)
        else: 
            raise "Invalid layout ID: Enter layout ID range from 1 to 3"
        obj.__init__(url, emc)
        return obj