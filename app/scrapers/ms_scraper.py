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
    Scraper11 as GA_Scraper11
)

class Scraper7(BaseScraper):
    def __init__(self, url, emc):
        super().__init__(url, emc)
        self.driver = self.init_webdriver()

    def parse(self):
        data = self.fetch()
        out = {}
        out["per_county"] = pd.DataFrame(data["per_county"])
        out["per_outage"] = pd.DataFrame(data["per_outage"])

        return out

    def fetch(self):
        print(f"fetching {self.emc} outages from {self.url}")
        raw_data = {}

        with urlopen(self.url) as response:
            data = json.loads(response.read())
            raw_data["per_county"] = data["reportData"]["reports"][0]["polygons"]
            raw_data["per_outage"] = data["outageData"]["outages"]
            #print(raw_data)            

        return raw_data

class Scraper10(BaseScraper):
    def __init__(self, url, emc):
        super().__init__(url, emc)
        self.driver = self.init_webdriver()

    def parse(self):
        data = self.fetch()
        out = {}
        out["per_emc"] = pd.DataFrame([data])

        return out

    def fetch(self):
        print(f"fetching {self.emc} outages from {self.url}")
        raw_data = {}
        self.driver.get(self.url)
        time.sleep(3)
        raw_data["currentOutages"] = self.driver.find_element(By.ID, "currentOutages").text
        raw_data["lastUpdated"] = self.driver.find_element(By.ID, "Last-Refresh-Time").text
        print(raw_data)
        return raw_data

class MSScraper:
    def __new__(cls, layout_id, url, emc):
        if layout_id == 2:
            obj = super().__new__(GA_Scraper11)
        elif layout_id == 7:
            obj = super().__new__(Scraper7)
        elif layout_id == 10:
            obj = super().__new__(Scraper10)
        else:
            raise "Invalid layout ID: Enter layout ID range from 1 to 2"
        obj.__init__(url, emc)
        return obj

