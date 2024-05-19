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

from .ga_scraper import (
    Scraper3 as GA_Scraper3,
    Scraper4 as GA_Scraper4,
    Scraper9 as GA_Scraper9,
    BaseScraper,
    Scraper11 as GA_Scraper11,
)

from .tx_scraper import (
    Scraper4 as TX_Scraper4,
)

# TODO: update for security
import ssl

ssl._create_default_https_context = ssl._create_unverified_context


class Scraper7(BaseScraper):
    def __init__(self, url, emc):
        super().__init__(url, emc)
        self.driver = self.init_webdriver()

    def parse(self):
        data = self.fetch()
        out = {}
        out["per_county"] = pd.DataFrame(data["per_county"])
        out["per_district"] = pd.DataFrame(data["per_district"])

        out["per_county"] = out["per_county"][out["per_county"]["affected"] != 0]
        out["per_district"] = out["per_district"][out["per_district"]["affected"] != 0]

        # Add timestamp
        out["per_county"]["timestamp"] = timenow()
        out["per_district"]["timestamp"] = timenow()

        # Add EMC
        out["per_county"]["emc"] = self.emc
        out["per_district"]["emc"] = self.emc

        return out

    def fetch(self):
        print(f"fetching {self.emc} outages from {self.url}")
        raw_data = {}

        with urlopen(self.url) as response:
            data = json.loads(response.read())
            raw_data["per_county"] = data["reportData"]["reports"][0]["polygons"]
            raw_data["per_district"] = data["reportData"]["reports"][1]["polygons"]

        return raw_data


class Scraper10(BaseScraper):
    def __init__(self, url, emc):
        super().__init__(url, emc)
        self.driver = self.init_webdriver()

    def parse(self):
        data = self.fetch()
        out = {}
        out["per_emc"] = pd.DataFrame([data])
        out["per_emc"]["timestamp"] = timenow()
        out["per_emc"]["emc"] = self.emc

        return out

    def fetch(self):
        print(f"fetching {self.emc} outages from {self.url}")
        raw_data = {}
        self.driver.get(self.url)
        time.sleep(3)
        raw_data["currentOutages"] = self.driver.find_element(
            By.ID, "currentOutages"
        ).text
        raw_data["lastUpdated"] = self.driver.find_element(
            By.ID, "Last-Refresh-Time"
        ).text
        print(raw_data)
        return raw_data


class MSScraper:
    def __new__(cls, layout_id, url, emc):
        if layout_id == 2:
            obj = super().__new__(GA_Scraper11)
        elif layout_id == 3:
            obj = super().__new__(GA_Scraper3)
        elif layout_id == 4:
            obj = super().__new__(GA_Scraper4)
        elif layout_id == 5:
            obj = super().__new__(GA_Scraper9)
        elif layout_id == 7:
            obj = super().__new__(Scraper7)
        elif layout_id == 8:
            obj = super().__new__(TX_Scraper4)
        elif layout_id == 10:
            obj = super().__new__(Scraper10)
        else:
            raise "Invalid layout ID: Enter layout ID range from 1 to 2"
        obj.__init__(url, emc)
        return obj
