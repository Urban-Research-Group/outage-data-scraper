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
    BaseScraper
)

class Scraper3(BaseScraper):
    def __init__(self, url, emc):
        super().__init__(url, emc)
        self.driver = self.init_webdriver()

    def parse(self):
        data = self.fetch()
        for d in data["per_outage"]["data"]:
            del d["extension"]
            del d["affectedAreas"]

        out = {}
        out["per_outage"] = pd.DataFrame(data["per_outage"]["data"])

        return out

    def fetch(self):
        print(f"fetching {self.emc} outages from {self.url}")
        raw_data = {}

        self.driver.get(self.url)
        for request in self.driver.requests:
            if "alloutages" in request.url:
                print(request.url)
                response = sw_decode(
                    request.response.body,
                    request.response.headers.get("Content-Encoding", "identity"),
                )
                raw_data["per_outage"] = json.loads(response.decode("utf-8"))

        return raw_data
    
class Scraper6(BaseScraper):
    def __init__(self, url, emc):
        super().__init__(url, emc)
        self.driver = self.init_webdriver()

    def parse(self):
        data = self.fetch()

        out = {}
        out["per_outage"] = pd.DataFrame(data["per_outage"])

        return out

    def fetch(self):
        print(f"fetching {self.emc} outages from {self.url}")
        raw_data = {}

        with urlopen(self.url) as response:
            raw_data["per_outage"] = json.loads(response.read())

        return raw_data

class LAScraper:
    def __new__(cls, layout_id, url, emc):
        if layout_id == 3:
            obj = super().__new__(Scraper3)
        elif layout_id == 6:
            obj = super().__new__(Scraper6)
        else:
            raise "Invalid layout ID: Enter layout ID range from 1 to 11"

        obj.__init__(url, emc)
        return obj
