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

# from webdriver_manager.chrome import ChromeDriverManager  # for local test
from .util import is_aws_env, make_request, timenow

# TODO: update for security
import ssl

ssl._create_default_https_context = ssl._create_unverified_context

from .ga_scraper import (
    BaseScraper,
    Scraper1 as GA_Scraper1,
    Scraper9 as GA_Scraper9, #layout3
    Scraper3 as GA_Scraper3,
    Scraper11 as GA_Scraper11, #outageentry
    Scraper6 as GA_Scraper6
)

class BaseScraper:
    def __init__(self, url, emc):
        self.url = url
        self.emc = emc
        # self.driver = self.init_webdriver()
        self.geo_locator = geopy.Nominatim(user_agent="1234")

    def fetch(self, url=None, header=None, data=None, method="GET", key="per_outage"):
        """Fetches data from url and returns a dict of dataframes"""
        print(f"fetching {self.emc} outages from {self.url}")
        # TODO: should only return raw data
        raw_data = {}

        url = url if url else self.url
        body, response = make_request(url, header, data, method)

        if isinstance(body, bytes):
            raw_data[key] = json.loads(body.decode("utf8"))
        else:
            raw_data[key] = json.loads(body)

        return raw_data

    def parse(self):
        pass

    def get_page_source(self, url=None, timeout=5):
        url = url if url else self.url
        self.driver.get(url)
        # let the page load
        time.sleep(timeout)
        page_source = self.driver.page_source

        return page_source

    def extract_zipcode(self, lat, lon):
        try:
            addr = self.geo_locator.reverse((lat, lon), timeout=10)
            if addr:
                return addr.raw["address"].get("postcode", "unknown")
            else:
                return "unknown"
        except Exception as e:
            print(e)
            return "unknown"

    def init_webdriver(self):
        chrome_driver_path = (
            "/opt/chromedriver"
            if is_aws_env()
            else "/Users/gtingliu/Desktop/Gatech/URG/outage-data-scraper/app/scrapers/chromedriver"
        )

        desired_capabilities = DesiredCapabilities.CHROME.copy()
        desired_capabilities["goog:loggingPrefs"] = {"performance": "ALL"}
        desired_capabilities["acceptInsecureCerts"] = True

        # Create the webdriver object and pass the arguments
        chrome_options = webdriver.ChromeOptions()
        # chrome_options.add_argument("--headless")
        chrome_options.add_argument("--headless=new")
        chrome_options.add_argument("--allow-insecure-localhost")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--disable-extensions")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--no-cache")
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--window-size=1024x768")
        chrome_options.add_argument("--user-data-dir=/tmp/user-data")
        chrome_options.add_argument("--hide-scrollbars")
        chrome_options.add_argument("--enable-logging")
        chrome_options.add_argument("--log-level=0")
        chrome_options.add_argument("--v=99")
        chrome_options.add_argument("--single-process")
        chrome_options.add_argument("--data-path=/tmp/data-path")
        chrome_options.add_argument("--ignore-certificate-errors")
        chrome_options.add_argument("--homedir=/tmp")
        chrome_options.add_argument("--disk-cache-dir=/tmp/cache-dir")
        chrome_options.add_argument(
            "user-agent=Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/61.0.3163.100 "
            "Safari/537.36"
        )
        chrome_options.headless = True
        selenium_options = {
            "request_storage_base_dir": "/tmp",  # Use /tmp to store captured data
            "exclude_hosts": "",
        }
        if is_aws_env():
            chrome_options.binary_location = "/opt/chrome/chrome"

        driver = webdriver.Chrome(
            # ChromeDriverManager().install(),  # for local test
            executable_path=chrome_driver_path,
            chrome_options=chrome_options,
            seleniumwire_options=selenium_options,
            desired_capabilities=desired_capabilities,
        )
        return driver



class SCScraper6(BaseScraper):
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



        time.sleep(5)
        page_source = {}
        select_elements = self.driver.find_elements(By.CLASS_NAME, "gwt-ListBox")
        menu = Select(select_elements[0])
        for idx, option in enumerate(menu.options):
            level = option.text
            menu.select_by_index(idx)
            time.sleep(3)
            page_source.update({f"per_{level}": self.driver.page_source})
        return page_source
    



class SCScraper: 
    def __new__(cls, layout_id, url, emc):
        if layout_id == 1:
            obj = super().__new__(GA_Scraper11) #outgeentry, layout 1 
        elif layout_id == 2:
            obj = super().__new__(GA_Scraper1) #GA1
        elif layout_id == 3:
            obj = super().__new__(GA_Scraper9) 
        elif layout_id == 4: 
            obj = super().__new__(GA_Scraper3)
        elif layout_id == 5: 
            obj = super().__new__(GA_Scraper6)
        else: 
            raise "Invalid layout ID: Enter layout ID range from 1 to 5"
        obj.__init__(url, emc)
        return obj

