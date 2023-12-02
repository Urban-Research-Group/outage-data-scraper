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

from .ga_scraper import BaseScraper


# TODO: update for security
import ssl

ssl._create_default_https_context = ssl._create_unverified_context


class Scraper1(BaseScraper):
    def __init__(self, url, emc):
        super().__init__(url, emc)
        self.driver = self.init_webdriver()

    def parse(self):
        data = self.fetch()
        for key, val in data.items():
            if val:
                df = pd.DataFrame()

                for v in val:
                    for i in range(len(v["areas"])):
                        if (
                            "cust_a" in v["areas"][i]
                            and "val" in v["areas"][i]["cust_a"]
                        ):
                            v["areas"][i]["cust_a"] = v["areas"][i]["cust_a"]["val"]

                    df = pd.concat([df, pd.DataFrame(v["areas"])], ignore_index=True)

                df = df[(df["cust_a"] != 0)]
                df["timestamp"] = timenow()
                df["EMC"] = self.emc
                data.update({key: df})
            else:
                print(
                    f"no '{key}' outage of {self.emc} update found at",
                    datetime.strftime(datetime.now(), "%m-%d-%Y %H:%M:%S"),
                )

        self.driver.close()
        self.driver.quit()

        return data

    def fetch(self):
        print(f"fetching {self.emc} outages from {self.url}")
        # get javascript rendered source page
        self.driver.get(self.url)
        # Sleeps for 5 seconds
        time.sleep(5)

        try:
            page_source = self.driver.page_source
            soup = BeautifulSoup(page_source, "html.parser")
            iframe_tag = self.driver.find_element(By.ID, "stormcenter")
            source_page = iframe_tag.get_attribute("src")

            self.driver.get(source_page)
            time.sleep(5)

            county_button = self.driver.find_element(
                By.XPATH, "/html/body/div[1]/div[8]/div/div[9]/a[1]"
            )

            data_source = []

            county_button.click()
            time.sleep(5)
            for request in self.driver.requests:
                if "report_county" in request.url:
                    data_source.append(request.url)
                    break

            # we have to do this again since click county button will refresh the page
            self.driver.get(source_page)
            time.sleep(5)

            zip_button = self.driver.find_element(
                By.XPATH, "/html/body/div[1]/div[8]/div/div[9]/a[2]"
            )

            zip_button.click()
            time.sleep(5)
            for request in self.driver.requests:
                if "report_zip" in request.url:
                    data_source.append(request.url)
                    break

            raw_data = {}
            for url in data_source:
                if "report_county" in url:
                    print(url)
                    raw_data["per_county"] = requests.get(url).json()["file_data"][
                        "areas"
                    ][0]["areas"]
                    print(type(raw_data["per_county"]))
                    print(len(raw_data["per_county"]))
                    print(f"got county data")
                elif "report_zip" in url:
                    print(url)
                    raw_data["per_zipcode"] = requests.get(url).json()["file_data"][
                        "areas"
                    ][0]["areas"]
                    print(type(raw_data["per_zipcode"]))
                    print(len(raw_data["per_zipcode"]))
                    print(f"got zip code data")
        except Exception as e:
            print(f"Error: {e}")
            self.driver.close()
            self.driver.quit()

        return raw_data


class ILScraper:
    def __new__(cls, layout_id, url, emc):
        if layout_id == 1:
            obj = super().__new__(Scraper1)
        else:
            raise "Invalid layout ID: Enter layout ID range from 1 to 1"
        obj.__init__(url, emc)
        return obj
