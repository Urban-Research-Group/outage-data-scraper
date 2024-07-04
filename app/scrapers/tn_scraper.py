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
    BaseScraper,
    Scraper1 as GA_Scraper1,
    Scraper5 as GA_Scraper5,
    Scraper9 as GA_Scraper9,
    Scraper11 as GA_Scraper11,
)
from .fl_scraper import Scraper13 as FL_Scraper13


# TODO: update for security
import ssl

ssl._create_default_https_context = ssl._create_unverified_context


class Scraper2(BaseScraper):
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

    def _parse(self, page_source):
        soup = BeautifulSoup(page_source, "html.parser")

        tables = soup.find_all(id="reportTable")

        # separate rows
        rows = tables[0].find_all("tr")
        header_row = rows[0]
        data_rows = rows[1:]

        # Extract the table header cells
        header_cells = header_row.find_all("th")
        header = [cell.get_text().strip() for cell in header_cells]
        cols = [h for h in header if h != ""]

        # Extract the table data cells
        data = []
        for row in data_rows:
            cells = row.find_all("td")
            data.append([cell.get_text().strip() for cell in cells])

        # Print the table data as a list of dictionaries
        table = [dict(zip(header, row)) for row in data]
        df = pd.DataFrame(table)
        if len(df.columns) > 1:
            df = df[cols]
            df = df.dropna(axis=0)
            df["timestamp"] = timenow()
            df["EMC"] = self.emc
            df = df[df["Aff"] != "0"]
        else:
            df = pd.DataFrame()

        return df

    def fetch(self):
        print(f"Fetching {self.emc} outages from {self.url}")
        # get javascript rendered source page
        self.driver.get(self.url)

        time.sleep(10)

        page_source = {}
        selector = self.driver.find_element(
            By.XPATH,
            "/html/body/div[2]/div/div[3]/div[2]/div/div/div[3]/font/select",
        )

        menu = Select(selector)
        for idx, option in enumerate(menu.options):
            level = option.text
            menu.select_by_index(idx)
            time.sleep(3)
            page_source.update({f"per_{level}": self.driver.page_source})

        return page_source


class Scraper5(BaseScraper):
    def __init__(self, url, emc):
        super().__init__(url, emc)
        self.driver = self.init_webdriver()

    def parse(self):
        data = self.fetch()
        for key, val in data.items():
            if val:
                if key == "per_district":
                    new_val = []
                    for item in val:
                        if "outage_reported" not in item:
                            continue
                        new_item = {
                            "district": item["district"],
                            "custom_qty": item.get("outage_reported", {}).get(
                                "customer_qty", 0
                            ),
                            "incident_qty": item.get("outage_reported", {}).get(
                                "incident_qty", 0
                            ),
                        }
                        new_val.append(new_item)
                    df = pd.DataFrame(new_val)
                    df["timestamp"] = timenow()
                    df["EMC"] = self.emc
                    data.update({key: df})
                elif key == "per_outage":
                    df = pd.DataFrame(val)
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
        print(f"Fetching {self.emc} outages from {self.url}")
        # get javascript rendered source page
        self.driver.get(self.url)
        # Sleeps for 5 seconds
        time.sleep(5)

        raw_data = {}
        for request in self.driver.requests:
            if "incidents" in request.url:
                data = requests.get(request.url).json()
                raw_data["per_district"] = data["district_metrics"]
                raw_data["per_outage"] = data["outage_points"]
                break

        return raw_data


class Scraper7(BaseScraper):
    def __init__(self, url, emc):
        super().__init__(url, emc)
        self.driver = self.init_webdriver()

    def parse(self):
        data = self.fetch()
        result = {}
        for key, val in data.items():
            if val:
                df = pd.DataFrame(val)
                df["timestamp"] = timenow()
                df["EMC"] = self.emc
                result["per_outage"] = df

                df = pd.DataFrame(val).drop(["id", "zipcode"], axis=1, errors="ignore")
                df = df.groupby("county", as_index=False)["customerCount"].sum()
                df["timestamp"] = timenow()
                df["EMC"] = self.emc
                result["per_county"] = df

                df = pd.DataFrame(val).drop(["id", "county"], axis=1, errors="ignore")
                df = df.groupby("zipcode", as_index=False)["customerCount"].sum()
                df["timestamp"] = timenow()
                df["EMC"] = self.emc
                result["per_zipcode"] = df

            else:
                print(
                    f"no '{key}' outage of {self.emc} update found at",
                    datetime.strftime(datetime.now(), "%m-%d-%Y %H:%M:%S"),
                )

        self.driver.close()
        self.driver.quit()

        return result

    def fetch(self):
        print(f"Fetching {self.emc} outages from {self.url}")
        # get javascript rendered source page
        self.driver.get(self.url)
        # Sleeps for 5 seconds
        time.sleep(5)

        raw_data = {}
        for request in self.driver.requests:
            if "electric-outage-details" in request.url:
                data = requests.get(request.url).json()
                raw_data["per_outage"] = data["electricOutageDetails"]
                break

        return raw_data


class TNScraper:
    def __new__(cls, layout_id, url, emc):
        if layout_id == 1:
            obj = super().__new__(GA_Scraper11)
        elif layout_id == 2:
            obj = super().__new__(Scraper2)
        elif layout_id == 3:
            obj = super().__new__(GA_Scraper1)
        elif layout_id == 4:
            obj = super().__new__(GA_Scraper9)
        elif layout_id == 5:
            obj = super().__new__(Scraper5)
        elif layout_id == 6:
            obj = super().__new__(GA_Scraper5)
        elif layout_id == 7:
            obj = super().__new__(Scraper7)
        elif layout_id == 8:
            obj = super().__new__(FL_Scraper13)

        else:
            raise "Invalid layout ID: Enter layout ID range from 1 to 3"
        obj.__init__(url, emc)
        return obj
