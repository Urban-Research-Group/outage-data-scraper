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
    Scraper11 as GA_Scraper11,
    Scraper1 as GA_Scraper1,
    Scraper9 as GA_Scraper9,
)


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
        # Sleeps for 5 seconds
        time.sleep(10)

        page_source = {}
        try:
            iframe_tag = self.driver.find_element(
                By.XPATH,
                "/html/body/div[1]/main/div/article/div[3]/div[1]/div/div/div/iframe",
            )
            self.driver.switch_to.frame(iframe_tag)
            time.sleep(50)  # since this iframe is super slow
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

        except Exception as e:
            print(f"Error: {e}")
            self.driver.close()
            self.driver.quit()

        return page_source


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
        else:
            raise "Invalid layout ID: Enter layout ID range from 1 to 3"
        obj.__init__(url, emc)
        return obj
