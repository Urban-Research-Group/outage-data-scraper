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
)


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
        print(f"Fetching {self.emc} outages from {self.url}")
        # get javascript rendered source page
        self.driver.get(self.url)
        # Sleeps for 5 seconds
        time.sleep(5)

        page_source = self.driver.page_source
        soup = BeautifulSoup(page_source, "html.parser")

        menu_button = self.driver.find_element(By.XPATH, "/html/body/header/button[1]")
        menu_button.click()
        time.sleep(2)

        sum_button = self.driver.find_element(By.XPATH, "/html/body/div[8]/div/a[2]")
        sum_button.click()
        time.sleep(2)

        # print(soup.prettify())

        city_button = self.driver.find_element(
            By.XPATH, "/html/body/div[8]/div/div[8]/a[1]"
        )

        data_source = []

        city_button.click()
        time.sleep(5)
        for request in self.driver.requests:
            if "report_nyc" in request.url or "report_ny" in request.url:
                data_source.append(request.url)
                print(request.url)
                break

        # we have to do this again since click county button will refresh the page
        self.driver.get(self.url)
        time.sleep(5)
        menu_button = self.driver.find_element(By.XPATH, "/html/body/header/button[1]")
        menu_button.click()
        time.sleep(2)

        sum_button = self.driver.find_element(By.XPATH, "/html/body/div[8]/div/a[2]")
        sum_button.click()
        time.sleep(2)

        west_button = self.driver.find_element(
            By.XPATH, "/html/body/div[8]/div/div[8]/a[2]"
        )

        west_button.click()
        time.sleep(5)
        for request in self.driver.requests:
            if "report_westchester" in request.url or "report_nj" in request.url:
                data_source.append(request.url)
                print(request.url)
                break

        raw_data = {}
        for url in data_source:
            if "report_nyc" in url:
                print(url)
                raw_data["per_borough_new_york"] = requests.get(
                    url, verify=False
                ).json()["file_data"]["areas"][0]["areas"]
                print(f"got borough data")
            elif "report_westchester" in url:
                print(url)
                raw_data["per_area_westchester"] = requests.get(
                    url, verify=False
                ).json()["file_data"]["areas"][0]["areas"]
                print(f"got area data")
            elif "report_ny" in url:
                print(url)
                raw_data["per_county_new_york"] = requests.get(
                    url, verify=False
                ).json()["file_data"]["areas"][0]["areas"]
                print(f"got new york's county data")
            elif "report_nj" in url:
                print(url)
                raw_data["per_county_new_jersey"] = requests.get(
                    url, verify=False
                ).json()["file_data"]["areas"][0]["areas"]
                print(f"got new jersey's county data")

        return raw_data


class Scraper2(BaseScraper):
    def __init__(self, url, emc):
        super().__init__(url, emc)
        self.driver = self.init_webdriver()

    def parse(self):
        data = self.fetch()

        for key, val in data.items():
            if val:
                df = pd.DataFrame(val["areas"])
                df[["cust_a", "percent_cust_a"]] = df[
                    ["cust_a", "percent_cust_a"]
                ].applymap(lambda x: x["val"])
                df = df[(df["cust_a"] != 0) | (df["n_out"] != 0)]
                df["timestamp"] = timenow()
                df["EMC"] = self.emc
                df.drop(columns=["gotoMap"], inplace=True)
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
        page_source = self.driver.page_source
        raw_data = {}
        # parse reports link
        soup = BeautifulSoup(page_source, "html.parser")

        containers = soup.find_all(class_="row report-link hyperlink-primary")
        links = {}
        for c in containers:
            links.update({c.getText(): c.get("href")})

        # get json reports
        visited = []  # somehow it we request multiple times
        raw_data = {}
        for k, v in links.items():
            self.driver.get(self.url + v[1:])
            print(f"Fetching data from {self.url+v[1:]}")
            time.sleep(5)
            requests = self.driver.requests
            for r in requests:
                if "report.json" in r.url and r.url not in visited:
                    visited.append(r.url)
                    print(f"Fetching report from {r.url}")
                    time.sleep(5)
                    response = sw_decode(
                        r.response.body,
                        r.response.headers.get("Content-Encoding", "identity"),
                    )
                    data = json.loads(response.decode("utf8", "ignore"))

                    if "Town" in k:
                        raw_data["per_town"] = data["file_data"]
                        print(f"got town data")
                    elif "County" in k:
                        raw_data["per_county"] = data["file_data"]
                        print(f"got county data")
        return raw_data


class Scraper3(BaseScraper):
    def __init__(self, url, emc):
        super().__init__(url, emc)
        self.driver = self.init_webdriver()

    def parse(self):
        data = self.fetch()

        for key, val in data.items():
            if val:
                df = pd.DataFrame(val)

                # df = df[(df["cust_a"] != 0)]
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

    def _fetch(self, url):
        self.driver.get(url)
        # Sleeps for 5 seconds
        time.sleep(5)

        table_element = self.driver.find_element(By.XPATH, "/html/body/table[2]")
        table_html = table_element.get_attribute("outerHTML")
        soup = BeautifulSoup(table_html, "html.parser")
        table_rows = soup.find_all("tr")

        columns = [
            column.get_text(strip=True)
            for column in table_rows[0].find_all(["th", "td"])
        ]

        # Storing values in a list of dictionaries
        data = []
        for row in table_rows[1:-1]:  # Ignore the last tr tag
            values = [value.get_text(strip=True) for value in row.find_all("td")]
            row_data = {columns[i]: values[i] for i in range(len(columns))}
            data.append(row_data)
        return data

    def fetch(self):
        print(f"Fetching {self.emc} outages from {self.url}")
        # get javascript rendered source page
        self.driver.get(self.url)
        # Sleeps for 5 seconds
        time.sleep(5)

        raw_data = {}
        page_source = self.driver.page_source
        soup = BeautifulSoup(page_source, "html.parser")
        xpath = ""
        if self.emc == "rg&e":
            xpath = "/html/body/div[1]/div[1]/main/div[1]/div/div/div/div/section/div/div[2]/div/div/div/div/div[2]/div/div/p[4]/iframe"
        elif self.emc == "nyseg":
            xpath = "/html/body/div[1]/div[1]/main/div[1]/div/div/div/div/section/div/div[2]/div/div/div/div/div[2]/div/div/p[5]/iframe"

        iframe_tag = self.driver.find_element(
            By.XPATH,
            xpath,
        )

        source_page = iframe_tag.get_attribute("src")

        # Display the list of dictionaries
        raw_data["per_county"] = self._fetch(source_page)
        towns_url = []
        for r in raw_data["per_county"]:
            towns_url.append(source_page[:-5] + r["County"] + source_page[-5:])

        raw_data["per_town"] = []
        for url in towns_url:
            raw_data["per_town"] += self._fetch(url)

        return raw_data


class NYScraper:
    def __new__(cls, layout_id, url, emc):
        if layout_id == 1:
            obj = super().__new__(Scraper1)
        elif layout_id == 2:
            obj = super().__new__(Scraper2)
        elif layout_id == 3:
            obj = super().__new__(Scraper3)
        else:
            raise "Invalid layout ID: Enter layout ID range from 1 to 2"
        obj.__init__(url, emc)
        return obj
