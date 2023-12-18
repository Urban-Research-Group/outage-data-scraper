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
    Scraper9 as GA_Scraper9,
    Scraper3 as GA_Scraper3,
    Scraper11 as GA_Scraper11,
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
                    print(f"got county data")
                elif "report_zip" in url:
                    print(url)
                    raw_data["per_zipcode"] = requests.get(url).json()["file_data"][
                        "areas"
                    ][0]["areas"]
                    print(f"got zip code data")
        except Exception as e:
            print(f"Error: {e}")
            self.driver.close()
            self.driver.quit()

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
        try:
            # parse reports link
            iframe_tag = self.driver.find_element(
                By.XPATH,
                "/html/body/app-root/app-euds-opco-branding/div/div/div/app-page-article/div/div[2]/div/div[2]/app-section/section/app-euds-card/div/app-iframe/iframe",
            )
            source_page = iframe_tag.get_attribute("src")
            print(f"Redirect to source_page: {source_page}")

            self.driver.get(source_page)

            time.sleep(5)
            page_source = self.driver.page_source

            soup = BeautifulSoup(page_source, "html.parser")

            containers = soup.find_all(class_="row report-link hyperlink-primary")
            links = {}
            for c in containers:
                links.update({c.getText(): c.get("href")})

            # get json reports
            raw_data = {}
            for k, v in links.items():
                self.url = "https://kubra.io/"
                self.driver.get(self.url + v[1:])
                print(f"Fetching data from {self.url+v[1:]}")
                time.sleep(5)
                requests = self.driver.requests
                visited = []  # somehow it we request multiple times
                for r in requests:
                    if "report.json" in r.url and r.url not in visited:
                        visited.append(r.url)
                        print(f"Fetching report from {r.url}")
                        response = sw_decode(
                            r.response.body,
                            r.response.headers.get("Content-Encoding", "identity"),
                        )
                        data = json.loads(response.decode("utf8", "ignore"))

                        if "county" in data["file_title"]:
                            raw_data["per_county"] = data["file_data"]
                            print(f"got county data")
                        elif "ctv" in data["file_title"]:
                            raw_data["per_city_town_village"] = data["file_data"]
                            print(f"got city, town, and village data")
                        elif "ward" in data["file_title"]:
                            raw_data["per_chicago_ward"] = data["file_data"]
                            print(f"got chicago ward data")

        except Exception as e:
            print(e)
            self.driver.close()
            self.driver.quit()
        return raw_data


class ILScraper:
    def __new__(cls, layout_id, url, emc):
        if layout_id == 1:
            obj = super().__new__(Scraper1)
        elif layout_id == 2:
            obj = super().__new__(Scraper2)
        elif layout_id == 3:
            obj = super().__new__(GA_Scraper9)
        elif layout_id == 4:
            obj = super().__new__(GA_Scraper1)
        elif layout_id == 5:
            obj = super().__new__(GA_Scraper3)
        elif layout_id == 6:
            obj = super().__new__(GA_Scraper11)
        else:
            raise "Invalid layout ID: Enter layout ID range from 1 to 2"
        obj.__init__(url, emc)
        return obj
