from bs4 import BeautifulSoup
import json
from selenium.webdriver.common.by import By
from seleniumwire.utils import decode as sw_decode
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import requests
import pandas as pd
from .util import timenow
from datetime import datetime
import time

from .ga_scraper import (
    BaseScraper,
    Scraper1 as GA_Scraper1,
    Scraper9 as GA_Scraper9,
    Scraper4 as GA_Scraper4,
    Scraper11 as GA_Scraper11,
    Scraper3 as GA_Scraper3,
    Scraper5 as GA_Scraper5,
)


class Scraper1(BaseScraper):
    def __init__(self, url, emc):
        super().__init__(url, emc)
        self.driver = self.init_webdriver()

    def parse(self):
        data = self.fetch()
        per_county_df = pd.DataFrame(data["per_county"]["outages"])
        per_county_df = per_county_df[per_county_df["Customers Out"] != "0"]
        data.update({"per_county": per_county_df})

        return data

    def fetch(self):
        print(f"fetching {self.emc} outages from {self.url}")
        # Send a request to the website and let it load
        self.driver.get(self.url)

        # Sleeps for 5 seconds
        self.driver.implicitly_wait(5)

        raw_data = {}

        data = self.driver.find_element(By.TAG_NAME, "pre").text

        raw_data["per_county"] = json.loads(data)

        return raw_data


class Scraper2(BaseScraper):
    def __init__(self, url, emc):
        super().__init__(url, emc)
        self.driver = self.init_webdriver()

    def parse(self):
        data = self.fetch()
        df_data = [
            [
                data["per_city"]["summaryFileData"]["totals"][0]["total_cust_s"],
                data["per_city"]["summaryFileData"]["totals"][0]["total_outages"],
                data["per_city"]["summaryFileData"]["totals"][0]["total_cust_a"]["val"],
                data["per_city"]["summaryFileData"]["date_generated"],
            ]
        ]
        print(df_data)
        per_city_df = pd.DataFrame(
            df_data,
            columns=[
                "total_customers_served",
                "total_outages",
                "total_customers_affected",
                "timestamp",
            ],
        )
        data.update({"per_city": per_city_df})
        print(per_city_df)

        return data

    def fetch(self):
        print(f"fetching {self.emc} outages from {self.url}")
        # Send a request to the website and let it load
        self.driver.get(self.url)

        # Sleeps for 5 seconds
        self.driver.implicitly_wait(5)

        raw_data = {}

        data = self.driver.find_element(By.TAG_NAME, "pre").text

        raw_data["per_city"] = json.loads(data)

        return raw_data


class Scraper3(BaseScraper):
    def __init__(self, url, emc):
        super().__init__(url, emc)
        self.driver = self.init_webdriver()

    def parse(self):
        data = self.fetch()
        df_data = []
        for outage in data["per_outage"]:
            df_data.append(
                [
                    outage["id"],
                    outage["createdDate"],
                    outage["lastUpdated"],
                    outage["startDate"],
                    outage["numPeople"],
                    outage["latitude"],
                    outage["longitude"],
                ]
            )
        per_outage_df = pd.DataFrame(
            df_data,
            columns=[
                "id",
                "createdDate",
                "lastUpdated",
                "startDate",
                "peopleAffected",
                "latitude",
                "longitude",
            ],
        )
        per_outage_df = per_outage_df[per_outage_df["peopleAffected"] != 0]
        data.update({"per_outage": per_outage_df})

        return data

    def fetch(self):
        print(f"fetching {self.emc} outages from {self.url}")
        # Send a request to the website and let it load
        self.driver.get(self.url)

        # Sleeps for 5 seconds
        self.driver.implicitly_wait(5)

        raw_data = {}

        data = self.driver.find_element(By.TAG_NAME, "pre").text

        raw_data["per_outage"] = json.loads(data)

        return raw_data


class Scraper4(BaseScraper):
    def __init__(self, url, emc):
        super().__init__(url, emc)
        self.driver = self.init_webdriver()

    def parse(self):
        data = self.fetch()
        per_outage_df = pd.DataFrame.from_records(data["per_outage"]["data"])
        data.update({"per_outage": per_outage_df})

        return data

    def fetch(self):
        print(f"fetching {self.emc} outages from {self.url}")
        # Send a request to the website and let it load
        self.driver.get(self.url)

        # Sleeps for 5 seconds
        # self.driver.implicitly_wait(10)
        time.sleep(10)

        raw_data = {}

        requests = self.driver.requests
        for r in requests:
            if "outages?jurisdiction=DEF" in r.url:
                print(f"scraping data from {r.url}")
                response = sw_decode(
                    r.response.body,
                    r.response.headers.get("Content-Encoding", "identity"),
                )
                data = response.decode("utf8", "ignore")
                raw_data["per_outage"] = json.loads(data)
            elif "counties?jurisdiction=DEF" in r.url:
                print(f"scraping data from {r.url}")
                response = sw_decode(
                    r.response.body,
                    r.response.headers.get("Content-Encoding", "identity"),
                )
                data = response.decode("utf8", "ignore")
                raw_data["per_county"] = json.loads(data)

        return raw_data


class Scraper5(BaseScraper):
    def __init__(self, url, emc):
        super().__init__(url, emc)
        self.driver = self.init_webdriver()

    def parse(self):
        data = self.fetch()
        df_data = []
        for obj in data["per_outage"]:
            for outage in obj["hits"]["hits"]:
                # print(outage)
                d = {
                    "id": outage["_id"],
                    "customerCount": outage["_source"]["customerCount"],
                    "estimatedTimeOfRestoration": outage["_source"][
                        "estimatedTimeOfRestoration"
                    ],
                    "reason": outage["_source"]["reason"],
                    "status": outage["_source"]["status"],
                    "updateTime": outage["_source"]["updateTime"],
                }
                df_data.append(d)
        per_outage_df = pd.DataFrame.from_records(df_data)
        data.update({"per_outage": per_outage_df})

        return data

    def fetch(self):
        print(f"fetching {self.emc} outages from {self.url}")
        # Send a request to the website and let it load
        self.driver.get(self.url)

        # Sleeps for 5 seconds
        # self.driver.implicitly_wait(5)
        time.sleep(5)

        raw_data = {}

        requests = self.driver.requests
        raw_data["per_outage"] = []
        for r in requests:
            if "_outage" in r.url:
                # print(f"scraping data from {r.url}")
                response = sw_decode(
                    r.response.body,
                    r.response.headers.get("Content-Encoding", "identity"),
                )
                data = response.decode("utf8", "ignore")
                raw_data["per_outage"].append(json.loads(data))
        return raw_data


# can't figure out how to scrape the xml
class Scraper6(BaseScraper):
    def __init__(self, url, emc):
        super().__init__(url, emc)
        self.driver = self.init_webdriver()

    def parse(self):
        data = self.fetch()
        per_outage_df = pd.DataFrame.from_records(data)
        data.update({"per_outage": per_outage_df})

        return data

    def fetch(self):
        print(f"fetching {self.emc} outages from {self.url}")
        # Send a request to the website and let it load
        self.driver.get(self.url)

        # Sleeps for 5 seconds
        self.driver.implicitly_wait(5)

        raw_data = {}
        xml_data = requests.get(self.url).content
        soup = BeautifulSoup(xml_data, "xml")
        report = soup.find_all("report")
        print(report)
        for t in report.find_all("t"):
            print(t.find("e"))
        return raw_data


class Scraper7(BaseScraper):
    def __init__(self, url, emc):
        super().__init__(url, emc)
        self.driver = self.init_webdriver()

    def parse(self):
        data = self.fetch()
        per_outage_df = pd.DataFrame.from_records(data["per_outage"]["returndata"])
        data.update({"per_outage": per_outage_df})

        return data

    def fetch(self):
        print(f"fetching {self.emc} outages from {self.url}")
        # Send a request to the website and let it load
        self.driver.get(self.url)

        self.driver.implicitly_wait(5)

        raw_data = {}

        data = self.driver.find_element(By.TAG_NAME, "pre").text

        raw_data["per_outage"] = json.loads(data)

        return raw_data


class Scraper8(BaseScraper):
    def __init__(self, url, emc):
        super().__init__(url, emc)
        self.driver = self.init_webdriver()

    def parse(self):
        data = self.fetch()
        per_outage_df = pd.DataFrame.from_records(data)
        data.update({"per_outage": per_outage_df})

        return data

    def fetch(self):
        print(f"fetching {self.emc} outages from {self.url}")
        # Send a request to the website and let it load
        self.driver.get(self.url)

        self.driver.implicitly_wait(5)

        raw_data = {}

        data = self.driver.find_element(By.TAG_NAME, "pre").text

        raw_data["per_outage"] = json.loads(data)

        return raw_data


# TODO: This scraper is similar to NC-1, need to refactor
class Scraper9(BaseScraper):
    def __init__(self, url, emc):
        super().__init__(url, emc)
        self.driver = self.init_webdriver()

    def parse(self):
        data = self.fetch()

        for key, val in data.items():
            print(key)
            if val:
                df = pd.DataFrame(val)
                df = df[(df["Number of Outages"] != 0)]
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

        # Initialize dictionary to hold table data
        table_data = {
            "Location": [],
            "Number of Outages": [],
            "Affected Customers": [],
            "Percentage Affected": [],
            "Last Updated": [],
        }

        try:
            span_element = WebDriverWait(self.driver, 20).until(
                EC.element_to_be_clickable(
                    (
                        By.XPATH,
                        "//span[@class='jurisdiction-selection-select-state__item-text' and text()='Duke Energy Carolinas']",
                    )
                )
            )

            # Click on the span element
            span_element.click()

            print("Clicked on 'Duke Energy Carolinas' successfully!")
        except:
            print("Skip clicking on 'Duke Energy Carolinas'")
            pass  # we might have cached it

        try:
            h3_element = WebDriverWait(self.driver, 20).until(
                EC.element_to_be_clickable((By.CLASS_NAME, "maps-panel-title"))
            )

            # Click on the h3 element
            h3_element.click()
            print("Clicked on 'Report & View Outages' successfully!")

            outage_summary_button = WebDriverWait(self.driver, 10).until(
                EC.element_to_be_clickable(
                    (By.XPATH, "//button[@aria-label='Outage Summary']")
                )
            )
            # Click on the "OUTAGE SUMMARY" button
            outage_summary_button.click()

            print("summary clicked successfully!")

            # print out current page source
            # print(self.driver.page_source)

            time.sleep(5)

            outage_summary_table_span = WebDriverWait(self.driver, 10).until(
                EC.element_to_be_clickable(
                    (
                        By.XPATH,
                        "/html/body/app-root/outage-home/section/county-panel/section/div[2]/div[4]/button/span",
                    )
                )
            )

            # Click on the "Outage Summary Table" span element
            outage_summary_table_span.click()

            print("Clicked on 'Outage Summary Table' successfully!")

            WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located(
                    (By.CLASS_NAME, "outage-summary-table-content-row")
                )
            )

            # Find all table rows
            table_rows = self.driver.find_elements(
                By.CLASS_NAME, "outage-summary-table-content-row"
            )

            # Iterate over table rows
            for row in table_rows:
                cells = row.find_elements(
                    By.CLASS_NAME, "outage-summary-table-content-body-item"
                )
                table_data["Location"].append(cells[0].text)
                table_data["Number of Outages"].append(cells[1].text)
                table_data["Affected Customers"].append(cells[2].text)
                table_data["Percentage Affected"].append(cells[3].text)
                table_data["Last Updated"].append(cells[4].text)
            print("Table data created successfully!")
        except Exception as e:
            print(f"Error: {e}")
            self.driver.close()
            self.driver.quit()

        raw_data = {}
        raw_data["per_county"] = table_data
        return raw_data


class FLScraper:
    def __new__(cls, layout_id, url, emc):
        if layout_id == 1:
            obj = super().__new__(GA_Scraper1)
        elif layout_id == 2:
            obj = super().__new__(GA_Scraper9)
        elif layout_id == 3:
            obj = super().__new__(GA_Scraper4)
        elif layout_id == 4:
            obj = super().__new__(GA_Scraper11)
        elif layout_id == 5:
            obj = super().__new__(GA_Scraper5)
        elif layout_id == 6:
            obj = super().__new__(Scraper1)
        elif layout_id == 7:
            obj = super().__new__(Scraper3)
        elif layout_id == 8:
            obj = super().__new__(Scraper9)
        elif layout_id == 9:
            obj = super().__new__(Scraper5)
        elif layout_id == 10:
            obj = super().__new__(Scraper6)
        elif layout_id == 11:
            obj = super().__new__(Scraper7)
        elif layout_id == 12:
            obj = super().__new__(GA_Scraper3)
        else:
            raise "Invalid layout ID: Enter layout ID range from 1 to 12"
        obj.__init__(url, emc)
        return obj
