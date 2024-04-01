from bs4 import BeautifulSoup
import json
from selenium.webdriver.common.by import By
from seleniumwire.utils import decode as sw_decode
import requests
import pandas as pd

from .ga_scraper import (
    BaseScraper,
    Scraper1 as GA_Scraper1,
    Scraper9 as GA_Scraper9,
    Scraper4 as GA_Scraper4,
    Scraper11 as GA_Scraper11,
    Scraper5 as GA_Scraper5
)

class Scraper1(BaseScraper):
    def __init__(self, url, emc):
        super().__init__(url, emc)
        self.driver = self.init_webdriver()

    def parse(self):
        data = self.fetch()
        per_county_df = pd.DataFrame(data["per_county"]["outages"])
        data.update({"per_county": per_county_df})

        return data

    def fetch(self):
        print(f"fetching {self.emc} outages from {self.url}")
        # Send a request to the website and let it load
        self.driver.get(self.url)

        # Sleeps for 5 seconds
        self.driver.implicitly_wait(5)

        raw_data = {}

        data = self.driver.find_element(By.TAG_NAME, 'pre').text
            
        raw_data["per_county"] = json.loads(data)

        return raw_data
    
class Scraper2(BaseScraper):
    def __init__(self, url, emc):
        super().__init__(url, emc)
        self.driver = self.init_webdriver()

    def parse(self):
        data = self.fetch()
        df_data = [[
            data["per_city"]["summaryFileData"]["totals"][0]["total_cust_s"],
            data["per_city"]["summaryFileData"]["totals"][0]["total_outages"],
            data["per_city"]["summaryFileData"]["totals"][0]["total_cust_a"]["val"],
            data["per_city"]["summaryFileData"]["date_generated"]
        ]]
        print(df_data)
        per_city_df = pd.DataFrame(df_data, columns=["total_customers_served", "total_outages", "total_customers_affected", "timestamp"])
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

        data = self.driver.find_element(By.TAG_NAME, 'pre').text
            
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
            df_data.append([outage.id, outage.createdDate, outage.lastUpdated, outage.startDate, outage.numPeople, outage.latitude, outage.longitude])
        per_outage_df = pd.DataFrame(df_data, columns=["id", "createdDate", "lastUpdated", "startDate", "peopleAffected", "latitude", "longitude"])
        data.update({"per_outage": per_outage_df})

        return data

    def fetch(self):
        print(f"fetching {self.emc} outages from {self.url}")
        # Send a request to the website and let it load
        self.driver.get(self.url)

        # Sleeps for 5 seconds
        self.driver.implicitly_wait(5)

        raw_data = {}

        data = self.driver.find_element(By.TAG_NAME, 'pre').text
            
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
        self.driver.implicitly_wait(5)

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
                print(outage)
                d = {
                    "id": outage["_id"],
                    "customerCount": outage["_source"]["customerCount"],
                    "estimatedTimeOfRestoration": outage["_source"]["estimatedTimeOfRestoration"],
                    "reason": outage["_source"]["reason"],
                    "status": outage["_source"]["status"],
                    "updateTime": outage["_source"]["updateTime"]
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
        self.driver.implicitly_wait(5)

        raw_data = {}

        requests = self.driver.requests
        raw_data["per_outage"] = []
        for r in requests:
            if "_outage" in r.url:
                print(f"scraping data from {r.url}")
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

        data = self.driver.find_element(By.TAG_NAME, 'pre').text
            
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

        data = self.driver.find_element(By.TAG_NAME, 'pre').text
            
        raw_data["per_outage"] = json.loads(data)

        return raw_data

class FLScraper:
    def __new__(cls, layout_id, url, emc):
        if layout_id == 1:
            obj = super().__new__(GA_Scraper1)
        elif layout_id == 2:
            obj = super().__new__(GA_Scraper9)
        elif layout_id == 3:
            obj = super().__new__(Scraper2)
        elif layout_id == 4:
            obj = super().__new__(GA_Scraper11)
        elif layout_id == 5:
            obj = super().__new__(GA_Scraper5)
        elif layout_id == 6:
            obj = super().__new__(Scraper1)
        elif layout_id == 7:
            obj = super().__new__(Scraper3)
        elif layout_id == 8:
            obj = super().__new__(Scraper4)
        elif layout_id == 9:
            obj = super().__new__(Scraper5)
        elif layout_id == 10:
            obj = super().__new__(Scraper6)
        elif layout_id == 11:
            obj = super().__new__(Scraper7)
        else:
            raise "Invalid layout ID: Enter layout ID range from 1 to 2"
        obj.__init__(url, emc)
        return obj