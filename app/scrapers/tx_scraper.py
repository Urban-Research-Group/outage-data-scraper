import logging
import json
import pandas as pd
import time

from bs4 import BeautifulSoup
from datetime import datetime
from seleniumwire.utils import decode as sw_decode
from .util import make_request, timenow
from .ga_scraper import (
    BaseScraper,
    Scraper1 as GA_Scraper1,
    Scraper2 as GA_Scraper2,
    Scraper3 as GA_Scraper3,
    Scraper6 as GA_Scraper6,
    Scraper9 as GA_Scraper9,
    Scraper11 as GA_Scraper11,
)
from selenium.webdriver.common.by import By

# TODO: update for security
import ssl

ssl._create_default_https_context = ssl._create_unverified_context
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException

class Scraper1(BaseScraper):
    def __init__(self, url, emc):
        super().__init__(url, emc)
        self.driver = self.init_webdriver()

    def parse(self):
        suffix = ["?report=report-panel-county", "?report=report-panel-zip"]

        data = {}
        for s in suffix:
            url = self.url + s
            print(f"fetching {self.emc} outages from {url}")
            html = self.get_page_source(url = url, find_type = "css", findkeyword = ".report-table.tree")
            # parse table
            soup = BeautifulSoup(html, "lxml")
            table = soup.select_one(".report-table.tree")
            rows = table.select("tr")
            
            # Change from appending to a list to list comprehension will speed the list operation about 50%.
            raw_data = [[cell.text.strip() for cell in row.find_all('td')] for row in rows[2:]]

            loc = "COUNTY" if s == "?report=report-panel-county" else "ZIP"
            header = ["VIEW", loc, "CUSTOMER OUTAGES", "CUSTOMERS SERVED", "% AFFECTED"]
            table_data = [dict(zip(header, row)) for row in raw_data]
            df = pd.DataFrame(table_data)[
                [loc, "CUSTOMER OUTAGES", "CUSTOMERS SERVED", "% AFFECTED"]
            ]                              
            df["timestamp"] = timenow()
            df["EMC"] = self.emc
            df = df[df["CUSTOMER OUTAGES"] != "0"]
            key = "per_county" if loc == "COUNTY" else "per_zipcode"
            data.update({key: df})

        return data

class Scraper3(BaseScraper):
    def __init__(self, url, emc):
        super().__init__(url, emc)
        self.driver = self.init_webdriver()

    def parse(self):
        pass

    def fetch(self=None):
        pass


class Scraper4(BaseScraper):
    def __init__(self, url, emc):
        super().__init__(url, emc)

    def parse(self):
        data = self.fetch()

        for key, val in data.items():
            df = pd.DataFrame(val)
            df["lastUpdatedTime"] = df["lastUpdatedTime"].apply(
                pd.to_datetime, unit="ms"
            )
            df["timestamp"] = timenow()
            df["EMC"] = self.emc
            data.update({key: df})

        return data


class Scraper5(BaseScraper):
    def __init__(self, url, emc):
        super().__init__(url, emc)
        self.driver = self.init_webdriver()
    
    def parse(self):
        data = self.fetch()

        for key, val in data.items():
            if val:
                df = pd.DataFrame(val["areas"])
                df['cust_a'] = df['cust_a'].map(lambda x: x['val'])
                df['percent_cust_a'] = df['percent_cust_a'].map(lambda x: x['val'])

                # Filter rows in a more efficient manner
                df = df.query("cust_a != 0 or n_out != 0")

                df = df.copy()  # Make a copy to avoid SettingWithCopyWarning
                df['timestamp'] = pd.Timestamp.now()
                df['EMC'] = self.emc

                # Now, since df is explicitly copied, this operation should not cause warnings
                df.drop(columns=['gotoMap'], inplace=True)
                data.update({key: df})
            else:
                print(
                    f"no outage of {self.emc} update found at",
                    datetime.strftime(datetime.now(), "%m-%d-%Y %H:%M:%S"),
                )
        # print(data)
        return data
    
    
    def wait_for_json_request(self, part_of_url, timeout=10):
        """
        Waits for a JSON request to be made that contains a specific part in its URL.
        Args:
            part_of_url: A substring of the URL to look for.
            timeout: How long to wait for the request, in seconds.
        """
        start_time = time.time()
        while True:
            requests = [r for r in self.driver.requests if part_of_url in r.url and
                        r.response and 'application/json' in r.response.headers.get('Content-Type', '')]
            if requests:
                return requests[0]  # Return the first matching request
            elif time.time() - start_time > timeout:
                raise TimeoutError(f"JSON request containing '{part_of_url}' not found within timeout.")
            time.sleep(0.5)  # Short sleep to avoid busy loop


    def fetch(self):
        print(f"fetching {self.emc} outages from {self.url}")
        # get javascript rendered source page

        # let the page load
        if self.emc != "Texas-New Mexico Power Co.":
            findkeyword = "a.row.report-link.hyperlink-primary"
            page_source = self.get_page_source(find_type="css", findkeyword=findkeyword)

        else:
            self.driver.get(self.url)
            iframe_tag = self.driver.find_elent(By.ID, "sc5_iframe")
            source_page = iframe_tag.get_attribute("src")
            print("Redirect to", source_page)
            self.url = "https://kubra.io/"
            page_source = self.get_page_source(url=source_page, 
                                               find_type="css",
                                               findkeyword="a.row.report-link.hyperlink-primary")
            
        soup = BeautifulSoup(page_source, "lxml")
        containers = soup.find_all(class_="row report-link hyperlink-primary")
        links = {}
        counter = 0  # we use counter to avoid duplicate key bug
        for c in containers:
            links.update({counter: c.get("href")})
            counter += 1
        # get json reports
        raw_data = {}
        for k, v in links.items():
            new_url = self.url + v[1:]
            self.driver.get(new_url)
            
            # Use the dynamic wait method instead of time.sleep(5)
            hash_part = v.split("/")[-1]
            try:
                json_request = self.wait_for_json_request(hash_part)
                response = sw_decode(
                    json_request.response.body,
                    json_request.response.headers.get("Content-Encoding", "identity"),
                )
                data = response.decode("utf8", "ignore")
                if any([x in data for x in ["zip", "Zip"]]):
                    raw_data["per_zipcode"] = json.loads(data)["file_data"]
                elif "county" in data:
                    raw_data["per_county"] = json.loads(data)["file_data"]
                elif any([x in data for x in ["city", "Cities"]]):
                    raw_data["per_city"] = json.loads(data)["file_data"]
            except TimeoutError as e:
                print(e)
        return raw_data


class Scraper6(BaseScraper):
    def __init__(self, url, emc):
        super().__init__(url, emc)
        self.driver = self.init_webdriver()
    
    def parse(self):
        print(f"fetching {self.emc} outages from {self.url}")
        # Send a request to the website and let it load
        self.driver.get(self.url)

        try:
            self.wait_for_request(
                lambda request: "geometryType=esriGeometryEnvelope" in request.url
            )
        except TimeoutError:
            print("The specific request was not made within the timeout period.")
            return {}

        data = {}
        for request in self.driver.requests:
            if "geometryType=esriGeometryEnvelope" in request.url:
                response = sw_decode(
                    request.response.body,
                    request.response.headers.get("Content-Encoding", "identity"),
                )
                data_str = response.decode()
                if data_str[0] == "{":
                    data["per_outage"] = json.loads(data_str)
                else:
                    start = data_str.index("(") + 1
                    end = data_str.rindex(")")
                    data["per_outage"] = json.loads(data_str[start:end])

        for key, val in data.items():

            df = pd.DataFrame([x["attributes"] for x in val["features"]])
            # Convert date columns directly without apply()
            df["BEGINTIME"] = pd.to_datetime(df["BEGINTIME"], unit="ms")
            df["ESTIMATEDTIMERESTORATION"] = pd.to_datetime(df["ESTIMATEDTIMERESTORATION"], unit="ms")

            # For geometry, assuming each geometry is a dictionary with 'x' and 'y' keys
            # Extract 'x' and 'y' directly into the DataFrame without creating a separate DataFrame first
            df["x"] = [x["geometry"]["x"] for x in val["features"]]
            df["y"] = [x["geometry"]["y"] for x in val["features"]]
            # Set a single timestamp for the entire column
            current_timestamp = pd.Timestamp.now()  # Or use your 'timenow()' if it's different
            df["timestamp"] = current_timestamp

            df["EMC"] = self.emc  # Assign EMC value directly
            # df.dropna(inplace=True)
            data.update({key: df})
            
        return data


class Scraper7(BaseScraper):
    def __init__(self, url, emc):
        super().__init__(url, emc)
        self.driver = self.init_webdriver()

    def parse(self):
        print(f"fetching {self.emc} outages from {self.url}")
        # Send a request to the website and let it load
        self.driver.get(self.url)
        
        try:
            self.wait_for_request(
                lambda request: "loadLatLongOuterOutage" in request.url
            )
        except TimeoutError:
            print("The specific request was not made within the timeout period.")
            return {}

        data = {}
        for request in self.driver.requests:
            if "loadLatLongOuterOutage" in request.url:
                response = sw_decode(
                    request.response.body,
                    request.response.headers.get("Content-Encoding", "identity"),
                )
                data["per_outage"] = json.loads(response.decode("utf8", "ignore"))

        for key, val in data.items():
            df = pd.DataFrame(json.loads(val["d"])["Table"])
            current_timestamp = pd.Timestamp.now()  # Call once and use for all rows
            df["timestamp"] = current_timestamp
            df["EMC"] = self.emc
            df = df.dropna()
            data.update({key: df})

        return data


class Scraper10(BaseScraper):
    def __init__(self, url, emc):
        super().__init__(url, emc)

    def parse(self):
        data = self.fetch()

        for key, val in data.items():
            df = pd.DataFrame(val["outageLst"])
            df["zip"] = df.apply(
                lambda row: self.extract_zipcode(row["lat"], row["lon"]), axis=1
            )
            df["timestamp"] = timenow()
            df["EMC"] = self.emc
            # df = df[df['status'] != 'Restored']
            data.update({key: df})

        return data


class Scraper18(BaseScraper):
    def __init__(self, url, emc):
        super().__init__(url, emc)

    def parse(self):
        # TODO: to be implemented
        data = self.fetch()
        for key, val in data.items():
            df = pd.DataFrame(val)
            df["timestamp"] = timenow()
            df["EMC"] = self.emc
            df = df[df["affectedCount"] != 0]
            data.update({key: df})
        return data


class TXScraper:
    def __new__(cls, layout_id, url, emc):
        if layout_id == 1:
            obj = super().__new__(Scraper1)
        # elif layout_id == 2:
        #     obj = super().__new__(Scraper2)
        # elif layout_id == 3:
        #     obj = super().__new__(Scraper3)
        elif layout_id == 4:
            obj = super().__new__(Scraper4)
        elif layout_id == 5:
            obj = super().__new__(Scraper5)
        elif layout_id == 6:
            obj = super().__new__(Scraper6)
        elif layout_id == 7:
            obj = super().__new__(Scraper7)
        elif layout_id == 8:
            obj = super().__new__(GA_Scraper1)
        # elif layout_id == 9:
        #     obj = super().__new__(Scraper9)
        elif layout_id == 10:
            obj = super().__new__(Scraper10)
        elif layout_id == 11:
            obj = super().__new__(GA_Scraper9)
        elif layout_id == 12:
            obj = super().__new__(GA_Scraper11)
        elif layout_id == 13:
            obj = super().__new__(GA_Scraper6)
        # elif layout_id == 14:
        #     obj = super().__new__(Scraper14)
        # elif layout_id == 15:
        #     obj = super().__new__(Scraper15)
        elif layout_id == 16:
            obj = super().__new__(GA_Scraper3)
        elif layout_id == 17:
            obj = super().__new__(GA_Scraper1)
        elif layout_id == 18:
            obj = super().__new__(Scraper18)
        else:
            raise "Invalid layout ID: Enter layout ID range from 1 to 18"

        obj.__init__(url, emc)
        return obj
