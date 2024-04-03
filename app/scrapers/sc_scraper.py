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


class Scraper1(BaseScraper): 
    def __init__(self, url, emc): #outageentry
        super().__init__(url, emc)
        self.driver = self.init_webdriver()

    def parse(self):
        data = self.fetch()

        for key, val in data.items():
            if key == "per_substation":
                per_substation_df = pd.DataFrame(val["rows"]["subs"])
                per_substation_df["timestamp"] = timenow()
                per_substation_df["EMC"] = self.emc
                per_substation_df = per_substation_df[
                    (per_substation_df.SubTotalConsumersOut != 0)
                    | (per_substation_df.SubTotalMetersAffectedByDeviceOutages != 0)
                ]
                data.update({key: per_substation_df})
            elif key == "per_county":
                per_county_df = pd.DataFrame(val["rows"])
                per_county_df["timestamp"] = timenow()
                per_county_df["EMC"] = self.emc
                per_county_df = per_county_df[per_county_df.out != 0]
                data.update({key: per_county_df})
            elif key == "per_outage":
                if val:
                    isHighTraffic = val["isHighTraffic"]
                    updateTime = val["timestamp"]
                    per_outage_df = pd.DataFrame()
                    for k, v in val.items():
                        if isinstance(v, dict):
                            if v["markers"]:
                                df = pd.DataFrame(v["markers"])
                                df["service_index_name"] = v["service_index_name"]
                                df["outages"] = v["outages"]
                                df["NumConsumers"] = v["stats"]["NumConsumers"]
                                df["zip_code"] = df.apply(
                                    lambda row: self.extract_zipcode(
                                        row["lat"], row["lon"]
                                    ),
                                    axis=1,
                                )
                                per_outage_df = df

                    per_outage_df["isHighTraffic"] = isHighTraffic
                    per_outage_df["updateTime"] = updateTime
                    per_outage_df["timestamp"] = timenow()
                    per_outage_df["EMC"] = self.emc
                    data.update({key: per_outage_df})

        self.driver.close()
        self.driver.quit()

        return data

    def fetch(self=None):
        print(f"fetching {self.emc} outages from {self.url}")
        raw_data = {}

        # Send a request to the website and let it load
        self.driver.get(self.url)

        # Sleeps for 5 seconds
        time.sleep(5)

        
        for request in self.driver.requests:
            if "ShellOut" in request.url:
                response = sw_decode(
                    request.response.body,
                    request.response.headers.get("Content-Encoding", "identity"),
                )
                data = response.decode("utf8", "ignore")
                if "sub_outages" in data:
                    raw_data["per_substation"] = json.loads(data)
                elif "cfa_county_data" in data:
                    raw_data["per_county"] = json.loads(data)
                elif "isHighTraffic" in data:
                    raw_data["per_outage"] = json.loads(data)
       
        return raw_data
    
class Scraper2(BaseScraper):
    def __init__(self, url, emc):
        super().__init__(url, emc)
        self.driver = self.init_webdriver()

    def parse(self):
        data = self.fetch()

        for key, val in data.items():
            if not val:
                data.update({key: pd.DataFrame()})
            else:
                if key == "per_county":
                    # Turns out that some val has more than one item
                    boundaries_lists = [item["boundaries"] for item in val]

                    flattened_boundaries = []
                    for b in boundaries_lists:
                        flattened_boundaries = flattened_boundaries + b

                    # Creating a DataFrame from the concatenated list
                    per_loc_df = pd.DataFrame(flattened_boundaries)
                    # per_loc_df = pd.DataFrame(val[0]["boundaries"])

                    per_loc_df = per_loc_df[
                        (per_loc_df["customersAffected"] != 0)
                        | (per_loc_df["customersOutNow"] != 0)
                    ]
                    per_loc_df["timestamp"] = timenow()
                    per_loc_df["EMC"] = self.emc
                    data.update({key: per_loc_df})
                if key == "per_outage":
                    per_outage_df = pd.DataFrame(val)
                    per_outage_df["timestamp"] = timenow()
                    zips = [
                        self.extract_zipcode(
                            x["outagePoint"]["lat"], x["outagePoint"]["lng"]
                        )
                        for x in val
                    ]
                    per_outage_df["zip"] = zips
                    per_outage_df["EMC"] = self.emc
                    data.update({key: per_outage_df})

        return data

    def fetch(self):
        print(f"fetching {self.emc} outages from {self.url}")
        raw_data = {}
        if self.emc == "Taylor Electric Coop, Inc.":
            self.driver.get(self.url)
            time.sleep(5)
            _ = self.driver.page_source
            self.url = self.driver.find_element(
                By.XPATH,
                "/html/body/div/div[1]/div[2]/div/div/main/article/div/div/div/div/section/div/div[2]/div/div[4]/div/div/a",
            ).get_attribute("href")
            print("new url", self.url)

        with urlopen(self.url + "data/boundaries.json") as response:
            raw_data["per_county"] = json.loads(response.read())

        with urlopen(self.url + "data/outages.json") as response:
            raw_data["per_outage"] = json.loads(response.read())

        return raw_data
    
class SCScraper3(BaseScraper):
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
        tables = soup.find_all("table")
        # separate rows
        rows = tables[1].find_all("tr")
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
            df = df[df["# Out"] != "0"]
        else:
            df = pd.DataFrame()

        return df

    def fetch(self):
        print(f"fetching {self.emc} outages from {self.url}")
        self.driver.get(self.url)
        time.sleep(10)

        if self.emc != "Karnes Electric Coop, Inc.":
            button = self.driver.find_elements(
                "xpath", '//*[@id="OMS.Customers Summary"]'
            )
            if button:
                wait = WebDriverWait(self.driver, 10)
                label = wait.until(
                    EC.element_to_be_clickable(
                        (By.XPATH, '//*[@id="OMS.Customers Summary"]')
                    )
                )
                self.driver.execute_script("arguments[0].scrollIntoView();", label)
                label.click()

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
    
class SCScraper4(BaseScraper):
    def __init__(self, url, emc):
        super().__init__(url, emc)

    def parse(self):
        data = self.fetch()

        for key, val in data.items():
            if not val:
                data.update({key: pd.DataFrame()})
            else:
                if key == "per_county":
                    per_loc_df = pd.DataFrame(val)
                    per_loc_df = per_loc_df[per_loc_df["CustomersAffected"] != "0"]
                    per_loc_df["timestamp"] = timenow()
                    per_loc_df["EMC"] = self.emc
                    per_loc_df.drop(columns=["Shape"], inplace=True)
                    data.update({key: per_loc_df})
                if key == "per_outage":
                    per_outage_df = pd.DataFrame(val["MobileOutage"])
                    per_outage_df["timestamp"] = timenow()
                    # TODO: sometimes error? mapping later
                    # zips = [self.extract_zipcode(x['X'], x['Y']) for x in [val['MobileOutage']]]
                    # zips = [self.extract_zipcode(x['Y'], x['X']) for x in [val['MobileOutage']]]
                    # per_outage_df['zip'] = zips
                    per_outage_df["EMC"] = self.emc
                    data.update({key: per_outage_df})

        return data

    def fetch(self):
        print(f"fetching {self.emc} outages from {self.url}")
        raw_data = {}
        print(self.url)

        # TODO: simplify
        body, response = make_request(
            self.url + "MobileMap/OMSMobileService.asmx/GetAllCounties"
        )
        temp = xmltodict.parse(body.decode("utf8"))
        if "MobileCounty" in temp["ArrayOfMobileCounty"]:
            raw_data["per_county"] = temp["ArrayOfMobileCounty"]["MobileCounty"]

        body, response = make_request(
            self.url + "MobileMap/OMSMobileService.asmx/GetAllOutages"
        )
        temp = xmltodict.parse(body.decode("utf8"))
        raw_data["per_outage"] = temp["MobileOutageInfo"]["Outages"]
        return raw_data





class SCScraper: 
    def __new__(cls, layout_id, url, emc):
        if layout_id == 1:
            obj = super().__new__(Scraper1) 
        if layout_id == 2:
            obj = super().__new__(Scraper2) #GA1
        else: 
            raise "Invalid layout ID: Enter layout ID range from 1 to 2"
        obj.__init__(url, emc)
        return obj

