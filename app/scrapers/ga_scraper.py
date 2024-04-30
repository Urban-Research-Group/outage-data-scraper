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
        self.driver = None
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

    def wait_for_request(self, condition, timeout=10):
        """
        Waits for a specific request to be made that matches the given condition.
        Args:
            condition: A function that takes a request object and returns True if the condition is met.
            timeout: How long to wait for the condition to be met, in seconds.
        """
        start_time = time.time()
        while True:
            for request in self.driver.requests:
                if condition(request):
                    return request  # Return the request if condition is met
            if time.time() - start_time > timeout:
                raise TimeoutError("Request not found within timeout period.")
            time.sleep(0.5)  # Short sleep to avoid an overly busy loop

    def get_page_source(self, url=None, find_type=None, findkeyword=None, timeout=10):

        url = url if url else self.url
        self.driver.get(url)

        # Map find_type to Selenium's By types
        find_type_map = {
            "class": By.CLASS_NAME,
            "id": By.ID,
            "tag": By.TAG_NAME,
            "css": By.CSS_SELECTOR,
            "xpath": By.XPATH,
            "name": By.NAME,
            "link_text": By.LINK_TEXT,
            "partial_link_text": By.PARTIAL_LINK_TEXT,
        }

        # Check if the provided find_type is supported
        if find_type not in find_type_map:
            raise ValueError(f"Unsupported find_type: {find_type}")
        by_type = find_type_map[find_type]

        # let the page load
        try:
            # wait for the element to load
            WebDriverWait(self.driver, timeout).until(
                # css_selector is often same as the keyword BeautifulSoup trying to find
                EC.presence_of_element_located((by_type, findkeyword))
            )
            page_source = self.driver.page_source
        except Exception as e:
            print(f"An error occurred: {e}")
            page_source = None  # Return None or appropriate content in case of failure
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

    def wait_for_json_request(self, part_of_url, timeout=10):
        """
        Waits for a JSON request to be made that contains a specific part in its URL.
        Args:
            part_of_url: A substring of the URL to look for.
            timeout: How long to wait for the request, in seconds.
        """
        start_time = time.time()

        if self.driver == None:
            self.driver = self.init_webdriver()

        while True:
            requests = [
                r
                for r in self.driver.requests
                if part_of_url in r.url
                and r.response
                and "application/json" in r.response.headers.get("Content-Type", "")
            ]
            if requests:
                return requests[0]  # Return the first matching request
            elif time.time() - start_time > timeout:
                raise TimeoutError(
                    f"JSON request containing '{part_of_url}' not found within timeout."
                )
            time.sleep(0.5)  # Short sleep to avoid busy loop

    def init_webdriver(self):
        chrome_driver_path = (
            "/opt/chromedriver"
            if is_aws_env()
            else "/Users/xuanzhangliu/Downloads/chromedriver-mac-arm64/chromedriver"
        )

        desired_capabilities = DesiredCapabilities.CHROME.copy()
        desired_capabilities["pageLoadStrategy"] = "eager"  # or 'none'
        desired_capabilities["goog:loggingPrefs"] = {"performance": "ALL"}
        desired_capabilities["acceptInsecureCerts"] = True

        # Create the webdriver object and pass the arguments
        chrome_options = webdriver.ChromeOptions()
        chrome_options.add_argument("--headless")
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
        chrome_options.add_experimental_option(
            "prefs", {"profile.managed_default_content_settings.images": 2}
        )
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
    def __init__(self, url, emc):
        super().__init__(url, emc)
        self.driver = self.init_webdriver()

    def parse(self):
        data = self.fetch()
        timenow_value = (
            timenow()
        )  # Call once and reuse, assuming it returns a static time for the run.

        for key, val in data.items():
            if not val:
                data[key] = pd.DataFrame()  # Direct assignment is fine here.
                continue  # Skip the rest of the loop if val is empty.

            # Processing per_county data.
            if key == "per_county":
                flattened_boundaries = [
                    boundary for item in val for boundary in item["boundaries"]
                ]
                per_loc_df = pd.DataFrame(flattened_boundaries)
                per_loc_df = per_loc_df[
                    (per_loc_df["customersAffected"] != 0)
                    | (per_loc_df["customersOutNow"] != 0)
                ]
                per_loc_df["timestamp"] = timenow_value
                per_loc_df["EMC"] = self.emc
                data[key] = per_loc_df

            # Processing per_outage data.
            elif key == "per_outage":
                per_outage_df = pd.DataFrame(val)
                zips = [
                    self.extract_zipcode(point["lat"], point["lng"])
                    for point in per_outage_df["outagePoint"]
                ]
                per_outage_df["zip"] = zips
                per_outage_df["timestamp"] = timenow_value
                per_outage_df["EMC"] = self.emc
                data[key] = per_outage_df

        return data

    def fetch(self):
        print(f"fetching {self.emc} outages from {self.url}")
        raw_data = {}

        with urlopen(self.url + "data/boundaries.json") as response:
            raw_data["per_county"] = json.loads(response.read())

        with urlopen(self.url + "data/outages.json") as response:
            raw_data["per_outage"] = json.loads(response.read())

        return raw_data


class Scraper2(BaseScraper):
    def __init__(self, url, emc):
        super().__init__(url, emc)

    def parse(self):
        data = self.fetch()
        for key, val in data.items():
            if val:
                per_outage_df = pd.DataFrame(val["Outages"])
                per_outage_df["timestamp"] = timenow()
                # TODO: sometimes error? mapping later
                # zips = [self.extract_zipcode(x['OutageLocation']['X'], x['OutageLocation']['Y']) for x in val['Outages']]
                # zips = [self.extract_zipcode(x['OutageLocation']['Y'],x['OutageLocation']['X']) for x in val['Outages']]
                # per_outage_df['zip'] = zips
                per_outage_df["EMC"] = self.emc
                data.update({key: per_outage_df})
            else:
                data.update({key: pd.DataFrame()})

        return data

    def fetch(self):
        print(f"fetching {self.emc} outages from {self.url}")
        raw_data = {}

        body, response = make_request(self.url + "api/weboutageviewer/get_live_data")
        raw_data["per_outage"] = json.loads(body.decode("utf8"))

        return raw_data


class Scraper3(BaseScraper):
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


class Scraper4(BaseScraper):
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
        print(f"fetching {self.emc} outages from {self.url}")
        # get javascript rendered source page
        self.driver.get(self.url)
        page_source = self.get_page_source(
            find_type="css", findkeyword="a.row.report-link.hyperlink-primary"
        )
        if not page_source:
            print("Failed to load the page source.")
            return {}
        # parse reports link
        soup = BeautifulSoup(page_source, "lxml")
        containers = soup.find_all(class_="row report-link hyperlink-primary")

        links = {}
        links = {container.text: container.get("href") for container in containers}

        # get json reports
        raw_data = {}
        for k, v in links.items():
            if "kubra" in self.url:
                self.url = "https://kubra.io/"
            self.driver.get(self.url + v[1:])
            time.sleep(5)
            requests = self.driver.requests

            for r in requests:
                if "kubra.io/data" in r.url:
                    print(f"scraping data from {r.url}")
                    response = sw_decode(
                        r.response.body,
                        r.response.headers.get("Content-Encoding", "identity"),
                    )
                    data = response.decode("utf8", "ignore")
                    if any([x in data for x in ["zip", "Zip", "ZIP", "City"]]):
                        raw_data["per_zipcode"] = json.loads(data)["file_data"]
                        print(f"got zip code data")
                    elif "county" in data or "County" in data:
                        raw_data["per_county"] = json.loads(data)["file_data"]
                        print(f"got county data")
                    elif "city" in data or "City" in data:
                        raw_data["per_city"] = json.loads(data)["file_data"]
                        print(f"got city data")

        return raw_data


class Scraper5(BaseScraper):
    def __init__(self, url, emc):
        super().__init__(url, emc)

    def parse(self):
        data = self.fetch()
        for key, val in data.items():
            if key == "per_outage" and val:
                df = pd.DataFrame(val)
                df["timestamp"] = timenow()
                df[["startTime", "lastUpdatedTime"]] = df[
                    ["startTime", "lastUpdatedTime"]
                ].apply(pd.to_datetime, unit="ms")
                df["EMC"] = self.emc
                df["zip_code"] = df.apply(
                    lambda row: self.extract_zipcode(row["latitude"], row["longitude"]),
                    axis=1,
                )
                data.update({key: df})
            else:
                if val == []:
                    data.update({key: pd.DataFrame()})
                print(
                    f"no outage of {self.emc} update found at",
                    datetime.strftime(datetime.now(), "%m-%d-%Y %H:%M:%S"),
                )
        return data

    def fetch(self):
        print(f"fetching {self.emc} outages from {self.url}")
        body, response = make_request(self.url)
        raw_data = {"per_outage": json.loads(body)}
        return raw_data


class Scraper6(BaseScraper):
    def __init__(self, url, emc):
        super().__init__(url, emc)

    def parse(self):
        raw_data = self.fetch()
        data = {}

        for key, val in raw_data.items():
            for r in val["reportData"]["reports"]:
                if r["id"] == "County":
                    per_county_df = pd.DataFrame(r["polygons"])
                    per_county_df["EMC"] = self.emc
                    per_county_df["timestamp"] = timenow()
                    per_county_df = per_county_df[per_county_df["affected"] > 0]
                    data.update({"per_county": per_county_df})
                elif r["id"] == "Zip":
                    per_zipcode_df = pd.DataFrame(r["polygons"])
                    per_zipcode_df["EMC"] = self.emc
                    per_zipcode_df["timestamp"] = timenow()
                    per_zipcode_df = per_zipcode_df[per_zipcode_df["affected"] > 0]
                    data.update({"per_zipcode": per_zipcode_df})

            per_outage_df = pd.DataFrame(val["outageData"]["outages"])
            per_outage_df["EMC"] = self.emc
            per_outage_df["timestamp"] = timenow()
            data.update({"per_outage": per_outage_df})

        return data


class Scraper7(BaseScraper):
    def __init__(self, url, emc):
        super().__init__(url, emc)
        self.driver = self.init_webdriver()

    def parse(self):
        data = self.fetch()

        for key, val in data.items():
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
            else:
                print(
                    f"no '{key}' outage of {self.emc} update found at",
                    datetime.strftime(datetime.now(), "%m-%d-%Y %H:%M:%S"),
                )

        return data

    def fetch(self):
        print(f"fetching {self.emc} outages from {self.url}")
        # Send a request to the website and let it load
        self.driver.get(self.url)
        time.sleep(5)
        try:
            self.wait_for_request(lambda request: "ShellOut" in request.url)
        except TimeoutError:
            print("The specific request was not made within the timeout period.")
            return {}

        raw_data = {}

        for request in self.driver.requests:
            if "ShellOut" in request.url:
                print(request.url)

                try:
                    response = sw_decode(
                        request.response.body,
                        request.response.headers.get("Content-Encoding", "identity"),
                    )
                except:
                    print("Error url:", request.url)
                data = response.decode("utf8")
                if "isHighTraffic" in data:
                    raw_data["per_outage"] = json.loads(data)
        return raw_data


class Scraper8(BaseScraper):
    def __init__(self, url, emc):
        super().__init__(url, emc)


class Scraper9(BaseScraper):
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
        if (
            self.emc != "Karnes Electric Coop, Inc."
            and self.emc != "BrightRidge"
            and self.emc != "San Patricio Electric Coop, Inc."
        ):
            WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located((By.ID, "OMS.Customers Summary"))
            )
            button = self.driver.find_elements(
                "xpath", '//*[@id="OMS.Customers Summary"]'
            )
            if button:
                label = WebDriverWait(self.driver, 10).until(
                    EC.element_to_be_clickable((By.ID, "OMS.Customers Summary"))
                )
                self.driver.execute_script("arguments[0].scrollIntoView();", label)
                label.click()
        WebDriverWait(self.driver, 10).until(
            EC.presence_of_element_located((By.CLASS_NAME, "gwt-ListBox"))
        )
        page_source = {}
        select_elements = self.driver.find_elements(By.CLASS_NAME, "gwt-ListBox")
        time.sleep(1)
        menu = Select(select_elements[0])
        for idx, option in enumerate(menu.options):
            level = option.text
            page_source.update({f"per_{level}": self.driver.page_source})
        return page_source


class Scraper10(BaseScraper):
    def __init__(self, url, emc=""):
        super().__init__(url, emc)
        self.driver = self.init_webdriver()

    def parse(self):
        data = self.fetch()
        df = pd.DataFrame(data["per_county"])

        df.rename(columns={"attributes": "data"}, inplace=True)
        df = pd.concat([df.drop(["data"], axis=1), df["data"].apply(pd.Series)], axis=1)
        df.columns = df.columns.str.lower()
        df.rename(columns={"cont_sum": "Members Affected"}, inplace=True)
        print(df)
        df["timestamp"] = timenow()
        df["EMC"] = self.emc
        df = df[df["Members Affected"] != "0"]
        data = {"per_county": df}
        return data

    def fetch(self):
        self.driver.get(self.url)
        time.sleep(5)
        for r in self.driver.requests:
            if "maps.ssemc.com" in r.url and "index.html" in r.url:
                self.url = r.url
                print("Redirect to", self.url)
                break

        self.driver.get(self.url)
        time.sleep(30)  # super slow site...
        for r in self.driver.requests:
            if "County" in r.url or "county" in r.url:
                self.url = r.url
                print("Fetch data from", self.url)
                break

        raw_data = {}
        try:
            raw_data["per_county"] = requests.get(self.url).json()["features"]
        except Exception as e:
            print(e.text)
            print("No data!")

        return raw_data


class Scraper11(BaseScraper):
    def __init__(self, url, emc):
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

        if self.emc != "Walton EMC":
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
        else:
            # def make_request(url, headers=None, data=None, method="GET"):
            url = "https://www.outageentry.com/Outages/ajax/ajaxShellOut.php"

            data = {"action": "get", "client": "walton", "target": "cfa_county_data"}

            raw_data["per_county"] = json.loads(
                make_request(url, None, data, "POST")[0]
            )

            data["target"] = "cfa_substation_data"

            raw_data["per_substation"] = json.loads(
                make_request(url, None, data, "POST")[0]
            )

            data["target"] = "cfa_device_markers"

            raw_data["per_outage"] = json.loads(
                make_request(url, None, data, "POST")[0]
            )
        return raw_data


class GAScraper:
    def __new__(cls, layout_id, url, emc):
        if layout_id == 1:
            obj = super().__new__(Scraper1)
        elif layout_id == 2:
            obj = super().__new__(Scraper2)
        elif layout_id == 3:
            obj = super().__new__(Scraper3)
        elif layout_id == 4:
            obj = super().__new__(Scraper4)
        elif layout_id == 5:
            obj = super().__new__(Scraper5)
        elif layout_id == 6:
            obj = super().__new__(Scraper6)
        elif layout_id == 7:
            obj = super().__new__(Scraper7)
        elif layout_id == 8:
            obj = super().__new__(Scraper8)
        elif layout_id == 9:
            obj = super().__new__(Scraper9)
        elif layout_id == 10:
            obj = super().__new__(Scraper10)
        elif layout_id == 11:
            obj = super().__new__(Scraper11)
        else:
            raise "Invalid layout ID: Enter layout ID range from 1 to 11"

        obj.__init__(url, emc)
        return obj
