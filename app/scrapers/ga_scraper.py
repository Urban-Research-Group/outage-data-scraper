import logging
import json
import pandas as pd
import geopy
import xmltodict
import time

from bs4 import BeautifulSoup
from datetime import datetime
from urllib.request import urlopen, Request
from seleniumwire.utils import decode as sw_decode
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities
from seleniumwire import webdriver
from .util import is_aws_env, make_request, timenow

# TODO: update for security
import ssl
ssl._create_default_https_context = ssl._create_unverified_context


class BaseScraper:
    def __init__(self, url, emc):
        self.url = url
        self.emc = emc
        # self.driver = self.init_webdriver()
        self.geo_locator = geopy.Nominatim(user_agent='1234')

    def fetch(self):
        pass

    def parse(self):
        pass

    def get_page_source(self, url=None, timeout=5):
        url= url if url else self.url
        self.driver.get(url)
        # let the page load
        time.sleep(timeout)
        page_source = self.driver.page_source

        return page_source

    def extract_zipcode(self, lat, lon):
        addr = self.geo_locator.reverse((lat, lon))
        if addr:
            return addr.raw['address'].get('postcode', 'unknown')
        else:
            return 'unknown'

    def init_webdriver(self):
        chrome_driver_path = '/opt/chromedriver' if is_aws_env() else 'chromedriver'

        desired_capabilities = DesiredCapabilities.CHROME.copy()
        desired_capabilities["goog:loggingPrefs"] = {"performance": "ALL"}
        desired_capabilities['acceptInsecureCerts'] = True

        # Create the webdriver object and pass the arguments
        chrome_options = webdriver.ChromeOptions()
        # chrome_options.add_argument('--headless')
        chrome_options.add_argument('--headless=new')
        chrome_options.add_argument('--disable-dev-shm-usage')
        chrome_options.add_argument('--disable-extensions')
        chrome_options.add_argument('--no-sandbox')
        chrome_options.add_argument('--no-cache')
        chrome_options.add_argument('--disable-gpu')
        chrome_options.add_argument('--window-size=1024x768')
        chrome_options.add_argument('--user-data-dir=/tmp/user-data')
        chrome_options.add_argument('--hide-scrollbars')
        chrome_options.add_argument('--enable-logging')
        chrome_options.add_argument('--log-level=0')
        chrome_options.add_argument('--v=99')
        chrome_options.add_argument('--single-process')
        chrome_options.add_argument('--data-path=/tmp/data-path')
        chrome_options.add_argument('--ignore-certificate-errors')
        chrome_options.add_argument('--homedir=/tmp')
        chrome_options.add_argument('--disk-cache-dir=/tmp/cache-dir')
        chrome_options.add_argument(
            'user-agent=Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/61.0.3163.100 '
            'Safari/537.36')
        chrome_options.headless = True
        selenium_options = {
            'request_storage_base_dir': '/tmp',  # Use /tmp to store captured data
            'exclude_hosts': ''
        }
        if is_aws_env():
            chrome_options.binary_location = '/opt/chrome/chrome'

        driver = webdriver.Chrome(executable_path=chrome_driver_path,
                                  chrome_options=chrome_options,
                                  seleniumwire_options=selenium_options,
                                  desired_capabilities=desired_capabilities)
        return driver


class Scraper1(BaseScraper):
    def __init__(self, url, emc):
        super().__init__(url, emc)

    def parse(self):
        data = self.fetch()

        for key, val in data.items():
            if key == 'per_county' and data[key]:
                per_loc_df = pd.DataFrame(val[0]['boundaries'])
                per_loc_df = per_loc_df[(per_loc_df['customersAffected'] != 0) | (per_loc_df['customersOutNow'] != 0)]
                per_loc_df['timestamp'] = timenow()
                per_loc_df['EMC'] = self.emc
                data.update({key: per_loc_df})
            elif key == 'per_outage' and data[key]:
                per_outage_df = pd.DataFrame(val)
                per_outage_df['timestamp'] = timenow()
                zips = [self.extract_zipcode(x['outagePoint']['lat'], x['outagePoint']['lng']) for x in val]
                per_outage_df['zip'] = zips
                per_outage_df['EMC'] = self.emc
                data.update({key: per_outage_df})
            else:
                print(f"no outage of {self.emc} update found at",
                      datetime.strftime(datetime.now(), "%m-%d-%Y %H:%M:%S"))

        return data

    def fetch(self):
        print(f"fetching {self.emc} outages from {self.url}")
        raw_data = {}
        with urlopen(self.url + 'data/boundaries.json') as response:
            raw_data['per_county'] = json.loads(response.read())

        with urlopen(self.url + 'data/outages.json') as response:
            raw_data['per_outage'] = json.loads(response.read())

        return raw_data


class Scraper2(BaseScraper):
    def __init__(self, url, emc):
        super().__init__(url, emc)

    def parse(self):
        data = self.fetch()
        for key, val in data.items():
            if val:
                per_outage_df = pd.DataFrame(val['Outages'])
                per_outage_df['timestamp'] = timenow()
                zips = [self.extract_zipcode(x['OutageLocation']['X'], x['OutageLocation']['Y']) for x in val['Outages']]
                per_outage_df['zip'] = zips
                per_outage_df['EMC'] = self.emc
                data.update({key: per_outage_df})
        return data

    def fetch(self):
        print(f"fetching {self.emc} outages from {self.url}")
        raw_data = {}

        body, response = make_request(self.url+'api/weboutageviewer/get_live_data')
        raw_data['per_outage'] = json.loads(body.decode('utf8'))

        return raw_data


class Scraper3(BaseScraper):
    def __init__(self, url, emc):
        super().__init__(url, emc)

    def parse(self):
        data = self.fetch()

        for key, val in data.items():
            if key == 'per_county' and val:
                per_loc_df = pd.DataFrame(val)
                per_loc_df = per_loc_df[per_loc_df['CustomersAffected'] != '0']
                per_loc_df['timestamp'] = timenow()
                per_loc_df['EMC'] = self.emc
                per_loc_df.drop(columns=['Shape'], inplace=True)
                data.update({key: per_loc_df})
            elif key == 'per_outage' and val:
                per_outage_df = pd.DataFrame(val['MobileOutage'])
                per_outage_df['timestamp'] = timenow()
                zips = [self.extract_zipcode(x['X'], x['Y'], self.geo_locator) for x in [val['MobileOutage']]]
                per_outage_df['zip'] = zips
                per_outage_df['EMC'] = self.emc
                data.update({key: per_outage_df})
            else:
                print(f"no outage of {self.emc} update found at",
                      datetime.strftime(datetime.now(), "%m-%d-%Y %H:%M:%S"))

        return data

    def fetch(self):
        print(f"fetching {self.emc} outages from {self.url}")
        raw_data = {}

        # TODO: simplify
        body, response = make_request(self.url + 'MobileMap/OMSMobileService.asmx/GetAllCounties')
        temp = xmltodict.parse(body.decode("utf8"))
        raw_data['per_county'] = temp['ArrayOfMobileCounty']['MobileCounty']

        body, response = make_request(self.url + 'MobileMap/OMSMobileService.asmx/GetAllOutages')
        temp = xmltodict.parse(body.decode("utf8"))
        raw_data['per_outage'] = temp['MobileOutageInfo']['Outages']

        return raw_data


class Scraper4(BaseScraper):
    def __init__(self, url, emc):
        super().__init__(url, emc)
        self.driver = self.init_webdriver()

    def parse(self):
        data = self.fetch()

        for key, val in data.items():
            if val:
                df = pd.DataFrame(val['areas'])
                df[['cust_a', 'percent_cust_a']] = df[['cust_a', 'percent_cust_a']].applymap(lambda x : x['val'])
                df = df[(df['cust_a'] != 0) | (df['n_out'] != 0)]
                df['timestamp'] = timenow()
                df['EMC'] = self.emc
                df.drop(columns=['gotoMap'], inplace=True)
                data.update({key: df})
            else:
                print(f"no outage of {self.emc} update found at",
                      datetime.strftime(datetime.now(), "%m-%d-%Y %H:%M:%S"))
        return data

    def fetch(self):
        print(f"fetching {self.emc} outages from {self.url}")
        # get javascript rendered source page
        self.driver.get(self.url)
        # Sleeps for 5 seconds
        time.sleep(5)
        page_source = self.driver.page_source

        # parse reports link
        soup = BeautifulSoup(page_source, "html.parser")
        containers = soup.find_all(class_="row report-link hyperlink-primary")
        links = {}
        for c in containers:
            links.update({c.get("data-metrics-label"): c.get("href")})

        # get json reports
        raw_data = {}
        for k, v in links.items():
            self.driver.get(self.url+v[1:])
            time.sleep(5)
            requests = self.driver.requests
            for r in requests:
                if v in r.url:
                    # print(r)
                    response = sw_decode(r.response.body,
                                         r.response.headers.get('Content-Encoding', 'identity'))
                    data = response.decode("utf8", 'ignore')
                    if any([x in data for x in ['zip', 'Zip', 'City']]):
                        raw_data['per_zipcode'] = json.loads(data)['file_data']
                    elif 'county' in data:
                        raw_data['per_county'] = json.loads(data)['file_data']
        return raw_data


class Scraper5(BaseScraper):
    def __init__(self, url, emc):
        super().__init__(url, emc)

    def parse(self):
        data = self.fetch()
        for key, val in data.items():
            if key == 'per_outage' and val:
                df = pd.DataFrame(val)
                df['timestamp'] = timenow()
                df[['startTime', 'lastUpdatedTime', 'etrTime']] = df[['startTime', 'lastUpdatedTime', 'etrTime']].apply(
                    pd.to_datetime, unit='ms')
                df['EMC'] = self.emc
                df['zip_code'] = df.apply(lambda row: self.extract_zipcode(row['latitude'], row['longitude']), axis=1)
                data.update({key: df})
            else:
                print(f"no outage of {self.emc} update found at",
                      datetime.strftime(datetime.now(), "%m-%d-%Y %H:%M:%S"))

        return data

    def fetch(self):
        print(f"fetching {self.emc} outages from {self.url}")
        body, response = make_request(self.url)
        raw_data = {'per_outage': json.loads(body)}
        return raw_data


class Scraper6(BaseScraper):
    def __init__(self, url, emc):
        super().__init__(url, emc)
        self.driver = self.init_webdriver()

    def parse(self):
        data = self.fetch()

        # rows = []
        # for row in data['data']['reports']['report']['dataset']['t']:
        #     position = row['e'][5].split(',')
        #     rows.append([row['e'][0], row['e'][1], row['e'][2], float(position[0]), float(position[1])])
        #
        # df = pd.DataFrame(rows, columns=['name', 'customersServed', 'customersAffected', 'latitutde', 'longitude'])
        # df['zip_code'] = df.apply(lambda row: extract_zipcode(row['latitutde'], row['longitude']), axis=1)
        # return df[['name', 'zip_code', 'customersAffected', 'customersServed']]
        return data

    def fetch(self):
        print(f"fetching {self.emc} outages from {self.url}")
        # TODO: configure seleniumwire options to log requets
        # Send a request to the website and let it load
        self.driver.get(self.url)

        # Sleeps for 5 seconds
        time.sleep(10)

        raw_datas = {}

        for request in self.driver.requests:
            if "outages/data" in request.url:
                data = sw_decode(request.response.body, request.response.headers.get('Content-Encoding', 'identity'))
                data = data.decode("utf8")
                if "County" in data:
                    raw_datas.update({'per_county': xmltodict.parse(data)})

        return raw_datas


class Scraper7(BaseScraper):

    def __init__(self, url, emc):
        super().__init__(url, emc)
        self.driver = self.init_webdriver()

    def parse(self):
        data = self.fetch()

        for key, val in data.items():
            if val:
                isHighTraffic = val['isHighTraffic']
                updateTime = val['timestamp']
                per_outage_df = pd.DataFrame()
                for k, v in val.items():
                    if isinstance(v, dict):
                        if v['markers']:
                            df = pd.DataFrame(v['markers'])
                            df['service_index_name'] = v['service_index_name']
                            df['outages'] = v['outages']
                            df['NumConsumers'] = v['stats']['NumConsumers']
                            df['zip_code'] = df.apply(lambda row: self.extract_zipcode(row['lat'], row['lon']), axis=1)
                            per_outage_df = df

                per_outage_df['isHighTraffic'] = isHighTraffic
                per_outage_df['updateTime'] = updateTime
                per_outage_df['timestamp'] = timenow()
                per_outage_df['EMC'] = self.emc
                data.update({key: per_outage_df})
            else:
                print(f"no outage of {self.emc} update found at",
                      datetime.strftime(datetime.now(), "%m-%d-%Y %H:%M:%S"))

        return data

    def fetch(self):
        print(f"fetching {self.emc} outages from {self.url}")
        # Send a request to the website and let it load
        self.driver.get(self.url)

        # Sleeps for 5 seconds
        time.sleep(5)

        raw_data = {}
        for request in self.driver.requests:
            if "ShellOut" in request.url:
                response = sw_decode(request.response.body, request.response.headers.get('Content-Encoding', 'identity'))
                data = response.decode("utf8")
                if 'isHighTraffic' in data:
                    raw_data['per_outage'] = json.loads(data)

        return raw_data


class Scraper8(BaseScraper):
    def __init__(self, url, emc):
        super().__init__(url, emc)


class Scraper9(BaseScraper):
    def __init__(self, url, emc):
        super().__init__(url, emc)
        self.driver = self.init_webdriver()

    def parse(self):
        self.fetch()
        soup = BeautifulSoup(self.driver.page_source, "html.parser")
        tables = soup.find_all("table")

        # Find the table rows using their tag name
        rows = tables[1].find_all('tr')

        # Extract the table header row
        header_row = rows[0]

        # Extract the table data rows
        data_rows = rows[1:]

        # Extract the table header cells
        header_cells = header_row.find_all('th')
        header = [cell.get_text().strip() for cell in header_cells]

        # Extract the table data cells
        data = []
        for row in data_rows:
            cells = row.find_all('td')
            data.append([cell.get_text().strip() for cell in cells])

        # Print the table data as a list of dictionaries
        table_data = [dict(zip(header, row)) for row in data]
        df = pd.DataFrame(table_data)[:-1][['County', '# Out', '# Served', '% Out']]
        df['timestamp'] = timenow()
        df['EMC'] = self.emc
        df = df[df['# Out'] != '0']
        data = {'per_county': df}
        return data

    def fetch(self):
        print(f"fetching {self.emc} outages from {self.url}")
        # Send a request to the website and let it load
        self.driver.get(self.url)
        time.sleep(10)


class Scraper10(BaseScraper):
    def __init__(self, url, emc=''):
        super().__init__(url, emc)

    def parse(self):
        source_page = self.fetch()
        soup = BeautifulSoup(source_page, "html.parser")
        table = soup.find("table")
        # Find the table rows using their tag name
        rows = table.find_all('tr')

        # Extract the table header row
        header_row = rows[0]

        # Extract the table data rows
        data_rows = rows[1:]

        # Extract the table header cells
        header_cells = header_row.find_all('th')
        header = [cell.get_text().strip() for cell in header_cells]

        # Extract the table data cells
        data = []
        for row in data_rows:
            cells = row.find_all('td')
            data.append([cell.get_text().strip() for cell in cells])

        # Print the table data as a list of dictionaries
        table_data = [dict(zip(header, row)) for row in data]
        df = pd.DataFrame(table_data)[:-1]
        df['timestamp'] = timenow()
        df['EMC'] = self.emc
        df = df[df['Customers Affected'] != '0']
        data = {'per_county': df}
        return data

    def fetch(self):
        body, response = make_request(self.url)
        return body


class Scraper11(BaseScraper):
    def __init__(self, url, emc):
        super().__init__(url, emc)
        self.driver = self.init_webdriver()

    def parse(self):
        data = self.fetch()

        for key, val in data.items():
            if key == 'per_substation':
                per_substation_df = pd.DataFrame(val['rows']['subs'])
                per_substation_df['timestamp'] = timenow()
                per_substation_df['EMC'] = self.emc
                per_substation_df = per_substation_df[(per_substation_df.SubTotalConsumersOut != 0) |
                                                      (per_substation_df.SubTotalMetersAffectedByDeviceOutages != 0)]
                data.update({key: per_substation_df})
            elif key == 'per_county':
                per_county_df = pd.DataFrame(val['rows'])
                per_county_df['timestamp'] = timenow()
                per_county_df['EMC'] = self.emc
                per_county_df = per_county_df[per_county_df.out != 0]
                data.update({key: per_county_df})
            elif key == 'per_outage':
                if val:
                    isHighTraffic = val['isHighTraffic']
                    updateTime = val['timestamp']
                    per_outage_df = pd.DataFrame()
                    for k, v in val.items():
                        if isinstance(v, dict):
                            if v['markers']:
                                df = pd.DataFrame(v['markers'])
                                df['service_index_name'] = v['service_index_name']
                                df['outages'] = v['outages']
                                df['NumConsumers'] = v['stats']['NumConsumers']
                                df['zip_code'] = df.apply(lambda row: self.extract_zipcode(row['lat'], row['lon']), axis=1)
                                per_outage_df = df

                    per_outage_df['isHighTraffic'] = isHighTraffic
                    per_outage_df['updateTime'] = updateTime
                    per_outage_df['timestamp'] = timenow()
                    per_outage_df['EMC'] = self.emc
                    data.update({key: per_outage_df})

        return data

    def fetch(self):
        print(f"fetching {self.emc} outages from {self.url}")
        # Send a request to the website and let it load
        self.driver.get(self.url)

        # Sleeps for 5 seconds
        time.sleep(5)

        raw_data = {}
        for request in self.driver.requests:
            if "ShellOut" in request.url:
                response = sw_decode(request.response.body,
                                     request.response.headers.get('Content-Encoding', 'identity'))
                data = response.decode("utf8", 'ignore')
                if 'sub_outages' in data:
                    raw_data['per_substation'] = json.loads(data)
                elif 'cfa_county_data' in data:
                    raw_data['per_county'] = json.loads(data)
                elif 'isHighTraffic' in data:
                    raw_data['per_outage'] = json.loads(data)

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
