import logging
import json
import pandas as pd
import geopy
import xmltodict
import io
import os
import boto3
import time

from urllib.error import HTTPError, URLError
from urllib.request import urlopen, Request
from datetime import datetime
from seleniumwire import webdriver
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities
from seleniumwire.utils import decode as sw_decode


# TODO: update for security
import ssl

# TODO: update ssl security
ssl._create_default_https_context = ssl._create_unverified_context


def is_aws_env():
    return os.environ.get('AWS_LAMBDA_FUNCTION_NAME') or os.environ.get('AWS_EXECUTION_ENV')


def save(df, bucket_name=None, file_path=None):
    if is_aws_env():
        s3_client = boto3.client("s3")
        with io.StringIO() as csv_buffer:
            df.to_csv(csv_buffer, index=False)

            response = s3_client.put_object(
                Bucket=bucket_name, Key=file_path, Body=csv_buffer.getvalue()
            )

            status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")

            if status == 200:
                print(f"Successful S3 put_object response. Status - {status}")
            else:
                print(f"Unsuccessful S3 put_object response. Status - {status}")
    else:
        local_path = f"{os.getcwd()}/../{bucket_name}/{file_path}"
        df.to_csv(local_path, index=False)
        print(f"outages data saved to {file_path}")


def make_request(url, headers={'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 13_2) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36'}):
    # TODO: refactor all 'urlopen'
    request = Request(url, headers=headers or {})
    try:
        with urlopen(request, timeout=10) as response:
            print(response.status)
            return response.read(), response
    except HTTPError as error:
        print(error.status, error.reason)
    except URLError as error:
        print(error.reason)
    except TimeoutError:
        print("Request timed out")


def extract_zipcode(lat, lon):
    geo_locator = geopy.Nominatim(user_agent='1234')
    addr = geo_locator.reverse((lat, lon))
    if addr:
        return addr.raw['address'].get('postcode', 'unknown')
    else:
        return 'unknown'


def timenow():
    return datetime.strftime(datetime.now(), "%m-%d-%Y %H:%M:%S")


class BaseScraper:
    def __init__(self, url, emc):
        self.url = url
        self.emc = emc
        self._tmp_folder = '/tmp/img-scrpr-chrm/'

    def fetch(self):
        pass

    def parse(self):
        pass

    def init_webdriver(self):
        # TODO: make sure chromedriver path
        chrome_driver_path = '/opt/chromedriver' if is_aws_env() else 'chromedriver'

        desired_capabilities = DesiredCapabilities.CHROME
        desired_capabilities["goog:loggingPrefs"] = {"performance": "ALL"}

        # Create the webdriver object and pass the arguments
        chrome_options = webdriver.ChromeOptions()
        chrome_options.add_argument('--headless')
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
            'user-agent=Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/61.0.3163.100 Safari/537.36')
        chrome_options.headless = True
        selenium_options = {
            'request_storage_base_dir': '/tmp',  # Use /tmp to store captured data
            'exclude_hosts': ''
        }
        # TODO: check binary locations
        chrome_options.binary_location = '/opt/chrome/chrome'
        driver = webdriver.Chrome(executable_path=chrome_driver_path,
                                  chrome_options=chrome_options,
                                  seleniumwire_options=selenium_options)
        return driver

    # def __get_default_chrome_options(self):
    #     chrome_options = webdriver.ChromeOptions()
    #
    #     lambda_options = [
    #         '--autoplay-policy=user-gesture-required',
    #         '--disable-background-networking',
    #         '--disable-background-timer-throttling',
    #         '--disable-backgrounding-occluded-windows',
    #         '--disable-breakpad',
    #         '--disable-client-side-phishing-detection',
    #         '--disable-component-update',
    #         '--disable-default-apps',
    #         '--disable-dev-shm-usage',
    #         '--disable-domain-reliability',
    #         '--disable-extensions',
    #         '--disable-features=AudioServiceOutOfProcess',
    #         '--disable-hang-monitor',
    #         '--disable-ipc-flooding-protection',
    #         '--disable-notifications',
    #         '--disable-offer-store-unmasked-wallet-cards',
    #         '--disable-popup-blocking',
    #         '--disable-print-preview',
    #         '--disable-prompt-on-repost',
    #         '--disable-renderer-backgrounding',
    #         '--disable-setuid-sandbox',
    #         '--disable-speech-api',
    #         '--disable-sync',
    #         '--disable-gpu',
    #         '--disk-cache-size=33554432',
    #         '--hide-scrollbars',
    #         '--ignore-gpu-blacklist',
    #         '--ignore-certificate-errors',
    #         '--metrics-recording-only',
    #         '--mute-audio',
    #         '--no-default-browser-check',
    #         '--no-first-run',
    #         '--no-pings',
    #         '--no-sandbox',
    #         '--no-zygote',
    #         '--password-store=basic',
    #         '--use-gl=swiftshader',
    #         '--use-mock-keychain',
    #         '--single-process',
    #         '--headless',
    #         'user-agent=Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/61.0.3163.100 Safari/537.36',
    #         '--v=99',
    #         '--window-size=1280x1696']
    #
    #     chrome_options.add_argument('--disable-gpu')
    #     for argument in lambda_options:
    #         chrome_options.add_argument(argument)
    #     chrome_options.add_argument('--user-data-dir={}'.format(self._tmp_folder + '/user-data'))
    #     chrome_options.add_argument('--data-path={}'.format(self._tmp_folder + '/data-path'))
    #     chrome_options.add_argument('--homedir={}'.format(self._tmp_folder))
    #     chrome_options.add_argument('--disk-cache-dir={}'.format(self._tmp_folder + '/cache-dir'))
    #
    #     chrome_options.binary_location = os.getcwd() + "/bin/headless-chromium"
    #
    #     return chrome_options


class Scraper1(BaseScraper):
    def __init__(self, url, emc):
        super().__init__(url, emc)

    def parse(self):
        data = self.fetch()

        for key, val in data.items():
            if key == 'per_county' and data[key]:
                per_loc_df = pd.DataFrame(val[0]['boundaries'])
                per_loc_df['timestamp'] = timenow()
                per_loc_df['EMC'] = self.emc
                data.update({key: per_loc_df})
            elif key == 'per_outage' and data[key]:
                per_outage_df = pd.DataFrame(val)
                per_outage_df['timestamp'] = timenow()
                zips = [extract_zipcode(x['outagePoint']['lat'], x['outagePoint']['lng']) for x in val]
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
        response_df = pd.DataFrame(data)[
            ['CustomersOutNow', 'CustomersRestored', 'CustomersServed', 'OutageLocation_lon',
             'OutageLocation_lat', 'MapLocation', 'District', 'OutageName']]
        response_df.columns = ['customersAffected', 'customersRestored', 'customersServed', 'OutageLocation_lon',
                               'OutageLocation_lat', 'MapLocation', 'District', 'name']
        response_df['zip_code'] = response_df.apply(
            lambda row: extract_zipcode(row['OutageLocation_lat'], row['OutageLocation_lon']), axis=1)

        return response_df[['name', 'zip_code', 'customersAffected', 'customersRestored', 'customersServed']]

    def fetch(self):
        response = urlopen(self.url)
        data = response.read()
        response.close()

        data = json.loads(data.decode())['Outages']
        for outage in data:
            outage['OutageLocation_lon'] = outage['OutageLocation']['X']
            outage['OutageLocation_lat'] = outage['OutageLocation']['Y']

        return data


class Scraper3(BaseScraper):
    def __init__(self, url, emc):
        super().__init__(url, emc)

    def parse(self):
        data = self.fetch()

        for key, val in data.items():
            if key == 'per_county' and val:
                per_loc_df = pd.DataFrame(val)
                per_loc_df['timestamp'] = timenow()
                per_loc_df['EMC'] = self.emc
                per_loc_df.drop(columns=['Shape'], inplace=True)
                data.update({key: per_loc_df})
            elif key == 'per_outage' and val:
                per_outage_df = pd.DataFrame(val['MobileOutage'])
                per_outage_df['timestamp'] = timenow()
                zips = [extract_zipcode(x['X'], x['Y']) for x in [val['MobileOutage']]]
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

    def parse(self):
        pass

    def fetch(self):
        # get county/zipcode report link from html

        # get response from the requests

        pass


class Scraper5(BaseScraper):
    def __init__(self, url, emc):
        super().__init__(url, emc)

    def parse(self):
        data = self.fetch()
        for key, val in data.items():
            if key == 'per_outage' and val:
                df = pd.DataFrame(val)
                # [['county', 'numPeople', 'latitude', 'longitude', 'title']]
                # df.columns = ['name', 'customersAffected', 'latitude', 'longitude', 'type']
                df['timestamp'] = timenow()
                df['EMC'] = self.emc
                df['zip_code'] = df.apply(lambda row: extract_zipcode(row['latitude'], row['longitude']), axis=1)
                data.update({key: df})

        return data

    def fetch(self):
        print(f"fetching {self.emc} outages from {self.url}")
        body, response = make_request(self.url)
        raw_data = {}
        raw_data['per_outage'] = json.loads(body)
        return raw_data


class Scraper6(BaseScraper):
    def __init__(self, url, emc):
        super().__init__(url, emc)

    def parse(self):
        data = self.fetch()

        rows = []
        for row in data['data']['reports']['report']['dataset']['t']:
            position = row['e'][5].split(',')
            rows.append([row['e'][0], row['e'][1], row['e'][2], float(position[0]), float(position[1])])

        df = pd.DataFrame(rows, columns=['name', 'customersServed', 'customersAffected', 'latitutde', 'longitude'])
        df['zip_code'] = df.apply(lambda row: extract_zipcode(row['latitutde'], row['longitude']), axis=1)
        return df[['name', 'zip_code', 'customersAffected', 'customersServed']]

    def fetch(self):
        datas = []
        for request in self.driver.requests:
            if "data?_" in request.url:
                data = sw_decode(request.response.body, request.response.headers.get('Content-Encoding', 'identity'))
                data = data.decode("utf8")
                datas.append(data)

        data_response = ""
        for data in datas:
            if "County" in data:
                data_response = data
                break

        data_response = xmltodict.parse(data_response)
        return data_response


class Scraper7(BaseScraper):

    def __init__(self, url):
        super().__init__()
        self.url = url

    def fetch(self):
        datas = []
        for request in self.driver.requests:
            if "ShellOut" in request.url:
                data = sw_decode(request.response.body, request.response.headers.get('Content-Encoding', 'identity'))
                data = data.decode("utf8")
                datas.append(data)

        data_response = datas[-1]
        return json.loads(data_response)


class Scraper8(BaseScraper):
    def __init__(self, url, emc):
        super().__init__(url, emc)


class Scraper9(BaseScraper):
    def __init__(self, url, emc):
        super().__init__(url, emc)

    def parse(self):
        pass

    def fetch(self):
        pass


class Scraper10(BaseScraper):
    def __init__(self, url, emc=''):
        super().__init__(url, emc)

    def parse(self):
        response = self.fetch()
        attributes = [x['attributes'] for x in response['features']]
        response_df = pd.DataFrame(attributes)[['NAME', 'o_ConT', 'o_TotalCustomers']]
        response_df.columns = ['name', 'customersAffected', 'customersServed']
        return response_df

    def fetch(self):
        response = urlopen(self.url)
        data_json = json.loads(response.read())
        return data_json


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
                data.update({key: per_substation_df})
            elif key == 'per_county':
                per_county_df = pd.DataFrame(val['rows'])
                per_county_df['timestamp'] = timenow()
                per_county_df['EMC'] = self.emc
                data.update({key: per_county_df})
            elif key == 'per_outage':
                per_outage_df = pd.DataFrame(data['per_outage']['0']['markers'])
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


class Scraper:
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
