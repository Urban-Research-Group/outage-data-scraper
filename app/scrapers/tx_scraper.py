import logging
import json
import pandas as pd
import geopy
import xmltodict
import io
import os
import boto3
import time

from bs4 import BeautifulSoup
from datetime import datetime
from urllib.error import HTTPError, URLError
from urllib.request import urlopen, Request
from requests_html import HTMLSession
from seleniumwire import webdriver
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities
from seleniumwire.utils import decode as sw_decode
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC


# TODO: update for security
import ssl
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


def make_request(url, headers=None):
    # TODO: refactor all 'urlopen'
    if headers is None:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 13_2) AppleWebKit/537.36 (KHTML, like Gecko) '
                          'Chrome/109.0.0.0 Safari/537.36'}
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

    def fetch(self):
        pass

    def parse(self):
        pass

    def init_webdriver(self):
        chrome_driver_path = '/opt/chromedriver' if is_aws_env() else 'chromedriver'

        desired_capabilities = DesiredCapabilities.CHROME.copy()
        desired_capabilities["goog:loggingPrefs"] = {"performance": "ALL"}
        desired_capabilities['acceptInsecureCerts'] = True

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


class Scraper8(BaseScraper):
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





class TXScraper:
    def __new__(cls, layout_id, url, emc):
        if layout_id == 8:
            obj = super().__new__(Scraper8)
        else:
            raise "Invalid layout ID: Enter layout ID range from 1 to 11"

        obj.__init__(url, emc)
        return obj


if __name__ == "__main__":

    sc = TXScraper(layout_id=8,
                url='http://outage.bluebonnetelectric.coop:82/',
                emc='Blue Bonnet')
    print(sc.parse())