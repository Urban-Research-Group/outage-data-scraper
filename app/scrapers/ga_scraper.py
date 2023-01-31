import logging
import json
import pandas as pd
import geopy
import xmltodict
import io
import os
import boto3
import time

from urllib.request import urlopen
from datetime import datetime
from seleniumwire import webdriver
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities
from seleniumwire.utils import decode as sw_decode


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


def extract_zipcode(lat, lon):
    geo_locator = geopy.Nominatim(user_agent='1234')
    addr = geo_locator.reverse((lat, lon)).raw['address']
    return addr.get('postcode', 'unknown')


class BaseScraper:
    def __init__(self, url, emc):
        self.url = url
        self.emc = emc

    def fetch(self):
        pass

    def parse(self):
        pass

    def init_webdriver(self):
        # make sure chromedriver under home dir
        chrome_driver_path = 'chromedriver'

        desired_capabilities = DesiredCapabilities.CHROME
        desired_capabilities["goog:loggingPrefs"] = {"performance": "ALL"}

        # Create the webdriver object and pass the arguments
        options = webdriver.ChromeOptions()

        # Chrome will start in Headless mode
        options.add_argument('headless')

        # Ignores any certificate errors if there is any
        options.add_argument("--ignore-certificate-errors")

        # Startup the chrome webdriver with executable path and
        # pass the chrome options and desired capabilities as
        # parameters.
        return webdriver.Chrome(executable_path=chrome_driver_path,
                                chrome_options=options,
                                desired_capabilities=desired_capabilities)


class Scraper1(BaseScraper):
    def __init__(self, url, emc):
        super().__init__(url, emc)

    def parse(self):
        data = self.fetch()

        for key, val in data.items():
            if key == 'per_county' and data[key]:
                per_loc_df = pd.DataFrame(val[0]['boundaries'])
                per_loc_df['timestamp'] = datetime.strftime(datetime.now(), "%m-%d-%Y %H:%M:%S")
                data.update({key: per_loc_df})
            elif key == 'per_outage' and data[key]:
                per_outage_df = pd.DataFrame(val)
                per_outage_df['timestamp'] = datetime.strftime(datetime.now(), "%m-%d-%Y %H:%M:%S")
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


class Scraper4(BaseScraper):
    def __init__(self, url, emc):
        super().__init__(url, emc)


class Scraper5(BaseScraper):
    def __init__(self, url, emc):
        super().__init__(url, emc)

    def parse(self):
        response = self.fetch()
        response_df = pd.DataFrame(response)[['county', 'numPeople', 'latitude', 'longitude', 'title']]
        response_df.columns = ['name', 'customersAffected', 'latitude', 'longitude', 'type']

        response_df['zip_code'] = response_df.apply(lambda row: self.extract_zipcode(row['latitude'], row['longitude']),
                                                    axis=1)
        return response_df[['name', 'zip_code', 'customersAffected', 'latitude', 'longitude', 'type']]

    def fetch(self):
        response = urlopen(self.url)
        data_json = json.loads(response.read())
        return data_json


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
            # TODO: add timestamp, emc,
            if key == 'per_substation':
                data.update({key: pd.DataFrame(val['rows']['subs'])})
            elif key == 'per_county':
                data.update({key: pd.DataFrame(val['rows'])})
            elif key == 'per_outage':
                # TODO: save format?
                pass

        print(data)
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
                data = response.decode("utf8")
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
