import logging
import json
import pandas as pd
import geopy
import time
import xmltodict
import io
import boto3

from urllib.request import urlopen
from datetime import datetime
from seleniumwire import webdriver
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities
from seleniumwire.utils import decode as sw_decode


class BaseScraper:
    def __init__(self, url, emc):
        self.driver = ''
        self.url = url
        self.emc = emc
        self.geo_locator = geopy.Nominatim(user_agent='1234')
        # self.init_webdriver()

    def fetch(self):
        pass

    def parse(self):
        pass

    def df_to_s3(self, df, bucket_name, file_path):
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

    def extract_zipcode(self, lat, lon):
        addr = self.geo_locator.reverse((lat, lon)).raw['address']
        return addr.get('postcode', 'unknown')

    # def init_webdriver(self):
    #     chrome_driver_path = 'chromedriver.exe'
    #
    #     desired_capabilities = DesiredCapabilities.CHROME
    #     desired_capabilities["goog:loggingPrefs"] = {"performance": "ALL"}
    #
    #     # Create the webdriver object and pass the arguments
    #     options = webdriver.ChromeOptions()
    #
    #     # Chrome will start in Headless mode
    #     options.add_argument('headless')
    #
    #     # Ignores any certificate errors if there is any
    #     options.add_argument("--ignore-certificate-errors")
    #
    #     # Startup the chrome webdriver with executable path and
    #     # pass the chrome options and desired capabilities as
    #     # parameters.
    #     self.driver = webdriver.Chrome(executable_path=chrome_driver_path,
    #                                    chrome_options=options,
    #                                    desired_capabilities=desired_capabilities)
    #
    #     # Send a request to the website and let it load
    #     self.driver.get(self.url)
    #
    #     # Sleeps for 5 seconds
    #     time.sleep(5)


class Scraper1(BaseScraper):
    def __init__(self, url, emc=''):
        super().__init__(url, emc)

    def parse(self):
        outages, boundaries = self.fetch()
        if boundaries:
            df = pd.DataFrame(boundaries[0]['boundaries'])
            df['timestamp'] = datetime.strftime(datetime.now(), "%m-%d-%Y %H:%M:%S")
            return df
        if outages:
            df = pd.DataFrame(outages)
            df['timestamp'] = datetime.strftime(datetime.now(), "%m-%d-%Y %H:%M:%S")
            zips = [self.extract_zipcode(x['outagePoint']['lat'], x['outagePoint']['lng']) for x in outages]
            df['zip'] = zips
            df['EMC'] = self.emc
            return df
        else:
            print("no outage update found at", datetime.strftime(datetime.now(), "%m-%d-%Y %H:%M:%S"))
            return pd.DataFrame()

    def fetch(self):
        boundaries = urlopen(self.url + 'data/boundaries.json')
        outages = urlopen(self.url + 'data/outages.json')
        outages_data = json.loads(outages.read())
        boundaries_data = json.loads(boundaries.read())

        return outages_data, boundaries_data


class Scraper2(BaseScraper):
    def __init__(self, url, emc=''):
        super().__init__(url, emc)

    def parse(self):
        data = self.fetch()
        response_df = pd.DataFrame(data)[
            ['CustomersOutNow', 'CustomersRestored', 'CustomersServed', 'OutageLocation_lon',
             'OutageLocation_lat', 'MapLocation', 'District', 'OutageName']]
        response_df.columns = ['customersAffected', 'customersRestored', 'customersServed', 'OutageLocation_lon',
                               'OutageLocation_lat', 'MapLocation', 'District', 'name']
        response_df['zip_code'] = response_df.apply(
            lambda row: self.extract_zipcode(row['OutageLocation_lat'], row['OutageLocation_lon']), axis=1)

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


class Scraper5(BaseScraper):
    def __init__(self, url, emc=''):
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
    logger = logging.getLogger("ScraperL3")

    def __init__(self, url, emc=''):
        super().__init__(url, emc)

    def parse(self):
        data = self.fetch()

        rows = []
        for row in data['data']['reports']['report']['dataset']['t']:
            position = row['e'][5].split(',')
            rows.append([row['e'][0], row['e'][1], row['e'][2], float(position[0]), float(position[1])])

        df = pd.DataFrame(rows, columns=['name', 'customersServed', 'customersAffected', 'latitutde', 'longitude'])
        df['zip_code'] = df.apply(lambda row: self.extract_zipcode(row['latitutde'], row['longitude']), axis=1)
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
    logger = logging.getLogger("ScraperL7")

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
    def __init__(self, url='', emc=''):
        super().__init__(url, emc)

    def parse(self):
        data = (self.fetch())
        # print(data)
        df = pd.DataFrame(data['rows']['subs'])
        return df

    def fetch(self):
        datas = []
        index = 0
        i = 0
        for request in self.driver.requests:
            if "ShellOut" in request.url:
                data = sw_decode(request.response.body, request.response.headers.get('Content-Encoding', 'identity'))
                data = data.decode("utf8")
                datas.append(data)

                if 'SubName' in data:
                    index = i

                i += 1

        print(index)
        data_response = datas[index]
        return json.loads(data_response)

