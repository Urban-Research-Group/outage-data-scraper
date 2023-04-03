import logging
import json
import pandas as pd
import geopy
import xmltodict
import time

from bs4 import BeautifulSoup
from datetime import datetime
from seleniumwire.utils import decode as sw_decode
from .util import timenow
from .ga_scraper import BaseScraper, \
    Scraper1 as GA_Scraper1, \
    Scraper9 as GA_Scraper9, \
    Scraper11 as GA_Scraper11

# TODO: update for security
import ssl
ssl._create_default_https_context = ssl._create_unverified_context

class Scraper1(BaseScraper):
    def __init__(self, url, emc):
        super().__init__(url, emc)
        self.driver = self.init_webdriver()

    def parse(self):
        suffix = ['?report=report-panel-county', '?report=report-panel-zip']

        data = {}
        for s in suffix:
            url = self.url + s
            print(f"fetching {self.emc} outages from {url}")
            html = self.get_page_source(url)
            # print(html)

            # parse table
            soup = BeautifulSoup(html, 'html.parser')
            table = soup.find('table', attrs={'class': 'report-table tree'})
            rows = table.find_all('tr')

            data_rows = rows[2:]
            raw_data = []
            for row in data_rows:
                cells = row.find_all('td')
                raw_data.append([cell.text.strip() for cell in cells])

            loc = 'COUNTY' if s == '?report=report-panel-county' else 'ZIP'
            header = ['VIEW', loc, 'CUSTOMER OUTAGES', 'CUSTOMERS SERVED', '% AFFECTED']
            table_data = [dict(zip(header, row)) for row in raw_data]
            df = pd.DataFrame(table_data)[[loc, 'CUSTOMER OUTAGES', 'CUSTOMERS SERVED', '% AFFECTED']]
            df['timestamp'] = timenow()
            df['EMC'] = self.emc
            df = df[df['CUSTOMER OUTAGES'] != '0']
            key = 'per_county' if loc == 'COUNTY' else 'per_zipcode'
            data.update({key:df})

        return data


class Scraper3(BaseScraper):
    def __init__(self, url, emc):
        super().__init__(url, emc)
        self.driver = self.init_webdriver()

    def parse(self):
        pass

    def fetch(self):
        pass


class Scraper5(BaseScraper):
    def __init__(self, url, emc):
        super().__init__(url, emc)
        self.driver = self.init_webdriver()

    def parse(self):
        data = self.fetch()

        for key, val in data.items():
            if val:
                df = pd.DataFrame(val['areas'])
                df[['cust_a', 'percent_cust_a']] = df[['cust_a', 'percent_cust_a']].applymap(lambda x: x['val'])
                df = df[(df['cust_a'] != 0) | (df['n_out'] != 0)]
                df['timestamp'] = timenow()
                df['EMC'] = self.emc
                df.drop(columns=['gotoMap'], inplace=True)
                data.update({key: df})
            else:
                print(f"no outage of {self.emc} update found at",
                      datetime.strftime(datetime.now(), "%m-%d-%Y %H:%M:%S"))
        print(data)
        return data

    def fetch(self):
        print(f"fetching {self.emc} outages from {self.url}")
        # get javascript rendered source page
        self.driver.get(self.url)
        # let the page load
        time.sleep(10)
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
            json_requests = [r for r in requests if r.response and r.response.headers.get('Content-Type') == 'application/json']
            hash = v.split('/')[-1]
            for r in json_requests:
                if hash in r.url:
                    print(r)
                    response = sw_decode(r.response.body,
                                         r.response.headers.get('Content-Encoding', 'identity'))
                    data = response.decode("utf8", 'ignore')
                    if any([x in data for x in ['zip', 'Zip']]):
                        raw_data['per_zipcode'] = json.loads(data)['file_data']
                    elif 'county' in data:
                        raw_data['per_county'] = json.loads(data)['file_data']
                    elif any([x in data for x in ['city', 'Cities']]):
                        raw_data['per_city'] = json.loads(data)['file_data']
        return raw_data


class TXScraper:
    def __new__(cls, layout_id, url, emc):
        if layout_id == 1:
            obj = super().__new__(Scraper1)
        # elif layout_id == 2:
        #     obj = super().__new__(Scraper2)
        # elif layout_id == 3:
        #     obj = super().__new__(Scraper3)
        # elif layout_id == 4:
        #     obj = super().__new__(Scraper4)
        elif layout_id == 5:
            obj = super().__new__(Scraper5)
        # elif layout_id == 6:
        #     obj = super().__new__(Scraper6)
        # elif layout_id == 7:
        #     obj = super().__new__(Scraper7)
        elif layout_id == 8:
            obj = super().__new__(GA_Scraper1)
        # elif layout_id == 9:
        #     obj = super().__new__(Scraper9)
        # elif layout_id == 10:
        #     obj = super().__new__(Scraper10)
        elif layout_id == 11:
            obj = super().__new__(GA_Scraper9)
        elif layout_id == 12:
            obj = super().__new__(GA_Scraper11)
        # elif layout_id == 13:
        #     obj = super().__new__(Scraper13)
        # elif layout_id == 14:
        #     obj = super().__new__(Scraper14)
        # elif layout_id == 15:
        #     obj = super().__new__(Scraper15)
        # elif layout_id == 16:
        #     obj = super().__new__(Scraper16)
        # elif layout_id == 17:
        #     obj = super().__new__(Scraper17)
        else:
            raise "Invalid layout ID: Enter layout ID range from 1 to 17"

        obj.__init__(url, emc)
        return obj
