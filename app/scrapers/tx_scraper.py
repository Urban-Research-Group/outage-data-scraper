import logging
import json
import pandas as pd
import geopy
import xmltodict
import time

from bs4 import BeautifulSoup
from datetime import datetime
from urllib.request import urlopen
from seleniumwire.utils import decode as sw_decode
from .util import make_request, timenow
from .ga_scraper import BaseScraper

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


class Scraper5(BaseScraper):
    def __init__(self, url, emc):
        super().__init__(url, emc)
        self.driver = self.init_webdriver()

    def parse(self):
        data = self.fetch()

        for key, val in data.items():
            if val:
                df = pd.DataFrame(val['areas'])
                df[['cust_a', 'percent_cust_a']] = df[['cust_a', 'percent_cust_a']].applymap(lambda x : x['val'])
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
        time.sleep(3)
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
            time.sleep(3)
            requests = self.driver.requests
            for r in requests:
                if v in r.url:
                    print(r)
                    response = sw_decode(r.response.body,
                                         r.response.headers.get('Content-Encoding', 'identity'))
                    data = response.decode("utf8", 'ignore')
                    if any([x in data for x in ['zip', 'Zip', 'City']]):
                        raw_data['per_zipcode'] = json.loads(data)['file_data']
                    elif 'county' in data:
                        raw_data['per_county'] = json.loads(data)['file_data']
        return raw_data


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


class Scraper11(BaseScraper):
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
        data = {'per_county': df}
        return data

    def fetch(self):
        print(f"fetching {self.emc} outages from {self.url}")
        # Send a request to the website and let it load
        self.driver.get(self.url)
        time.sleep(10)
        # WebDriverWait(self.driver, 10).until(EC.frame_to_be_available_and_switch_to_it((By.ID, "ptifrmtgtframe")))
        # WebDriverWait(self.driver, 20).until(EC.visibility_of_element_located((By.XPATH, "/html/body/div[5]/div[2]/div/div/div[3]/div/div[2]/div/div/div/div[1]/table")))
        print(self.driver.page_source)


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
            obj = super().__new__(Scraper8)
        # elif layout_id == 9:
        #     obj = super().__new__(Scraper9)
        # elif layout_id == 10:
        #     obj = super().__new__(Scraper10)
        elif layout_id == 11:
            obj = super().__new__(Scraper11)
        # elif layout_id == 12:
        #     obj = super().__new__(Scraper12)
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
