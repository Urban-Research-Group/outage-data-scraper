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

from .util import is_aws_env, make_request, timenow

# TODO: update for security
import ssl

ssl._create_default_https_context = ssl._create_unverified_context

from .ga_scraper import (
    BaseScraper,
    Scraper1 as GA_Scraper1,
    Scraper3 as GA_Scraper3
)

class MSScraper9(BaseScraper): # not finished
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

    data = {}
    data["currentOutages"] = self.driver.find_element(By.ID, "Outages").text
    data["customersAffected"] = self.driver.find_element(By.ID, "Customers-Affected").text
    data["stillOut"] = self.driver.find_element(By.ID, "Still-Out").text
    return data



class MSScraper:
    def __new__(cls, layout_id, url, emc):
        if layout_id == 2:
            obj = super().__new__(GA_Scraper1)
        elif layout_id == 6:
            obj = super().__new__(GA_Scraper3)
        elif layout_id == 9:
            obj = super().__new__(MSScraper9)
        obj.__init__(url, emc)
        return obj