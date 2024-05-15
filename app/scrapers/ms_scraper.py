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

from .ga_scraper import (
    Scraper3 as GA_Scraper3,
    Scraper4 as GA_Scraper4,
    Scraper9 as GA_Scraper9,
)

from .tx_scraper import (
    Scraper4 as TX_Scraper4,
)

# TODO: update for security
import ssl

ssl._create_default_https_context = ssl._create_unverified_context

class MSScraper:
    def __new__(cls, layout_id, url, emc):
        if layout_id == 3:
            obj = super().__new__(GA_Scraper3)
        elif layout_id == 4:
            obj = super().__new__(GA_Scraper4)
        elif layout_id == 5:
            obj = super().__new__(GA_Scraper9)
        elif layout_id == 8:
            obj = super().__new__(TX_Scraper4)
        obj.__init__(url, emc)
        return obj