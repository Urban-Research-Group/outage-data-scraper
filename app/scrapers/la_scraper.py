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
    Scraper6 as GA_Scraper6 # sienatech

)

class LAScraper:
    def __new__(cls, layout_id, url, emc):
        if layout_id == 1:
            obj = super().__new__(GA_Scraper1)
        elif layout_id == 2: 
            obj = super().__new__(GA_Scraper6)
        else: 
            raise "Invalid layout ID: Enter layout ID range from 1 to 2"
        obj.__init__(url, emc)
        return obj