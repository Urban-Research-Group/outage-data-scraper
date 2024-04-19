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


class MSScraper:
    def __new__(cls, layout_id, url, emc):
        pass
        # obj.__init__(url, emc)
        # return obj
