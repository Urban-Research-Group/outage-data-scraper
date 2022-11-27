import logging
import json

from urllib.request import urlopen
from urllib.error import HTTPError, URLError

class BaseScraper:
    logger = logging.getLogger("BaseScraper")

    def __int__(self):
        self.driver = ''

    # def parse(self, response):
    #     pass

    def fetch(self, url):
        """
        make a GET request to the url
        :param url:
        :return: body, status
        """
        try:
            with urlopen(url, timeout=10) as response:
                print(response.status)
                return response.read(), response.status
        except HTTPError as error:
            print(error.status, error.reason)
        except URLError as error:
            print(error.reason)
        except TimeoutError:
            print("Request timed out")

    def save(self, data, file_path):
        pass






