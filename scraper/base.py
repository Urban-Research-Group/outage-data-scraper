import requests
import logging

class BaseScraper:
    logger = logging.getLogger("BaseScraper")

    def __int__(self):
        self.driver = ''

    def parse(self, response):
        pass
    
    def fetch(self):
        pass





