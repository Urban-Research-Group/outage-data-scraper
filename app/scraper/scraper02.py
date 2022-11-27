import logging
import json
from app.scraper.base import BaseScraper

class ScraperL2(BaseScraper):
    logger = logging.getLogger("ScraperL2")

    def __int__(self, url):
        super().__init__(url)
        self.url = url

    def parse(self):
        body, status = self.fetch(self.url)
        data = json.loads(body)
        print(data)







