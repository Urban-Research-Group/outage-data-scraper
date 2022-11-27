import logging
from app.scraper.base import BaseScraper

class ScraperL3(BaseScraper):
    logger = logging.getLogger("ScraperL3")

    def __int__(self):
        super().__init__()

    def parse(self, response):
        pass






