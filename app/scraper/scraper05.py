import logging
from app.scraper.base import BaseScraper

class ScraperL5(BaseScraper):
    logger = logging.getLogger("ScraperL5")

    def __int__(self):
        super().__init__()

    def parse(self, response):
        pass






