import logging
from app.scraper.base import BaseScraper

class ScraperL7(BaseScraper):
    logger = logging.getLogger("ScraperL7")

    def __int__(self):
        super().__init__()

    def parse(self, response):
        pass






