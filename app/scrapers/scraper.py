from scrapers.ca_scraper import CAScraper
from scrapers.ga_scraper import GAScraper
from scrapers.tx_scraper import TXScraper

class Scraper:
    def __new__(cls, state, layout_id, url, emc):
        if state == 'ga':
            return GAScraper(layout_id, url, emc)
        elif state == 'ca':
            return CAScraper(layout_id, url, emc)
        elif state == 'tx':
            return TXScraper(layout_id, url, emc)
        else:
            raise "Invalid input state"
