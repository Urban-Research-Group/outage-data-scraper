from .ga_scraper import (
    BaseScraper,
    Scraper1 as GA_Scraper1,
    Scraper3 as GA_Scraper3,
    Scraper6 as GA_Scraper6,
    Scraper7 as GA_Scraper7,
    Scraper9 as GA_Scraper9,
)

class NCScraper:
    def __new__(cls, layout_id, url, emc):
        if layout_id == 1:
            obj = super().__new__(GA_Scraper1)
        elif layout_id == 2:
            obj = super().__new__(GA_Scraper9)
        elif layout_id == 3:
            obj = super().__new__(GA_Scraper3)
        elif layout_id == 4:
            obj = super().__new__(GA_Scraper7)
        elif layout_id == 5:
            obj = super().__new__(GA_Scraper6)
        else:
            raise "Invalid layout ID: Enter layout ID range from 1 to 2"
        obj.__init__(url, emc)
        return obj