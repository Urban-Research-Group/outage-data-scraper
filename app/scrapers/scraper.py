from .ca_scraper import CAScraper
from .ga_scraper import GAScraper
from .tx_scraper import TXScraper
from .il_scraper import ILScraper
from .ny_scraper import NYScraper
from .tn_scraper import TNScraper
from .fl_scraper import FLScraper
from .nc_scraper import NCScraper
from .sc_scraper import SCScraper
from .la_scraper import LAScraper
from .ms_scraper import MSScraper
from .al_scraper import ALScraper


class Scraper:
    def __new__(cls, state, layout_id, url, emc):
        if state == "ga":
            return GAScraper(layout_id, url, emc)
        elif state == "ca":
            return CAScraper(layout_id, url, emc)
        elif state == "tx":
            return TXScraper(layout_id, url, emc)
        elif state == "il":
            return ILScraper(layout_id, url, emc)
        elif state == "ny":
            return NYScraper(layout_id, url, emc)
        elif state == "tn":
            return TNScraper(layout_id, url, emc)
        elif state == "fl":
            return FLScraper(layout_id, url, emc)
        elif state == "nc":
            return NCScraper(layout_id, url, emc)
        elif state == "ms":
            return MSScraper(layout_id, url, emc)
        elif state == "la":
            return LAScraper(layout_id, url, emc)
        elif state == "sc":
            return SCScraper(layout_id, url, emc)
        elif state == "al":
            return ALScraper(layout_id, url, emc)

        else:
            raise "Invalid input state"
