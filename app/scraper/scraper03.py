import logging
from base import BaseScraper

from urllib.request import urlopen
import xmltodict
import pandas as pd

class ScraperL3(BaseScraper):
    logger = logging.getLogger("ScraperL3")

    def __init__(self, url):
        super().__init__()
        self.url = url

    def parse(self):
        data = self.fetch()
        df =  pd.DataFrame(data['ArrayOfMobileCounty']['MobileCounty'])[['CountyName', 'CustomersServed', 'CustomersAffected']]
        df.columns = ['name', 'customersServed', 'customersAffected']
        return df[['name', 'customersAffected', 'customersServed']]


    def fetch(self):
        response = urlopen(self.url)
        data = response.read()
        response.close()

        data = xmltodict.parse(data)
        return data

if __name__ == '__main__':
    scrapper = ScraperL3('https://oms.coastalemc.com/OMSWebMap/MobileMap/OMSMobileService.asmx/GetAllCounties')
    print(scrapper.parse())

    scrapper = ScraperL3('https://www.upsonemc.com/OMSWebMap/MobileMap/OMSMobileService.asmx/GetAllCounties')
    print(scrapper.parse())

    scrapper = ScraperL3('http://map.irwinemc.com/omswebmap/MobileMap/OMSMobileService.asmx/GetAllCounties')
    print(scrapper.parse())


