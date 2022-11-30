import requests
import logging
from base import BaseScraper

import pandas as pd
from urllib.request import urlopen
import json

class ScraperL10(BaseScraper):
    logger = logging.getLogger("ScraperL10")

    def __init__(self, url):
        super().__init__()
        self.url = url

    def parse(self):
        response = self.fetch()
        attributes = [x['attributes'] for x in response['features']]
        response_df = pd.DataFrame(attributes)[['NAME', 'o_ConT', 'o_TotalCustomers']]
        response_df.columns = ['name', 'customersAffected', 'customersServed']
        return response_df

    def fetch(self):
        response = urlopen(self.url)
        data_json = json.loads(response.read())
        return data_json

if __name__ == '__main__':
    scrapper = ScraperL10('https://maps.ssemc.com/ninbouoockprserver/rest/services/OutageMap/PRODOutageMap_ALL_Cluster/FeatureServer/4/query?f=json&returnGeometry=true&spatialRel=esriSpatialRelIntersects&geometry=%7B%22xmin%22%3A-9353446.277200991%2C%22ymin%22%3A3952711.6066849865%2C%22xmax%22%3A-9314310.518718991%2C%22ymax%22%3A3991847.365166988%2C%22spatialReference%22%3A%7B%22wkid%22%3A102100%7D%7D&geometryType=esriGeometryEnvelope&inSR=102100&outFields=*&returnCentroid=false&returnExceededLimitFeatures=false&outSR=102100&resultType=tile&quantizationParameters=%7B%22mode%22%3A%22view%22%2C%22originPosition%22%3A%22upperLeft%22%2C%22tolerance%22%3A76.43702828515632%2C%22extent%22%3A%7B%22xmin%22%3A-9353446.277200991%2C%22ymin%22%3A3952711.6066849865%2C%22xmax%22%3A-9314310.518718991%2C%22ymax%22%3A3991847.3651669864%2C%22spatialReference%22%3A%7B%22wkid%22%3A102100%7D%7D%7D')
    print(scrapper.parse())

