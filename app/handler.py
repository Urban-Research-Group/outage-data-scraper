from scraper import scraper01
import aws_s3
import requests

def handler(event, context):
    # TODO configure this lambda handler, define event JSON format?
    """
    :param event:
    :param context:
    :return:
    """

    emc_startpage = {
        'Washington EMC': 'http://74.121.99.238:8081/',
        'Rayle EMC': 'http://outage.rayleemc.com:82/',
        'Grady EMC': 'http://outage.gradyemc.com:7576/',
        'Southern Rivers Energy': 'http://outage.southernriversenergy.com:82/',
        'Altamaha': 'http://outage.altamahaemc.com/',
        'Canoochee EMC': 'http://outage.canoocheeemc.com:8889/',
        'Jefferson Energy Cooperative': 'https://outage.jec.coop:83/',
        'Diverse Power': 'http://outages.diversepower.com/',
        'Okefenoke REMC': 'https://oms.oremc.co:8443/',
        'Amicalola EMC': 'https://outages.amicalolaemc.com:83/',
        'Satilla REMC': 'http://outageviewer.satillaemc.com:82/',
        'Central Georgia EMC': 'http://outage.cgemc.com:8181/',
        'Flint EMC': 'http://outage.flintemc.com:8283/',
        'North Georgia EMC': 'http://www2.ngemc.com:81/',
        'Excelsior EMC': 'http://outage.excelsioremc.com:8181/'
    }

    for emc, url in emc_startpage.items():
        print('checking for EMC:', emc)
        urls = f'{url}data/boundaries.json'
        # f'{url}data/outages.json',
        # f'{url}data/outageSummary.json']
        scrapper = scraper01.ScraperL1(urls)
        print(scrapper.parse())

    return "Successfully testing all layout #1 EMCs"


if __name__ == "__main__":
    handler(None, None)