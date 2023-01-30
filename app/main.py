import json
from scrapers import ga_scraper


def handler(event, context=""):
    layout_id = event['layout']
    EMCs = event['emc']
    bucket = event['bucket']
    state = event['folder']
    success_cnt = 0

    for emc, url in EMCs.items():
        try:
            sc = ga_scraper.Scraper(layout_id, url, emc)
            data = sc.parse()
            for idx, df in enumerate(data):
                timestamp = df['timestamp'][0]
                path = f"{state}/layout_{layout_id}/{idx}_{emc}_{timestamp}.csv"
                ga_scraper.save(df, bucket, path)
                print(f"outages of {emc} as of {timestamp} saved to {bucket} under {state}/layout_{layout_id}/")
            success_cnt += 1
        except Exception as e:
            print(f'{e}: unable to access {emc} at {url}')
            continue

    return {
        'statusCode': 200,
        'body': json.dumps(f'Successfully scraped {success_cnt} out of {len(EMCs)} EMC outages')
    }


if __name__ == "__main__":
    event = {
        "layout": 1,
        "emc": {
            "Washington EMC": "http://74.121.99.238:8081/",
            "Rayle EMC": "http://outage.rayleemc.com:82/",
            "Grady EMC": "http://outage.gradyemc.com:7576/",
            "Southern Rivers Energy": "http://outage.southernriversenergy.com:82/",
            "Altamaha": "http://outage.altamahaemc.com/",
            "Canoochee EMC": "http://outage.canoocheeemc.com:8889/",
            "Jefferson Energy Cooperative": "https://outage.jec.coop:83/",
            "Diverse Power": "http://outages.diversepower.com/",
            "Okefenoke REMC": "https://oms.oremc.co:8443/",
            "Amicalola EMC": "https://outages.amicalolaemc.com:83/",
            "Satilla REMC": "http://outageviewer.satillaemc.com:82/",
            "Central Georgia EMC": "http://outage.cgemc.com:8181/",
            "Flint EMC": "http://outage.flintemc.com:8283/",
            "North Georgia EMC": "http://www2.ngemc.com:81/",
            "Excelsior EMC": "http://outage.excelsioremc.com:8181/",
            "Dalton Utility": "https://outageviewer.dutil.com/"
        },
        "bucket": "ga-outage-data-dev",
        "folder": "../ga_data/layout_1"
    }
    handler(event)
