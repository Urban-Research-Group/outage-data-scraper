from scrapers import ga_scraper

def handler(event, context=""):
    layout_id = event['layout']
    EMCs = event['emc']
    bucket = event['bucket']
    folder = event['folder']

    success_cnt = 0
    failed_cnt = 0

    for emc, url in EMCs.items():
        try:
            print(f"Scraping current outages under {emc} from {url}")

            sc = ga_scraper.Scraper1(url=url, emc=emc)
            data = sc.parse()
            filename = f"{emc}_{data['timestamp'][0]}"
            filepath = f"{folder}/{filename}.csv"

            # to local
            # data.to_csv(f"../data/{filename}.csv", index=False)

            # to s3
            sc.df_to_s3(data, bucket, filepath)

            print(f"Successfully scraped {emc}")
            success_cnt += 1

        except:
            print(f"failed to scraped outage data from {emc}")
            failed_cnt += 1
            continue

    print(f"success {success_cnt}, failed {failed_cnt}")

    return f"success {success_cnt}, failed {failed_cnt}"

# if __name__ == "__main__":
#     event = {
#         "layout": "layout_id",
#         "emc": {
#             "Washington EMC": "http://74.121.99.238:8081/",
#             "Rayle EMC": "http://outage.rayleemc.com:82/",
#             "Grady EMC": "http://outage.gradyemc.com:7576/",
#             "Southern Rivers Energy": "http://outage.southernriversenergy.com:82/",
#             "Altamaha": "http://outage.altamahaemc.com/",
#             "Canoochee EMC": "http://outage.canoocheeemc.com:8889/",
#             "Jefferson Energy Cooperative": "https://outage.jec.coop:83/",
#             "Diverse Power": "http://outages.diversepower.com/",
#             "Okefenoke REMC": "https://oms.oremc.co:8443/",
#             "Amicalola EMC": "https://outages.amicalolaemc.com:83/",
#             "Satilla REMC": "http://outageviewer.satillaemc.com:82/",
#             "Central Georgia EMC": "http://outage.cgemc.com:8181/",
#             "Flint EMC": "http://outage.flintemc.com:8283/",
#             "North Georgia EMC": "http://www2.ngemc.com:81/",
#             "Excelsior EMC": "http://outage.excelsioremc.com:8181/",
#             "Dalton Utility": "https://outageviewer.dutil.com/"
#         },
#         "bucket": "ga-outage-data-dev",
#         "folder": "data/layout-1"
#     }
#     # handler(event)

