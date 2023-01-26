from scraper import scrapers

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

            sc = scrapers.Scraper1(url=url, emc=emc)
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



