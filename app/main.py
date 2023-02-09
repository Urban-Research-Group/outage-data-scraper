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
            for key, df in data.items():
                if len(df):
                    timestamp = df['timestamp'][0]
                    path = f"{state}/layout_{layout_id}/{key}_{emc}_{timestamp}.csv"
                    ga_scraper.save(df, bucket, path)
                    print(f"outages of {emc} as of {timestamp} saved to {bucket} under {state}/layout_{layout_id}/")
            success_cnt += 1
        except Exception as e:
            print(e)
            continue

    return {
        'statusCode': 200,
        'body': json.dumps(f'Successfully scraped {success_cnt} out of {len(EMCs)} EMC outages')
    }


if __name__ == "__main__":
    with open("../data/ga/layout_11.json") as f:
        test_event = json.loads(f.read())
    handler(test_event)

    # sc = ga_scraper.Scraper(layout_id=3,
    #                         url='http://oms.coastalemc.com/OMSWebMap/',
    #                         emc='Coastal Electric Cooperation')
    #
    # print(sc.parse())


