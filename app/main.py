import json
import time

from scrapers import Scraper
from scrapers.util import save, timenow

def handler(event, context=""):
    layout_id = event['layout']
    EMCs = event['emc']
    bucket = event['bucket']
    state = event['folder']
    success_cnt = 0

    for emc, url in EMCs.items():
        try:
            sc = Scraper(state, layout_id, url, emc)
            data = sc.parse()
            for key, df in data.items():
                if df.empty:
                    print(f"no {key} outages for {emc} as of {timenow()}")
                else:
                    path = f"{state}/layout_{layout_id}/{key}_{emc}.csv"
                    save(df, bucket, path)
            success_cnt += 1
        except Exception as e:
            print(e)
            continue

    return {
        'statusCode': 200,
        'body': json.dumps(f'Successfully scraped {success_cnt} out of {len(EMCs)} EMC outages')
    }


if __name__ == "__main__":
    start = time.time()

    # handler test here
    with open("../data/ga/layout_10.json") as f:
        test_event = json.loads(f.read())
    handler(test_event)

    # single test here
    # sc = Scraper(state='tx',
    #              layout_id=5,
    #              url='https://stormcenter.oncor.com/default.html',
    #              emc='Storm Center')
    # print(sc.parse())

    end = time.time()
    print(end - start)


