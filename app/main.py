import json
import time
import os

from scrapers import Scraper
from scrapers.util import save, timenow


def handler(event, context=""):
    layout_id = event["layout"]
    EMCs = event["emc"]
    bucket = event["bucket"]
    state = event["folder"]
    success_cnt = 0
    failures = []

    for emc, url in EMCs.items():
        try:
            sc = Scraper(state, layout_id, url, emc)
            data = sc.parse()
            for key, df in data.items():
                if df.empty:

                    print(f"no {key} outages for {emc} as of {timenow()}")
                else:
                    path = f"{state}/layout_{layout_id}/{key}_{emc}.csv"
                    #save(df, bucket, path)
                    print(df)
            success_cnt += 1
        except Exception as e:
            failure_reason = f"{state} Failed to scrape {emc}: {str(e)}"
            print(failure_reason)
            failures.append(failure_reason)
            continue

    if failures:
        print("Failures:")
        for failure in failures:
            print(failure)

    print(
        f"Successfully scraped {success_cnt} out of {len(EMCs)} EMC outages for {state}"
    )

    return {
        "statusCode": 200,
        "body": json.dumps(
            f"Successfully scraped {success_cnt} out of {len(EMCs)} EMC outages for {state}"
        ),
    }


if __name__ == "__main__":
    start = time.time()

    # handler test here
    event_path = os.path.join(os.getcwd(), "../events/la/layout_6.json")
    with open(event_path) as f:
        test_event = json.loads(f.read())
    handler(test_event)

    # single test here
    """ sc = Scraper(state='la',
                 layout_id=6,
                 url="https://utilisocial.io/datacapable/v2/p/lus/map/events",
                 emc="dev")
    print(sc.parse()) """

    end = time.time()
    print(end - start)
