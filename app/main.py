import json
import time
import os

from scrapers import Scraper
from scrapers.util import save, timenow

import pandas as pd

import argparse


def handler(event, context=""):
    layout_id = event["layout"]
    EMCs = event["emc"]
    bucket = event["bucket"]
    state = event["folder"]
    success_cnt = 0
    failures = {"state": [], "layout": [], "emc": [], "reason": [], "timestamp": []}

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
            failures["state"].append(state)
            failures["layout"].append(layout_id)
            failures["emc"].append(emc)
            failures["reason"].append(str(e))
            failures["timestamp"].append(timenow())

            print(f"{state} Failed to scrape {emc}: {str(e)}")

            continue

    # save failures to csv
    if len(failures["state"]) > 0:
        df = pd.DataFrame(failures)
        bucket = "scraperdowntime"
        path = f"{state}/layout_{layout_id}/failures.csv"
        save(df, bucket, path)

    # if len(failures["state"]) > 0:
    #     print("Failures:")
    #     for failure in failures:
    #         print(failure["emc"], failure["reason"])

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

    args = argparse.ArgumentParser()
    args.add_argument("-s", "--state", type=str, required=True)
    args.add_argument("-l", "--layout", type=str, required=True)
    args = args.parse_args()
    test_file = f"../events/{args.state}/layout_{args.layout}.json"

    start = time.time()

    # handler test here
    event_path = os.path.join(os.getcwd(), "../events/la/layout_6.json")
    event_path = os.path.join(os.getcwd(), test_file)
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
