import argparse

# Read csv file
import pandas as pd
import numpy as np
import datetime
from dataclasses import dataclass
import os
import logging


# determine if a row belongs to new outage
def is_new(last_row, row):
    if last_row is None:
        return True
    if (
        last_row["UtilityName"],
        last_row["StateName"],
        last_row["CountyName"],
        last_row["CityName"],
    ) != (row["UtilityName"], row["StateName"], row["CountyName"], row["CityName"]):
        return True
    if last_row["CustomersOut"] == 0:
        return True
    return False


def helper(outage_rows):
    result = {}

    for row in outage_rows:
        row["RecordDateTime"] = pd.to_datetime(row["RecordDateTime"])

    result["UtilityName"] = outage_rows[0]["UtilityName"]
    result["StateName"] = outage_rows[0]["StateName"]
    result["CountyName"] = outage_rows[0]["CountyName"]
    result["CityName"] = outage_rows[0]["CityName"]
    result["CountyFIPS"] = outage_rows[0]["CountyFIPS"]
    result["CustomersTracked"] = outage_rows[0]["CustomersTracked"]

    result["start_time"] = min([row["RecordDateTime"] for row in outage_rows])
    result["end_time"] = max([row["RecordDateTime"] for row in outage_rows])
    result["duration"] = result["end_time"] - result["start_time"]

    if result["duration"].total_seconds() == 0:
        return {}

    result["customer_affected_mean"] = 0

    for i in range(len(outage_rows) - 1):
        # customers out * interval with next row
        result["customer_affected_mean"] += (
            outage_rows[i + 1]["RecordDateTime"] - outage_rows[i]["RecordDateTime"]
        ).total_seconds() * outage_rows[i]["CustomersOut"]

    result["customer_affected_mean"] /= result["duration"].total_seconds()

    return result


def merger(outage_rows, threshold=0.05):
    if len(outage_rows) == 0:
        return []
    results = []

    events = []  # pair of start and end index
    flag = False  # False: finding start, True: finding end
    current_start = 0
    current_end = 0
    for i in range(len(outage_rows)):
        if not flag:
            if (
                outage_rows[i]["CustomersOut"]
                >= threshold * outage_rows[i]["CustomersTracked"]
            ):
                current_start = i
                flag = True
        else:
            if (
                outage_rows[i]["CustomersOut"]
                < threshold * outage_rows[i]["CustomersTracked"]
            ):
                current_end = i
                events.append((current_start, current_end))
                flag = False

    for start, end in events:
        results.append(helper(outage_rows[start : end + 1]))

    return results


parser = argparse.ArgumentParser()
parser.add_argument(
    "-l",
    "--loglevel",
    default="warning",
    help="Provide logging level. Example --loglevel debug, default=warning",
)

parser.add_argument(
    "-f",
    "--filename",
    default="test.csv",
    help="Provide filename. Example --filename test, default=test.csv",
)

parser.add_argument(
    "-t",
    "--threshold",
    default="0.0005",
    help="Provide threshold. Example --threshold 0.0005, default=0.0005",
)


args = parser.parse_args()
logging.root.handlers = []
d = {
    "debug": logging.DEBUG,
    "info": logging.INFO,
    "warning": logging.WARNING,
    "error": logging.ERROR,
    "critical": logging.CRITICAL,
}

logging.basicConfig(
    level=d[args.loglevel.lower()],
    format="[%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()],
)


# df = pd.read_csv("step0/NGEMC_2023_10.csv", encoding="utf-16")
# df = pd.read_csv("step0/GVECI_2023_05.csv", encoding="utf-16")
# df = pd.read_csv("step0/SCE_2023_06.csv", encoding="utf-16")
df = pd.read_csv("step0/" + args.filename + ".csv", encoding="utf-16")

logging.debug(df.shape[0])
# remove duplicate first, should not happen
idx = df.groupby(
    ["UtilityName", "StateName", "CountyName", "CityName", "RecordDateTime"]
)["CustomersOut"].idxmin()
df = df.loc[idx]
logging.debug(df.shape[0])


# Convert each row into OutageRow object

outage_rows = []  # belong to same outage
last_row = None
result = []
threshold = float(args.threshold)

for index, row in df.iterrows():
    if is_new(last_row, row):
        if len(outage_rows) > 0:
            tmp = merger(outage_rows, threshold)
            if len(tmp) != 0:
                result += tmp
        outage_rows = []
    outage_rows.append(row)
    last_row = row

if len(outage_rows) > 0:
    tmp = merger(outage_rows, threshold)
    if len(tmp) != 0:
        result += tmp

# Convert result to dataframe
result_df = pd.DataFrame(result)

# Filter out rows with "duration" == 0 seconds
if len(result_df) == 0:
    logging.warning("No outage found")
else:
    result_df = result_df[result_df["duration"] > pd.Timedelta(seconds=0)]


# Store to csv file
# result_df.to_csv("step1/NGEMC_2023_10.csv", index=False, encoding="utf-16")
result_df.to_csv(
    "step1/ganz/" + args.filename + "_" + str(threshold) + ".csv",
    index=False,
    encoding="utf-16",
)

logging.info(f"File stored to step1/ganz/{args.filename}_{threshold}.csv")
