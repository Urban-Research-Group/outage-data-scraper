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


def show_tracking(tracking):
    r = ""
    for x in tracking:
        r += str(x["row_customers_out"])
        r += ", "
    r = r[:-2]
    return r


def merger(outage_rows, threshold=0.1, abs_diff=0):
    if len(outage_rows) == 0:
        return []
    results = []
    tracking = []
    prev_row = None
    for row in outage_rows:
        current = {}

        # Basic information
        current["UtilityName"] = outage_rows[0]["UtilityName"]
        current["StateName"] = outage_rows[0]["StateName"]
        current["CountyName"] = outage_rows[0]["CountyName"]
        current["CityName"] = outage_rows[0]["CityName"]
        current["CountyFIPS"] = outage_rows[0]["CountyFIPS"]
        current["CustomersTracked"] = outage_rows[0]["CustomersTracked"]
        current["RecordDateTime"] = pd.to_datetime(row["RecordDateTime"])

        # Must be new outage
        if tracking == []:
            current["start_time"] = current["RecordDateTime"]
            current["end_time"] = current["RecordDateTime"]
            current["row_customers_out"] = row["CustomersOut"]

            current["customer_affected_total"] = 0
            current["customer_affected_mean"] = row["CustomersOut"]

            tracking.append(current)
            logging.info(
                f"Case 0: Tracking is empty, create new outage ({show_tracking(tracking)})"
            )
            prev_row = row
            continue

        last_outage = tracking[-1].copy()

        # Case A: increase of customers out is less than threshold, just update last outage
        if (
            row["CustomersOut"] - prev_row["CustomersOut"]
            < threshold * last_outage["row_customers_out"]
            or abs(row["CustomersOut"] - prev_row["CustomersOut"]) < abs_diff
        ) and row["CustomersOut"] >= prev_row["CustomersOut"]:
            logging.debug(
                f"last_outage: {last_outage['customer_affected_mean']} will update with {row['CustomersOut'] - prev_row['CustomersOut']}"
            )
            logging.debug(
                f"Last_outage's row_customers_out: {last_outage['row_customers_out']}"
            )
            tracking[-1]["end_time"] = current["RecordDateTime"]

            tracking[-1]["row_customers_out"] = last_outage["row_customers_out"] + (
                row["CustomersOut"] - prev_row["CustomersOut"]
            )

            tracking[-1]["customer_affected_total"] += (
                current["RecordDateTime"] - last_outage["end_time"]
            ).total_seconds() * last_outage["row_customers_out"]

            tracking[-1]["customer_affected_mean"] = (
                tracking[-1]["customer_affected_total"]
                / (
                    tracking[-1]["end_time"] - tracking[-1]["start_time"]
                ).total_seconds()
            )
            logging.info(
                f"Case A: increase of customers out is less than threshold, just update last outage, tracking: {show_tracking(tracking)}"
            )
            prev_row = row
            continue

        # Case B: increase of customers out is more than threshold, create new outage
        elif (
            row["CustomersOut"] - prev_row["CustomersOut"]
            >= threshold * last_outage["row_customers_out"]
            and abs(row["CustomersOut"] - prev_row["CustomersOut"]) >= abs_diff
        ) and row["CustomersOut"] >= prev_row["CustomersOut"]:
            current["start_time"] = current["RecordDateTime"]
            current["end_time"] = current["RecordDateTime"]
            current["row_customers_out"] = (
                row["CustomersOut"] - prev_row["CustomersOut"]
            )

            current["customer_affected_total"] = (
                current["RecordDateTime"] - current["start_time"]
            ).total_seconds() * current["row_customers_out"]

            current["customer_affected_mean"] = (
                row["CustomersOut"] - prev_row["CustomersOut"]
            )
            tracking.append(current)
            logging.info(
                f"Case B: increase of customers out is more than threshold, create new outage, tracking: {show_tracking(tracking)}"
            )

            prev_row = row
            continue
        # Case C: decrease of customers out is less than threshold, just update last outage
        elif (
            prev_row["CustomersOut"] - row["CustomersOut"]
            < threshold * last_outage["row_customers_out"]
            or abs(row["CustomersOut"] - prev_row["CustomersOut"]) < abs_diff
        ) and row["CustomersOut"] <= prev_row["CustomersOut"]:
            tracking[-1]["end_time"] = current["RecordDateTime"]
            tracking[-1]["row_customers_out"] = last_outage["row_customers_out"] - (
                prev_row["CustomersOut"] - row["CustomersOut"]
            )

            tracking[-1]["customer_affected_total"] += (
                current["RecordDateTime"] - last_outage["end_time"]
            ).total_seconds() * last_outage["row_customers_out"]

            tracking[-1]["customer_affected_mean"] = (
                tracking[-1]["customer_affected_total"]
                / (
                    tracking[-1]["end_time"] - tracking[-1]["start_time"]
                ).total_seconds()
            )
            logging.info(
                f"Case C: decrease of customers out is less than threshold, just update last outage, tracking: {show_tracking(tracking)}"
            )
            prev_row = row
            continue

        # Case D: decrease of customers out is more than threshold, record to first outage and add to results
        elif (
            prev_row["CustomersOut"] - row["CustomersOut"]
            >= threshold * last_outage["row_customers_out"]
            and abs(row["CustomersOut"] - prev_row["CustomersOut"]) >= abs_diff
        ) and row["CustomersOut"] <= prev_row["CustomersOut"]:

            decreasaing_amount = prev_row["CustomersOut"] - row["CustomersOut"]

            indexes_to_pop = []

            closest_index = 0
            for outage in tracking:
                if abs(decreasaing_amount - outage["row_customers_out"]) < abs(
                    decreasaing_amount - tracking[closest_index]["row_customers_out"]
                ):
                    closest_index = tracking.index(outage)

            # Ideally, decreasing_amount should be closed to tracking[0]['customer_affected_mean']
            if abs(
                decreasaing_amount - tracking[closest_index]["row_customers_out"]
            ) >= max(10, 0.1 * decreasaing_amount):
                logging.warning(
                    f"Problem: Decreasing amount {decreasaing_amount} is not close to closest tracking {tracking[closest_index]['customer_affected_mean']}"
                )
                tmp = ""
                for x in outage_rows:
                    tmp += str(x["CustomersOut"]) + ","
                tmp = tmp[:-1]
                logging.warning(f"Outage rows: {tmp}")
                logging.warning(f"We are tracking: {show_tracking(tracking)}")

                # if decreasinig amount <= closest_index, we should split this closest_index to 2 outages
                if decreasaing_amount <= tracking[closest_index]["row_customers_out"]:
                    # Create new outage
                    new_outage = tracking[closest_index].copy()
                    new_outage["row_customers_out"] = (
                        tracking[closest_index]["customer_affected_mean"]
                        - decreasaing_amount
                    )
                    new_outage["customer_affected_mean"] = (
                        tracking[closest_index]["customer_affected_mean"]
                        - decreasaing_amount
                    )
                    new_outage["customer_affected_total"] = (
                        new_outage["customer_affected_mean"]
                        * (
                            tracking[closest_index]["end_time"]
                            - tracking[closest_index]["start_time"]
                        ).total_seconds()
                    )

                    tracking.append(new_outage)

                    tracking[closest_index]["row_customers_out"] = decreasaing_amount
                    tracking[closest_index][
                        "customer_affected_mean"
                    ] = decreasaing_amount
                    tracking[closest_index]["customer_affected_total"] = (
                        tracking[closest_index]["customer_affected_mean"]
                        * (
                            tracking[closest_index]["end_time"]
                            - tracking[closest_index]["start_time"]
                        ).total_seconds()
                    )

                    indexes_to_pop.append(closest_index)

                    logging.warning(
                        f"Case D-1: split to tracking: {show_tracking(tracking)}"
                    )

                else:
                    logging.warning("Case D-2")
                    # indexes_to_pop.append(closest_index)
                    # sort tmp_l by descreasing customer_affected_mean
                    tracking = sorted(
                        tracking, key=lambda x: x["row_customers_out"], reverse=True
                    )

                    # Find the first outage that has row_customers_out < decreasaing_amount
                    for outage in tracking:
                        if outage["row_customers_out"] < decreasaing_amount:
                            closest_index = tracking.index(outage)
                            break

                    # Starting from closest_index, collect outages until the sum of row_customers_out >= decreasaing_amount
                    sum_customer_affected_mean = 0
                    for i in range(closest_index, len(tracking)):
                        sum_customer_affected_mean += tracking[i]["row_customers_out"]
                        if sum_customer_affected_mean >= decreasaing_amount:
                            sum_customer_affected_mean -= tracking[i][
                                "row_customers_out"
                            ]
                            continue
                        indexes_to_pop.append(i)

                logging.warning("=====================================")
            else:
                indexes_to_pop.append(closest_index)

            for index in indexes_to_pop:
                tracking[index]["end_time"] = current["RecordDateTime"]
                # tracking[index]['row_customers_out'] = row['CustomersOut']

                tracking[index]["customer_affected_total"] += (
                    current["RecordDateTime"] - last_outage["end_time"]
                ).total_seconds() * tracking[index]["row_customers_out"]

                tracking[index]["customer_affected_mean"] = (
                    tracking[index]["customer_affected_total"]
                    / (
                        tracking[index]["end_time"] - tracking[index]["start_time"]
                    ).total_seconds()
                )

                tracking[index]["duration"] = (
                    tracking[index]["end_time"] - tracking[index]["start_time"]
                )

                tracking[index].pop("row_customers_out")
                tracking[index].pop("customer_affected_total")
                tracking[index].pop("RecordDateTime")
                # 'row_customers_out']

                results.append(tracking[index])

            new_tracking = []
            for outage in tracking:
                if tracking.index(outage) not in indexes_to_pop:
                    new_tracking.append(outage.copy())
                else:
                    logging.debug(f"Pop outage: {outage['customer_affected_mean']}")

            tracking = sorted(new_tracking, key=lambda x: x["start_time"]).copy()

            logging.info(
                f"Case D: decrease of customers out is more than threshold, record 'closest' outage and add to results, tracking: {show_tracking(tracking)}"
            )

            prev_row = row
            continue
        logging.error("Case E: Something wrong")

    # Add last tracking to results
    if len(tracking) > 0:
        # Close all outages in tracking
        for outage in tracking:

            outage["customer_affected_total"] += (
                pd.to_datetime(prev_row["RecordDateTime"]) - outage["end_time"]
            ).total_seconds() * outage["row_customers_out"]

            outage["end_time"] = pd.to_datetime(prev_row["RecordDateTime"])
            outage["duration"] = outage["end_time"] - outage["start_time"]

            if (outage["end_time"] - outage["start_time"]).total_seconds() == 0:
                # logging.error(f"{show_tracking(tracking)}")
                # logging.error(f"outage: {outage}")
                continue

            outage["customer_affected_mean"] = (
                outage["customer_affected_total"]
                / (outage["end_time"] - outage["start_time"]).total_seconds()
            )

            outage.pop("row_customers_out")
            outage.pop("customer_affected_total")
            outage.pop("RecordDateTime")
            results.append(outage)

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

parser.add_argument(
    "-a",
    "--abs_diff",
    default="0",
    help="Provide abs_diff. Example --abs_diff 0, default=0",
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

df = pd.read_csv("step0/" + args.filename + ".csv", encoding="utf-16")

# show number of rows of df
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
abs_diff = float(args.abs_diff)

for index, row in df.iterrows():
    if is_new(last_row, row):
        if len(outage_rows) > 0:
            tmp = merger(outage_rows, threshold, abs_diff)
            if len(tmp) != 0:
                result += tmp
        outage_rows = []
    outage_rows.append(row)
    last_row = row

if len(outage_rows) > 0:
    tmp = merger(outage_rows, threshold, abs_diff)
    if len(tmp) != 0:
        result += tmp


# Convert result to dataframe
result_df = pd.DataFrame(result)

# Filter out rows with "duration" == 0 seconds
result_df = result_df[result_df["duration"] > pd.Timedelta(seconds=0)]

# Store to csv file
# result_df.to_csv("step1/test2.csv", index=False, encoding="utf-16")
result_df.to_csv(
    "step1/threshold/"
    + args.filename
    + "_"
    + args.threshold
    + "_"
    + args.abs_diff
    + ".csv",
    index=False,
    encoding="utf-16",
)
# result_df.to_csv("step1/GVECI_2023_05_modified_0.4.csv", index=False, encoding="utf-16")
# result_df.to_csv("step1/SCE_2023_06_modified_0.4.csv", index=False, encoding="utf-16")
logging.info(
    f"File stored to step1/threshold/{args.filename}_{args.threshold}_{args.abs_diff}.csv"
)
