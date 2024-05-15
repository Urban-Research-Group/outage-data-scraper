import argparse

# Read csv file
import pandas as pd
import numpy as np
import datetime
from dataclasses import dataclass
import os

parser = argparse.ArgumentParser()

parser.add_argument(
    "-f",
    "--filename",
    default="test.csv",
    help="Provide filename. Example --filename test, default=test.csv",
)

parser.add_argument(
    "-m",
    "--method",
    default="threshold",
    help="Provide method. Example --method threshold, default=threshold",
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

if args.method == "threshold":
    df = pd.read_csv(
        "step1/"
        + args.method
        + "/"
        + args.filename
        + "_"
        + args.threshold
        + "_"
        + args.abs_diff
        + ".csv",
        encoding="utf-16",
    )
else:
    df = pd.read_csv(
        "step1/" + args.method + "/" + args.filename + "_" + args.threshold + ".csv",
        encoding="utf-16",
    )

# calculate number of rows
frequency = df.shape[0]

# sum up duration column
df["duration"] = pd.to_timedelta(df["duration"])
total_duration = df["duration"].sum()

# sum up customer_affected_mean column
total_customer_affected_mean = df["customer_affected_mean"].sum()

# sum up customer_affected_mean * duration row by row
df["customer_affected_mean"] = pd.to_numeric(df["customer_affected_mean"])
df["duration"] = pd.to_numeric(df["duration"].dt.total_seconds())
df["customer_affected_mean_duration"] = df["customer_affected_mean"] * df["duration"]
total_customer_affected_mean_duration = df["customer_affected_mean_duration"].sum()

# print the result
print("=====================================================")
print(
    f"Property for {args.method} {args.filename} {args.threshold} {args.abs_diff if args.method == 'threshold' else ''}"
)
print("avg_duration", total_duration / frequency)
print("frequency", frequency)
print("avg_customer_affected_mean", total_customer_affected_mean / frequency)
print(
    "total_customer_affected_mean_duration",
    total_customer_affected_mean_duration / (frequency * 60),
)
print(
    "total_customer_affected_x_duration",
    total_customer_affected_mean_duration / (60),
)
print("=====================================================")
