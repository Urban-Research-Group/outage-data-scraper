import argparse
# Read csv file
import pandas as pd
import numpy as np
import datetime
from dataclasses import dataclass
import os

# df = pd.read_csv("files/North Georgia EMC_02_02_24.csv", encoding="utf-8")
# df = pd.read_csv(
#     "files/Guadalupe Valley Electric Coop, Inc._02_02_24.csv", encoding="utf-8"
# )
# df = pd.read_csv("files/ca_03_01_24.csv", encoding="utf-8")
parser = argparse.ArgumentParser()

parser.add_argument(
    "-f",
    "--filename",
    default="test.csv",
    help="Provide filename. Example --filename test, default=test.csv",
)

parser.add_argument(
    "-m",
    "--month",
    default="5",
    help="Provide month. Example --month 5, default=5",
)

parser.add_argument(
    "-y",
    "--year",
    default="2023",
    help="Provide year. Example --year 2023, default=2023",
)

parser.add_argument(
    "-nm",
    "--next_month",
    default="xxx",
)


args = parser.parse_args()
df = pd.read_csv("files/" +  args.filename + ".csv", encoding="utf-8")

# Add 0 if month is less than 10
if int(args.month) < 9:
    month = "0" + args.month
    year = args.year
    next_month = "0" + str(int(args.month) + 1)
    next_year = args.year
elif int(args.month) < 12:
    month = args.month
    year = args.year
    next_month = str(int(args.month) + 1)
    next_year = args.year
else:
    month = "12"
    year = args.year
    next_month = "01"
    next_year = str(int(args.year) + 1)

if args.next_month != "xxx":
    next_month = args.next_month


# filter out start_time between "2023-10-01 00:00:00" and "2023-10-31 23:59:59
# df["start_time"] = pd.to_datetime(df["start_time"])
df = df[
    (df["start_time"] >= year + "-"+month+"-01 00:00:00")
    & (df["start_time"] <=  next_year + "-"+next_month+"-01 00:00:00")
]

print(year + "-"+month+"-01 00:00:00")
print(next_year + "-"+next_month+"-01 00:00:00")
# calculate number of rows
frequency = df.shape[0]

# sum up duration column
total_duration = df["duration"].sum()

# sum up customer_affected_mean column
total_customer_affected_mean = df["customer_affected_mean"].sum()

# sum up customer_affected_mean * duration row by row
df["customer_affected_mean"] = pd.to_numeric(df["customer_affected_mean"])
df["duration"] = pd.to_numeric(df["duration"])
df["customer_affected_mean_duration"] = df["customer_affected_mean"] * df["duration"]
total_customer_affected_mean_duration = df["customer_affected_mean_duration"].sum()

# print the result
print("=====================================================")
print(f"Property for {args.filename} at {args.year}-{args.month}")
print("avg_duration", total_duration / frequency)
print("frequency", frequency)
print("avg_customer_affected_mean", total_customer_affected_mean / frequency)
print(
    "total_customer_affected_mean_duration",
    total_customer_affected_mean_duration / frequency,
)
print(
    "total_customer_affected_x_duration",
    total_customer_affected_mean_duration ,
)
print("=====================================================")

# Store to csv file
# result_df.to_csv("step1/NGEMC_2023_10.csv", index=False, encoding="utf-16")
# result_df.to_csv("step1/SCE_2023_06.csv", index=False, encoding="utf-16")
