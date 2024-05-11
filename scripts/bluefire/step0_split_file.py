# Read csv file
import pandas as pd
import numpy as np
import os


df = pd.read_csv("files/POUS_Export_CityByUtility_Raw_2023.csv", encoding="utf-16")

# Filter out rows with "UtilityName" including "Flint"
df = df[df["UtilityName"].str.contains("Flint")]
# df = df[df["UtilityName"].str.contains("Southern California Edison")]

# RecordDateTime is in the format of "YYYY-MM-DD HH:MM:SS"
# Convert it to datetime object
df["RecordDateTime"] = pd.to_datetime(df["RecordDateTime"])


# Filter out rows with "RecordDateTime" between "2023-10-01 00:00:00" and "2023-10-31 23:59:59
df = df[
    (df["RecordDateTime"] >= "2024-04-01 00:00:00")
    & (df["RecordDateTime"] <= "2024-04-11 00:00:00")
]


# Store to csv file
df.to_csv("step0/Flint_2023_05_6month.csv", index=False, encoding="utf-16")

# show the first 5 rows
# print(df.head(5))
