import os
import sys
import pkg_resources

initial_modules = set(sys.modules.keys())
sys.path.insert(0, "\\".join(os.path.dirname(__file__).split("\\")[:-1]))

import meteostat
import pandas as pd
import datetime
from src.config import *
import warnings

warnings.filterwarnings("ignore")

normalise_dict = {
    "Lima Airport": ["Lima I.A.", 8801, "WS"],
    "Ica": ["Ica", 8802, "WS"],
    "Callao": ["Callao", 0, "WS"],
    "Lima": ["Lima coast", 26, "S"],
    "Puno": ["Puno", 386, "S"],
    "Cajamarca ": ["Cajamarca", 143, "W"],
    "Trujillo Port": ["Trujillo Port", 413, "S"],
    "Cuzco": ["Cuzco", 18, "W"],
    "Loreto": ["Loreto", 111, "S"],
}


def retrieve_hourly_and_daily_data():
    stations_df = get_available_uae_stations()
    stations_id = stations_df["id"].to_list()
    start, end = get_datetimes_to_extract()

    # Hourly Data
    df = meteostat.Hourly(stations_id, start=start, end=end)
    df = df.fetch().reset_index().rename(columns={"station": "id"})
    df = (
        stations_df[["id", "name"]].merge(df, how="right", on="id").drop(columns=["id"])
    )
    df = df[["name", "time", "temp", "rhum", "wspd"]].rename(
        columns={
            "name": "weather_station",
            "time": "datetime",
            "temp": "temperature",
            "rhum": "humidity",
            "wspd": "wind_speed",
        }
    )
    df = pd.melt(
        df,
        id_vars=["datetime", "weather_station"],
        value_vars=["temperature", "humidity", "wind_speed"],
        var_name="weather_variable",
        value_name="nominal",
    )
    df["granularity"] = "hourly"
    df["station_id"] = df["weather_station"].apply(lambda x: normalise_dict[x][1])
    df["weather_station"] = df["weather_station"].apply(lambda x: normalise_dict[x][0])
    if "meteostat_hourly_actuals.parquet" in os.listdir(OUTPUTS_DIR):
        aux = pd.read_parquet(
            os.path.join(OUTPUTS_DIR, "meteostat_hourly_actuals.parquet")
        )
        max_date = aux["datetime"].max()
        df = df[df["datetime"] > max_date]
        df = pd.concat([df, aux])
        df.to_parquet(os.path.join(OUTPUTS_DIR, "meteostat_hourly_actuals.parquet"))
        df.to_csv(os.path.join(OUTPUTS_DIR, "meteostat_hourly_actuals.csv"))
    else:
        df.to_parquet(os.path.join(OUTPUTS_DIR, "meteostat_hourly_actuals.parquet"))
        df.to_csv(os.path.join(OUTPUTS_DIR, "meteostat_hourly_actuals.csv"))
    print("Retrieved Hourly data from meteostat")

    # Daily Data
    df2 = meteostat.Daily(stations_id, start=start, end=end)
    df2 = df2.fetch().reset_index().rename(columns={"station": "id"})
    df2 = (
        stations_df[["id", "name"]]
        .merge(df2, how="right", on="id")
        .drop(columns=["id"])
    )
    df2 = df2[["name", "time", "prcp"]].rename(
        columns={"name": "weather_station", "time": "datetime", "prcp": "rain"}
    )
    df2 = pd.melt(
        df2,
        id_vars=["datetime", "weather_station"],
        value_vars=["rain"],
        var_name="weather_variable",
        value_name="nominal",
    )
    df2["granularity"] = "daily"
    df2["station_id"] = df2["weather_station"].apply(lambda x: normalise_dict[x][1])
    df2["weather_station"] = df2["weather_station"].apply(
        lambda x: normalise_dict[x][0]
    )
    if "meteostat_daily_actuals.parquet" in os.listdir(OUTPUTS_DIR):
        aux = pd.read_parquet(
            os.path.join(OUTPUTS_DIR, "meteostat_daily_actuals.parquet")
        )
        max_date = aux["datetime"].max()
        df2 = df2[df2["datetime"] > max_date]
        df2 = pd.concat([df2, aux])
        df2.to_parquet(os.path.join(OUTPUTS_DIR, "meteostat_daily_actuals.parquet"))
        df2.to_csv(os.path.join(OUTPUTS_DIR, "meteostat_daily_actuals.csv"))
    else:
        df2.to_parquet(os.path.join(OUTPUTS_DIR, "meteostat_daily_actuals.parquet"))
        df2.to_csv(os.path.join(OUTPUTS_DIR, "meteostat_daily_actuals.csv"))
    print("Retrieved Daily data from meteostat")


def merge_meteostat_data():
    df = pd.read_parquet(os.path.join(OUTPUTS_DIR, "meteostat_hourly_actuals.parquet"))
    df2 = pd.read_parquet(os.path.join(OUTPUTS_DIR, "meteostat_daily_actuals.parquet"))
    df = pd.concat([df, df2])
    df.to_csv(os.path.join(COMPILED_OUTPUTS_DIR, "meteostat_actuals.csv"))
    df.to_parquet(os.path.join(COMPILED_OUTPUTS_DIR, "meteostat_actuals.parquet"))
    print("Merged meteostat actuals tables")


def get_available_uae_stations():
    stations_df = meteostat.Stations().region(country="PE").fetch().dropna()
    stations_df = stations_df.drop(
        columns=["wmo", "icao", "monthly_start", "monthly_end"]
    ).reset_index()
    stations_df.to_csv(os.path.join(OUTPUTS_DIR, "meteostat_stations.csv"))
    return stations_df


def get_datetimes_to_extract():
    today = datetime.datetime.today()
    end = datetime.datetime(
        today.year, today.month, today.day, today.hour, today.minute
    )
    if "meteostat_hourly_actuals.parquet" in os.listdir(OUTPUTS_DIR):
        start = pd.read_parquet(
            os.path.join(OUTPUTS_DIR, "meteostat_hourly_actuals.parquet")
        )["datetime"].max()
        start = datetime.datetime(
            start.year, start.month, start.day, start.hour, start.minute
        )
    else:
        start = datetime.datetime(2015, 1, 1, 00, 00)
    return (start, end)


if __name__ == "__main__":
    retrieve_hourly_and_daily_data()
    merge_meteostat_data()
    # Capture the list of modules after user imports
    final_modules = set(sys.modules.keys())

    # Determine which modules were added
    user_imported_modules = final_modules - initial_modules

    # Filter out standard library modules
    user_imported_modules = {
        name
        for name in user_imported_modules
        if name in pkg_resources.working_set.by_key
    }

    # Get the versions of the imported modules
    versions = {
        pkg.key: pkg.version
        for pkg in pkg_resources.working_set
        if pkg.key in user_imported_modules
    }

    # Write these modules to a requirements.txt file
    with open(os.path.join(BASE_DIR, "requirements.txt"), "w") as f:
        for module in user_imported_modules:
            f.write(f"{module}\n")

    print("User Imported Modules:", user_imported_modules)
