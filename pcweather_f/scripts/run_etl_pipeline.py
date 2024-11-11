import os
import warnings
import sys

sys.path.insert(0, "\\".join(os.path.dirname(__file__).split("\\")[:-1]))
warnings.filterwarnings("ignore")

import pandas as pd
from src.pcweather_f_client import pcweather_f_Client, get_datetime_string
from src.config import OUTPUTS_DIR, COMPILED_OUTPUTS_DIR
import warnings

warnings.filterwarnings("ignore")


def run_etl():
    # Get API data for actuals and forecasts
    pcweather_f_client = pcweather_f_Client()
    actuals, forecasts = pcweather_f_client.api_get_all()
    actuals["id"] = (
        actuals["weather_station"]
        + actuals["weather_variable"]
        + actuals["datetime"].astype(str)
    )
    forecasts["weather_variable"] = forecasts["weather_variable"].str.lower()
    forecasts["weather_variable"] = forecasts["weather_variable"].map(
        {"temp": "temperature", "rh": "humidity", "rain": "rain", "ws": "wind_speed"}
    )
    forecasts["id"] = (
        forecasts["weather_station"]
        + forecasts["weather_variable"]
        + forecasts["datetime"].astype(str)
        + forecasts["nominal"].astype(str)
    )
    forecasts = forecasts[
        [
            "datetime",
            "date",
            "hour",
            "file_datetime",
            "type",
            "weather_station",
            "weather_variable",
            "nominal",
            "station_id",
            "id",
        ]
    ]

    # Actuals appending and saving:
    current_actuals = pd.read_parquet(
        os.path.join(COMPILED_OUTPUTS_DIR, "pcweather_f_weather_actuals.parquet")
    )
    current_actuals["id"] = (
        current_actuals["weather_station"]
        + current_actuals["weather_variable"]
        + current_actuals["datetime"].astype(str)
    )
    new_actuals = actuals[~actuals["id"].isin(current_actuals["id"])].copy()
    filename = (
        "actual_data_" + get_datetime_string(actuals["file_datetime"][0]) + ".csv"
    )
    new_actuals.to_csv(os.path.join(OUTPUTS_DIR, filename))
    current_actuals = pd.concat(
        [current_actuals, new_actuals.drop(columns=["file_datetime"])]
    )
    current_actuals = current_actuals.drop(columns="id").reset_index(drop=True)
    current_actuals["date"] = pd.to_datetime(current_actuals["date"], format="%Y-%m-%d")
    current_actuals = current_actuals[~current_actuals["nominal"].isna()]
    current_actuals.to_csv(
        os.path.join(COMPILED_OUTPUTS_DIR, "pcweather_f_weather_actuals.csv")
    )
    current_actuals.to_parquet(
        os.path.join(COMPILED_OUTPUTS_DIR, "pcweather_f_weather_actuals.parquet")
    )

    # Forecasts appending and saving:
    current_forecasts = pd.read_parquet(
        os.path.join(COMPILED_OUTPUTS_DIR, "pcweather_f_weather_forecasts.parquet")
    )[
        [
            "datetime",
            "date",
            "hour",
            "file_datetime",
            "type",
            "weather_station",
            "weather_variable",
            "nominal",
            "station_id",
        ]
    ]
    current_forecasts["id"] = (
        current_forecasts["weather_station"]
        + current_forecasts["weather_variable"]
        + current_forecasts["datetime"].astype(str)
        + current_forecasts["nominal"].astype(str)
    )
    new_forecasts = forecasts[~forecasts["id"].isin(current_forecasts["id"])].copy()
    new_forecasts["hour"] = new_forecasts["hour"].str.replace(":03", ":30")
    new_forecasts["datetime"] = pd.to_datetime(
        new_forecasts["date"].astype(str) + " " + new_forecasts["hour"].astype(str),
        format="%Y-%m-%d %H:%M",
    )
    filename = (
        "forecast_data_" + get_datetime_string(forecasts["file_datetime"][0]) + ".csv"
    )
    new_forecasts.to_csv(os.path.join(OUTPUTS_DIR, filename))
    current_forecasts = pd.concat([current_forecasts, new_forecasts])
    last_fc = (
        current_forecasts.groupby(
            by=["datetime", "type", "weather_station", "weather_variable"]
        )
        .max()[["file_datetime"]]
        .reset_index()
        .rename(columns={"file_datetime": "last_file_date"})
    )
    current_forecasts = current_forecasts.merge(
        last_fc,
        how="left",
        on=["datetime", "type", "weather_station", "weather_variable"],
    )
    current_forecasts["last_fc"] = (
        current_forecasts["file_datetime"] == current_forecasts["last_file_date"]
    )
    current_forecasts = current_forecasts.drop(
        columns=["last_file_date", "id"]
    ).reset_index(drop=True)
    current_forecasts["date"] = pd.to_datetime(
        current_forecasts["date"], format="%Y-%m-%d"
    )
    current_forecasts["forecast_recency"] = (
        current_forecasts.groupby(
            ["datetime", "date", "hour", "type", "weather_station", "weather_variable"]
        )["file_datetime"]
        .rank(method="dense", ascending=False)
        .astype(int)
    )
    current_forecasts["forecast_days_ap"] = (
        current_forecasts["datetime"].dt.date
        - current_forecasts["file_datetime"].dt.date
    )
    current_forecasts["forecast_days_ap"] = current_forecasts["forecast_days_ap"].apply(
        lambda x: x.days if x.days >= 0 else 0
    )

    current_forecasts.to_csv(
        os.path.join(COMPILED_OUTPUTS_DIR, "pcweather_f_weather_forecasts.csv")
    )
    current_forecasts.to_parquet(
        os.path.join(COMPILED_OUTPUTS_DIR, "pcweather_f_weather_forecasts.parquet")
    )


if __name__ == "__main__":
    run_etl()
