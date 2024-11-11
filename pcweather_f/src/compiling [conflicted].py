import os
import pandas as pd
import datetime

from .config import (
    COMPILED_OUTPUTS_DIR,
    OUTPUTS_DIR,
    DATA_DIR,
    normalise_dict,
    normalise_dict_fc,
    MISSING_DIR,
)

actuals_data = os.path.join(DATA_DIR, "pcweather_f_actuals.csv")
missing_data1 = os.path.join(
    MISSING_DIR, "hourly_weather_half_hourly_weather_stations_formatted.parquet"
)
missing_data2 = os.path.join(
    MISSING_DIR, "half_hourly_weather_stations_formatted.parquet"
)
missing_data3 = os.path.join(MISSING_DIR, "pcweather_f_forecasts_archive_2.parquet")
forecast_data = os.path.join(DATA_DIR, "pcweather_f_forecasts.csv")
actuals_api = [
    os.path.join(OUTPUTS_DIR, file)
    for file in os.listdir(OUTPUTS_DIR)
    if file.startswith("actual")
]
forecast_api = [
    os.path.join(OUTPUTS_DIR, file)
    for file in os.listdir(OUTPUTS_DIR)
    if file.startswith("FC")
]


def compiling_all_actuals():
    dtype = {
        "year": int,
        "month": int,
        "day": int,
        "hour": int,
        "minute": int,
        "station": str,
        "excel filename": str,
        "excel sheet": str,
        "variables": str,
        "nominal": float,
        "type": str,
    }
    df = (
        pd.read_csv(actuals_data, index_col=0, parse_dates=["datetime"], dtype=dtype)
        .drop(
            columns=[
                "excel filename",
                "excel sheet",
                "year",
                "month",
                "day",
                "hour",
                "minute",
            ]
        )
        .rename(columns={"station": "weather_station", "variables": "weather_variable"})
    )
    df["date"] = pd.to_datetime(df["datetime"].dt.date)
    df["hour"] = df["datetime"].dt.time.astype(str).str.slice(stop=5)
    df["station_id"] = df["weather_station"].apply(lambda x: normalise_dict[x][1])
    df["weather_station"] = df["weather_station"].apply(lambda x: normalise_dict[x][0])

    df_2 = pd.DataFrame()
    cols = [
        "datetime",
        "date",
        "hour",
        "type",
        "weather_station",
        "station_id",
        "weather_variable",
        "nominal",
    ]
    dtype_2 = {
        "hour": str,
        "type": str,
        "weather_station": str,
        "station_id": int,
        "weather_variable": str,
        "nominal": float,
    }
    for file in actuals_api:
        aux = pd.read_csv(
            file, usecols=cols, parse_dates=["datetime", "date"], dtype=dtype_2
        )
        df_2 = pd.concat([df_2, aux]).reset_index(drop=True)
    df_2["weather_station"] = df_2["weather_station"].apply(
        lambda x: "Lima I.A." if x == "Jorge Chavez I.A." else x
    )
    df_2 = df_2.drop_duplicates()

    df = pd.concat([df, df_2]).drop_duplicates().reset_index(drop=True)
    aux1 = pd.read_parquet(missing_data1)
    aux2 = pd.read_parquet(missing_data2)
    df_3 = pd.concat([aux1, aux2]).reset_index(drop=True)
    df_3["hour"] = df_3["hour"].apply(lambda x: ":".join(str(x).split(":")[:-1]))
    df = pd.concat([df, df_3]).drop_duplicates()
    df = (
        df.groupby(
            by=[
                "datetime",
                "date",
                "hour",
                "weather_station",
                "weather_variable",
                "type",
                "station_id",
            ]
        )["nominal"]
        .mean()
        .reset_index()
    )
    df = df[~df["nominal"].isna()]

    df.to_csv(os.path.join(COMPILED_OUTPUTS_DIR, "pcweather_f_weather_actuals.csv"))
    df.to_parquet(os.path.join(COMPILED_OUTPUTS_DIR, "pcweather_f_weather_actuals.parquet"))


def compiling_all_forecasts():
    dtype = {
        "hour": str,
        "type": str,
        "weather_station": str,
        "station_id": int,
        "weather_variable": str,
        "nominal": float,
    }
    df = (
        pd.read_csv(
            forecast_data,
            index_col=0,
            parse_dates=["datetime", "date", "file_date"],
            dtype=dtype,
        )
        .drop(columns=["last_forecast"])
        .rename(columns={"file_date": "file_datetime"})
    )
    df["weather_station"] = df["weather_station"].apply(
        lambda x: normalise_dict_fc[x][0]
    )

    df_2 = pd.DataFrame()
    dtype_2 = {
        "hour": str,
        "type": str,
        "weather_station": str,
        "station_id": int,
        "weather_variable": str,
        "nominal": float,
    }
    for file in forecast_api:
        try:
            aux = pd.read_csv(
                file, index_col=0, parse_dates=["datetime", "date", "file_datetime"]
            ).drop(columns=["weather_station_api"])
        except:
            aux = pd.read_csv(
                file, index_col=0, parse_dates=["datetime", "date", "file_datetime"]
            )
        aux["weather_variable"] = aux["weather_variable"].str.lower()
        if ("temp" in aux["weather_variable"].unique()) or (
            "rh" in aux["weather_variable"].unique()
        ):
            aux["weather_variable"] = aux["weather_variable"].map(
                {"temp": "temperature", "rh": "humidity", "rain": "rain"}
            )
        df_2 = pd.concat([df_2, aux]).reset_index(drop=True)
    df_2["weather_station"] = df_2["weather_station"].apply(
        lambda x: "Lima I.A." if x == "Jorge Chavez I.A." else x
    )
    try:
        df_2 = df_2.drop(columns="id")
    except:
        pass
    df_2 = df_2.drop_duplicates()
    df = pd.concat([df, df_2]).drop_duplicates().reset_index(drop=True)
    current_files = df["file_datetime"].dt.date.unique().tolist()

    df_3 = pd.DataFrame()
    aux = pd.read_parquet(missing_data3).drop(columns=["file"])
    for weather in aux["weather_variable"].unique():
        aux1 = aux[aux["weather_variable"] == weather].copy()
        aux1 = aux1[~aux1["nominal"].isna()]
        df_3 = pd.concat([df_3, aux1])
    df_3 = df_3.reset_index(drop=True)
    archives = df_3["file_datetime"].dt.date.unique().tolist()
    to_add = [x for x in archives if x not in current_files]
    df_3 = df_3[df_3["file_datetime"].dt.date.isin(to_add)]

    df = pd.concat([df, df_3]).drop_duplicates().reset_index(drop=True)

    df["hour"] = df["hour"].str.replace(":03", ":30")
    df["datetime"] = pd.to_datetime(
        df["date"].astype(str) + " " + df["hour"].astype(str), format="%Y-%m-%d %H:%M"
    )
    df = df.drop_duplicates()

    last_fc = (
        df.groupby(by=["datetime", "type", "weather_station", "weather_variable"])
        .max()[["file_datetime"]]
        .reset_index()
        .rename(columns={"file_datetime": "last_file_date"})
    )
    df = df.merge(
        last_fc,
        how="left",
        on=["datetime", "type", "weather_station", "weather_variable"],
    )
    df["last_fc"] = df["file_datetime"] == df["last_file_date"]
    df = df.drop(columns=["last_file_date"])
    station_ids_temp = dict(
        df[~df["station_id"].isna()][["weather_station", "station_id"]]
        .drop_duplicates()
        .values
    )
    df["station_id"] = df["weather_station"].map(station_ids_temp)
    df["id"] = (
        df["weather_station"]
        + df["weather_variable"]
        + df["datetime"].astype(str)
        + df["nominal"].astype(str)
    )
    df = (
        df.groupby(
            by=[
                "id",
                "datetime",
                "date",
                "hour",
                "type",
                "weather_station",
                "weather_variable",
                "nominal",
                "station_id",
                "last_fc",
            ]
        )["file_datetime"]
        .max()
        .reset_index()
        .drop(columns="id")
    )

    df["forecast_recency"] = (
        df.groupby(
            ["datetime", "date", "hour", "type", "weather_station", "weather_variable"]
        )["file_datetime"]
        .rank(method="dense", ascending=False)
        .astype(int)
    )
    df["forecast_days_ap"] = df["datetime"].dt.date - df["file_datetime"].dt.date
    df["forecast_days_ap"] = df["forecast_days_ap"].apply(
        lambda x: x.days if x.days >= 0 else 0
    )
    df.to_csv(os.path.join(COMPILED_OUTPUTS_DIR, "pcweather_f_weather_forecasts.csv"))
    df.to_parquet(os.path.join(COMPILED_OUTPUTS_DIR, "pcweather_f_weather_forecasts.parquet"))


def compile_all():
    compiling_all_actuals()
    compiling_all_forecasts()
    print("Compiled completed.")


if __name__ == "__main__":
    compile_all()
