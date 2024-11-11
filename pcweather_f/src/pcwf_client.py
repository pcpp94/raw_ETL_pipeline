import requests
import os
import pandas as pd
import datetime

from .config import OUTPUTS_DIR, DATA_DIR, project_forecasts, location_dict


variables = {
    "hourly_weather_stations": ["TEMP", "RH", "WS"],
    "daily_weather_stations": ["RAIN"],
    "half_hourly_weather_stations": ["TEMP", "RH", "WS"],
}


def get_datetime_string(timestamp):
    date = str(timestamp.date()).replace("-", "_")
    hour_min = str(timestamp.hour) + "_" + str(timestamp.minute)
    date_time = date + "_" + hour_min
    return date_time


class pcweather_f_Client:
    """
    Class to query the pcweather_f API.
    """

    def __init__(
        self,
        base_url="https://api.pcweather_f.io",
        airports_url="https://api.pcweather_f.io/v2/json/airports/data/24h/get/",
        stations_url="https://api.pcweather_f.io/v2/json/aws/data/24h/get/",
        forecasts_url="https://api.pcweather_f.io/v2/json/forecast/get?station=",
        headers={
            "TOKEN": "111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111"
        },
    ):
        self.base_url = base_url
        self.airports_url = airports_url
        self.stations_url = stations_url
        self.forecasts_url = forecasts_url
        self.headers = headers
        self.stations_df = self.get_stations_info()

    def get_stations_info(self):
        df = pd.read_csv(os.path.join(DATA_DIR, "pcweather_f_api_stations_info.csv"))
        return df

    def api_get_all_airports_actuals(self):
        df = pd.DataFrame()
        for id_ in self.stations_df[self.stations_df["type"] == "Airports"][
            "station_id"
        ].tolist():
            aux_df = self.api_get_airport_actual(id_=id_)
            df = pd.concat([df, aux_df])
        return df

    def api_get_airport_actual(self, id_: int = None):

        if id_ == None:
            return
        stations = self.stations_df[self.stations_df["station_id"] == id_].reset_index(
            drop=True
        )
        # data = requests.get(self.airports_url + str(id_), headers=self.headers, verify=False).json()
        data = requests.get(self.airports_url + str(id_), headers=self.headers).json()
        df = pd.DataFrame(data["pcweather_fS"]["AIRPORT"]["DATA"])
        df["station_id"] = stations.loc[0, "station_id"]
        df["weather_station"] = stations.loc[0, "name_en"]
        df["type"] = "hourly_weather_stations"
        df["DATE_TIME"] = pd.to_datetime(df["DATE_TIME"], format="%Y-%m-%d %X")
        df["TEMPERATURE"] = pd.to_numeric(df["TEMPERATURE"])
        df["station_id"] = pd.to_numeric(df["station_id"])
        df.columns = df.columns.str.lower()
        df = df.rename(columns={"date_time": "datetime"})
        df = pd.melt(
            df,
            id_vars=["datetime", "type", "weather_station", "station_id"],
            value_vars=["temperature", "humidity"],
            var_name="weather_variable",
            value_name="nominal",
        )
        return df

    def api_get_all_stations_actuals(self):
        df = pd.DataFrame()
        for id_ in self.stations_df[self.stations_df["type"] == "HOURLY"][
            "station_id"
        ].tolist():
            aux_df = self.api_get_station_actual(id_=id_)
            df = pd.concat([df, aux_df])
        return df

    def api_get_station_actual(self, id_: int = None):

        if id_ == None:
            return
        stations = self.stations_df[self.stations_df["station_id"] == id_].reset_index(
            drop=True
        )
        # data = requests.get(self.stations_url + str(id_), headers=self.headers, verify=False).json()
        data = requests.get(self.stations_url + str(id_), headers=self.headers).json()
        df = pd.DataFrame(data["pcweather_fS"]["HOURLY"]["DATA"])
        df["station_id"] = stations.loc[0, "station_id"]
        df["weather_station"] = stations.loc[0, "name_en"]
        df["type"] = "half_hourly_weather_stations"
        df["DATE_TIME"] = pd.to_datetime(df["DATE_TIME"], format="%Y-%m-%d %X")
        df["DRY_TEMPERATURE"] = pd.to_numeric(df["DRY_TEMPERATURE"])
        df["RELATIVE_HUMIDITY"] = pd.to_numeric(df["RELATIVE_HUMIDITY"])
        df["station_id"] = pd.to_numeric(df["station_id"])
        df.columns = df.columns.str.lower()
        df = df.rename(
            columns={
                "date_time": "datetime",
                "dry_temperature": "temperature",
                "relative_humidity": "humidity",
            }
        )
        df = pd.melt(
            df,
            id_vars=["datetime", "type", "weather_station", "station_id"],
            value_vars=["temperature", "humidity"],
            var_name="weather_variable",
            value_name="nominal",
        )
        return df

    def api_get_actuals(self):

        airports = self.api_get_all_airports_actuals()
        stations = self.api_get_all_stations_actuals()
        df = pd.concat([airports, stations]).reset_index(drop=True)
        df["file_datetime"] = datetime.datetime.today().replace(second=0, microsecond=0)
        df["date"] = df["datetime"].dt.date
        df["hour"] = (df["datetime"].dt.time).astype(str).str.slice(stop=5)
        return df

    def api_get_forecasts(self):

        df = pd.DataFrame()

        for service in list(project_forecasts.keys()):
            # df_aux = requests.get(self.forecasts_url + service, headers=self.headers, verify=False).json()
            df_aux = requests.get(
                self.forecasts_url + service, headers=self.headers
            ).json()
            df_aux = pd.DataFrame(df_aux["pcweather_fS"]["FC"]["DATA"])
            df_aux["datetime"] = pd.to_datetime(
                df_aux["DATE"] + " " + df_aux["HOUR"], format="%d/%m/%Y %H:%M"
            )
            df_aux["DATE"] = pd.to_datetime(df_aux["DATE"], format="%d/%m/%Y")
            type = project_forecasts[service]
            ws_name_clean = location_dict[service][0]
            ws_name_short = location_dict[service][1]
            station_id = location_dict[service][2]
            values = variables[type]
            df_aux = pd.melt(
                df_aux,
                id_vars=["datetime", "DATE", "HOUR"],
                value_vars=values,
                var_name="weather_variable",
                value_name="nominal",
            )
            df_aux["type"] = type
            df_aux["weather_station"] = ws_name_clean
            df_aux["weather_station_api"] = ws_name_short
            df_aux["station_id"] = station_id
            df = pd.concat([df, df_aux])
        df = df.reset_index(drop=True)
        df.columns = df.columns.str.lower()
        df["file_datetime"] = datetime.datetime.today().replace(second=0, microsecond=0)
        return df

    def api_get_all(self):
        actuals = self.api_get_actuals()
        forecasts = self.api_get_forecasts()
        return actuals, forecasts

    def api_download_all(self):
        df = self.api_get_actuals().reset_index(drop=True)
        filename = "actual_data_" + get_datetime_string(df["file_datetime"][0]) + ".csv"
        df.to_csv(os.path.join(OUTPUTS_DIR, filename))
        df = self.api_get_forecasts().reset_index(drop=True)
        filename = (
            "forecast_data_" + get_datetime_string(df["file_datetime"][0]) + ".csv"
        )
        df.to_csv(os.path.join(OUTPUTS_DIR, filename))
