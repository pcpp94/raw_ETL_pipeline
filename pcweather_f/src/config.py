import os

BASE_DIR = os.path.abspath(
    os.path.join(os.path.dirname(os.path.abspath(__file__)), "..")
)
DATA_DIR = os.path.join(BASE_DIR, "data")
MISSING_DIR = os.path.join(BASE_DIR, "missing_2023")
OUTPUTS_DIR = os.path.join(BASE_DIR, "outputs")
COMPILED_OUTPUTS_DIR = os.path.join(BASE_DIR, "compiled_outputs")
NOTEBOOKS_DIR = os.path.join(BASE_DIR, "notebooks")

project_forecasts = {
    "daily_Trujillo": "daily_weather_stations",
    "daily_Lima": "daily_weather_stations",
    "daily_Lima": "daily_weather_stations",
    "daily_Ica": "daily_weather_stations",
    "daily_Cajamarca": "daily_weather_stations",
    "daily_Cuzco": "daily_weather_stations",
    "hourly_Lima": "hourly_weather_stations",
    "hourly_Ica": "hourly_weather_stations",
    "15mins_Puno": "half_hourly_weather_stations",
    "15mins_Cajamarca": "half_hourly_weather_stations",
    "15mins_Trujillo": "half_hourly_weather_stations",
    "15mins_Cuzco": "half_hourly_weather_stations",
    "15mins_Loreto": "half_hourly_weather_stations",
}

location_dict = {
    "daily_Trujillo": ["Trujillo Port", "Trujillo", 413],
    "daily_Lima": ["Lima I.A.", "Lima", 8801],
    "daily_Lima": ["Lima coast", "Lima", 26],
    "daily_Ica": ["Ica", "Ica", 8802],
    "daily_Cajamarca": ["Cajamarca", "Cajamarca", 143],
    "daily_Cuzco": ["Cuzco", "Cuzco", 18],
    "hourly_Lima": ["Lima I.A.", "Lima", 8801],
    "hourly_Ica": ["Ica", "Ica", 8802],
    "15mins_Puno": ["Puno", "Puno", 386],
    "15mins_Cajamarca": ["Cajamarca", "Cajamarca", 143],
    "15mins_Trujillo": ["Trujillo Port", "Trujillo", 413],
    "15mins_Cuzco": ["Cuzco", "Cuzco", 18],
    "15mins_Loreto": ["Loreto", "Loreto", 111],
}

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

normalise_dict_fc = {
    "Lima": ["Lima I.A.", 8801, "WS"],
    "Ica": ["Ica", 8802, "WS"],
    "Lima": ["Lima coast", 26, "S"],
    "Puno": ["Puno", 386, "S"],
    "Cajamarca": ["Cajamarca", 143, "W"],
    "Trujillo": ["Trujillo Port", 413, "S"],
    "Cuzco": ["Cuzco", 18, "W"],
    "Loreto": ["Loreto", 111, "S"],
}
