import os

BASE_DIR = os.path.abspath(
    os.path.join(os.path.dirname(os.path.abspath(__file__)), "..")
)
OUTPUTS_DIR = os.path.join(BASE_DIR, "outputs")
COMPILED_OUTPUTS_DIR = os.path.join(BASE_DIR, "compiled_outputs")
NOTEBOOKS_DIR = os.path.join(BASE_DIR, "notebooks")
DATA_EXTRA_DIR = os.path.join(BASE_DIR, "data_extra")
PROJECTS_DIR = os.path.abspath(
    os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..")
)
DEMANDA_DIR = os.path.join(PROJECTS_DIR, "demanda_data", "outputs")
DEMANDA_GEO_DIR = os.path.join(PROJECTS_DIR, "demanda_geo", "outputs")
GEO_DIR = os.path.join(
    PROJECTS_DIR, "arcgis_api_data_parsing_double_authentication", "outputs"
)
MINERIA_CHICA_DIR = os.path.join(
    PROJECTS_DIR, "parsing_wrongly_formatted_excels", "outputs"
)
