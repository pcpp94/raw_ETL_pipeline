import sys
import os

sys.path.insert(0, "\\".join(os.path.dirname(__file__).split("\\")[:-1]))
import warnings

warnings.filterwarnings("ignore")

fuentes = [
    "costa_centro",
    "costa_norte",
    "sierra_norte",
    "costa_sur",
    "sierra_sur",
    "selva_norte",
    "selva_sur",
]

from demanda_geografia.src.client.union_client import Demanda_GEO_Client
from src.etl_functions import get_files_needing_update


def run_etl(fuente):
    client = Demanda_GEO_Client(fuente=fuente)
    if fuente in ["costa_centro", "costa_norte"]:
        client.etl_dict_available()
    else:
        client.etl_only_geo_available()
    client.etl_final_df()
    print(f"Finished with {fuente}")


if __name__ == "__main__":
    for fuente in fuentes:
        if fuente in get_files_needing_update():
            run_etl(fuente=fuente)
        else:
            print(
                f"Dataflow {fuente}: 'demanda_geo' table output is newer than input from 'demanda_data'"
            )
