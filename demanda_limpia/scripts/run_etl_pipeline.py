import sys
import os

sys.path.insert(0, "\\".join(os.path.dirname(__file__).split("\\")[:-1]))

from src.client.region_mapper import Region_Mapper
from src.client.demanda_region import Demanda_Regiones_Client
from src.utils import get_files_needing_update


def run_etl():
    run_region_mapper()
    run_data_merging()


def run_region_mapper():
    client = Region_Mapper()
    client.etl_mapper_lima()
    client.etl_mapper_provincia()
    print(f"Finished with Region Mapping")


def run_data_merging():
    client = Demanda_Regiones_Client()
    client.etl_merge_region_by_geo()
    client.etl_compile_all_fuentes()
    print("Finished merging region data with geo data.")


if __name__ == "__main__":
    if len(get_files_needing_update()) > 0:
        run_etl()
    else:
        print(
            "'demanda_regions' table output is newer than all inputs from 'demanda_geo'"
        )
