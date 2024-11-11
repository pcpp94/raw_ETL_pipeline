import pandas as pd
import os
from typing import Literal
from dataclasses import dataclass

from .. import etl_mapper
from ..config import DATA_EXTRA_DIR


@dataclass
class Region_Mapper:
    """
    Class object to Map Supply Points (Regiones).
    We are using the following tables from the GEO: subestacion, LineasAltas, UnionLineas
    """

    def etl_mapper_lima(self):
        self.substations, self.substations_400kv, self.ss_to_loop = (
            etl_mapper.get_substations(departamento="lima")
        )
        self.lines_source_destination = etl_mapper.lines_connectivity()
        self.ad_links_count_geo_only = etl_mapper.three_levels_iteration_mapping(
            self.lines_source_destination, self.ss_to_loop, self.substations_400kv
        )
        self.ad_links = etl_mapper.manual_tweaks_and_others(
            self.ad_links_count_geo_only
        )
        self.ad_links.to_csv(os.path.join(DATA_EXTRA_DIR, "lima_mapping.csv"))
        print("Saved Lima Mapping csv file.")

    def etl_mapper_provincia(self):
        self.substations, self.substations_400kv, self.ss_to_loop = (
            etl_mapper.get_substations(departamento="prov")
        )  ## It's not all 400kV for the Provincias, variable name is still 40kV
        self.lines_source_destination = etl_mapper.lines_connectivity()
        self.ne_links = etl_mapper.three_levels_iteration_mapping_wo_400kv(
            self.lines_source_destination,
            self.ss_to_loop,
            self.substations_400kv,
            self.substations,
        )
        self.ne_links.to_csv(os.path.join(DATA_EXTRA_DIR, "prov_mapping.csv"))
        print("Saved Provincias Mapping csv file.")
