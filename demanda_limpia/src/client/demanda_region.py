import pandas as pd
import os
from typing import Literal
from dataclasses import dataclass

from .. import etl_merging
from ..config import COMPILED_OUTPUTS_DIR


@dataclass
class Demanda_Regiones_Client:
    """
    Class object to Map Supply Points (Regiones) and get the demanda data by:
        - All locational parameters: Region, Region, Coordinates, etc.
        - selva_sur GEO Portal parameters: Substation official name, location ID, etc.
    """

    fuentes = [
        "costa_centro",
        "costa_norte",
        "mineria",
        "costa_sur",
        "sierra_sur",
        "selva_norte",
        "selva_sur",
    ]
    fuente_no_geo = "mineria"
    extra_fuentes = ["minera_chica"]

    def etl_merge_region_by_geo(self):
        for fuente in self.fuentes:
            etl_merging.add_region_codes_to_demanda(demanda_flow=fuente)
        etl_merging.add_region_codes_to_minera_chica()
        print("Finished merging Regiones by GEO")

    def etl_compile_all_fuentes(self):
        etl_merging.compile_fuentes()
        etl_merging.reduced_versions()
        print("Finished saving the compiled tables by GEO, Regiones, etc.")
