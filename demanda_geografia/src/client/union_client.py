import pandas as pd
import os
from typing import Literal
from dataclasses import dataclass


from .. import etl_functions
from ..config import OUTPUTS_DIR, GEO_OUTPUTS_DIR, DEMANDA_OUTPUTS_DIR


@dataclass
class Demanda_GEO_Client:
    """
    Object to perform the Merging of Demanda Data with the GEO data.
    """

    fuente: Literal[
        "costa_centro",
        "costa_norte",
        "sierra_norte",
        "costa_sur",
        "sierra_sur",
        "selva_norte",
        "selva_sur",
    ]

    def __post_init__(self):
        self.geo_files = etl_functions.get_geo_files(
            os.path.join(GEO_OUTPUTS_DIR, "selva_sur", "final")
        )
        self.geo_df = etl_functions.get_geo_df(self.geo_files, self.fuente)
        self.demanda_files = etl_functions.get_demanda_files(DEMANDA_OUTPUTS_DIR)
        self.demanda_df, self.fuente_variables, self.fuente_dict = (
            etl_functions.get_demanda_df(self.demanda_files, self.fuente, self.geo_df)
        )
        print("Demanda_GEO_Client initiated succesfully.")

    def etl_dict_available(self):
        self.dict_match, self.missing_match = etl_functions.dict_match(
            self.fuente_variables, self.fuente_dict
        )
        self.dict_fuzz_match = etl_functions.fuzz_match_dict(
            self.missing_match, self.fuente_dict, self.fuente
        )
        self.match_df = pd.concat([self.dict_match, self.dict_fuzz_match]).reset_index(
            drop=True
        )
        print("GEO Substation Dictionary Created and matched succesfully.")

    def etl_only_geo_available(self):
        self.dict_fuzz_match = etl_functions.fuzz_match_dict(
            self.fuente_variables, self.fuente_dict, self.fuente
        )
        self.match_df = self.dict_fuzz_match.reset_index(drop=True).copy()
        print("GEO Substation Dictionary Created and matched succesfully.")

    def etl_final_df(self):
        demanda_geo_df = etl_functions.merge_etl(
            self.demanda_df, self.match_df, self.geo_df, self.fuente
        )
        demanda_geo_df.to_csv(os.path.join(OUTPUTS_DIR, f"{self.fuente}_geo.csv"))
        demanda_geo_df.to_parquet(
            os.path.join(OUTPUTS_DIR, f"{self.fuente}_geo.parquet")
        )
        print(f"Demanda_GEO output files for {self.fuente} saved succesfully.")
