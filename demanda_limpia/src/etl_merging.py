import pandas as pd
import numpy as np
import geopandas as gpd
from shapely.geometry import Point
import os

from .config import (
    DEMANDA_GEO_DIR,
    DEMANDA_DIR,
    DATA_EXTRA_DIR,
    GEO_DIR,
    OUTPUTS_DIR,
    MINERIA_CHICA_DIR,
    COMPILED_OUTPUTS_DIR,
)


def compile_fuentes():
    input_files = [
        os.path.join(OUTPUTS_DIR, x)
        for x in os.listdir(OUTPUTS_DIR)
        if x[-4:] != ".csv"
    ]
    main_flows_df = pd.DataFrame()
    for file in input_files:
        aux = pd.read_parquet(file)
        if file == "minera_chica_geo_region.parquet":
            pass
        else:
            aux["demanda_stream"] = "_".join(os.path.split(file)[1].split("_")[:-2])
        main_flows_df = pd.concat([main_flows_df, aux])
    main_flows_df = (
        main_flows_df.groupby(
            by=[
                "date_time",
                "variables",
                "nombrecorto",
                "ubicacionid",
                "nombreestacion",
                "planningstatus",
                "region",
                "region_code",
                "region_name",
            ],
            dropna=False,
        )
        .agg(
            {
                "nominal": "mean",
                "x": "mean",
                "y": "mean",
                "demanda_stream": lambda x: "|".join(x),
                "excel_sheet": lambda x: "|".join(x),
                "excel_file": lambda x: "|".join(x),
            }
        )
        .reset_index()
    )

    main_flows_df["demanda_stream"] = main_flows_df["demanda_stream"].replace(
        "costa_norte|costa_centro", "costa_norte"
    )
    double = [x for x in main_flows_df["demanda_stream"].unique() if "|" in x]
    single = [x.split("|")[0] for x in double]
    mix = tuple(zip(double, single))
    for before, after in mix:
        main_flows_df["demanda_stream"] = main_flows_df["demanda_stream"].replace(
            before, after
        )

    main_flows_df = main_flows_df[~main_flows_df["date_time"].isna()]
    upper_date_limit = main_flows_df[main_flows_df["demanda_stream"] != "minera_chica"][
        "date_time"
    ].max()
    main_flows_df = main_flows_df[main_flows_df["date_time"] <= upper_date_limit]

    main_flows_df.to_parquet(
        os.path.join(COMPILED_OUTPUTS_DIR, "demanda_data_by_region.parquet")
    )
    main_flows_df.to_csv(
        os.path.join(COMPILED_OUTPUTS_DIR, "demanda_data_by_region.csv"), index=False
    )
    print("Saved Compiled Demanda Data by GEO, Region, and more.")


def reduced_versions():
    df = pd.read_parquet(
        os.path.join(COMPILED_OUTPUTS_DIR, "demanda_data_by_region.parquet")
    )
    df[
        [
            "date_time",
            "variables",
            "nombrecorto",
            "ubicacionid",
            "nombreestacion",
            "region",
            "region_code",
            "region_name",
            "demanda_stream",
            "nominal",
        ]
    ].to_csv(
        os.path.join(COMPILED_OUTPUTS_DIR, "demanda_data_reduced.csv"), index=False
    )
    df.groupby(
        by=[
            "date_time",
            "nombrecorto",
            "nombreestacion",
            "region",
            "region_code",
            "region_name",
            "demanda_stream",
        ]
    )["nominal"].sum().reset_index().to_csv(
        os.path.join(COMPILED_OUTPUTS_DIR, "demanda_data_very_reduced.csv"),
        index=False,
    )
    df.groupby(by=["date_time", "variables"])["nominal"].sum().reset_index().pivot(
        index="date_time", columns="variables", values="nominal"
    ).to_csv(os.path.join(COMPILED_OUTPUTS_DIR, "datetime_variable_cols.csv"))
    print("Reduced versions saved.")


def add_region_codes_to_demanda(demanda_flow):

    ### Loading the Demanda-GEO table:
    if demanda_flow == "mineria":
        demanda_geo_df = pd.read_parquet(
            os.path.join(DEMANDA_DIR, f"{demanda_flow}_gold.parquet")
        )
    else:
        demanda_geo_df = pd.read_parquet(
            os.path.join(DEMANDA_GEO_DIR, f"{demanda_flow}_geo.parquet")
        )
    if "index" in demanda_geo_df.columns:
        demanda_geo_df = demanda_geo_df.drop(columns="index")
    if demanda_flow == "costa_centro":
        demanda_geo_df = demanda_geo_df[
            demanda_geo_df["nombrecorto"] != "mineria"
        ]  # mineria: There are Demanda Files for it specifically. It would be duplicating values if left in the costa_centro flow as well.
    if demanda_flow == "mineria":
        demanda_geo_df = demanda_geo_df[
            ~demanda_geo_df["variables"].str.contains("Billed by")
        ]
        demanda_geo_df = demanda_geo_df[demanda_geo_df["selva_sur_flow"] == "Export"]
        demanda_geo_df = demanda_geo_df.drop(columns="selva_sur_flow")
        demanda_geo_df["nombrecorto"] = "mineria"
        demanda_geo_df["nombreestacion"] = "mineria Export by selva_sur"
        demanda_geo_df["region_code"] = "mineria"
        demanda_geo_df["region_name"] = "Provincias Cobre"
        demanda_geo_df["ubicacionid"] = "mineria"
        demanda_geo_df["planningstatus"] = "PAC"
        demanda_geo_df["region"] = "mineria"
    if demanda_flow == "costa_sur":
        demanda_geo_df["region_code"] = "sur_costa"
        demanda_geo_df["region_name"] = "Colombia Electricity"
    if demanda_flow == "selva_norte":
        demanda_geo_df["region_code"] = "selva_norte"
        demanda_geo_df["region_name"] = "Iquitos Electricity Authority"
    if demanda_flow == "sierra_sur":
        demanda_geo_df["region_code"] = "sur_costa"
        demanda_geo_df["region_name"] = "Colombia Electricity"
    len_1 = len(demanda_geo_df)

    if demanda_flow in ["costa_centro", "costa_norte", "selva_sur"]:
        ### Loading the Substations to Regiones Mapping table: ('parent' is the Region Code // 'target' is the substation name from that region.)
        regions_mapped_df = pd.read_csv(
            os.path.join(DATA_EXTRA_DIR, "lima_mapping.csv"), index_col=0
        )
        regions_mapped_df["fullname"] = regions_mapped_df["fullname"].str.strip()
        regions_mapped_df["fullname"] = regions_mapped_df["fullname"].map(
            dict(
                zip(
                    regions_mapped_df["fullname"].unique().tolist(),
                    [
                        " ".join(x.split(" ")[:-1])
                        for x in regions_mapped_df["fullname"].unique().tolist()
                    ],
                )
            )
        )
        regions_mapped_df = regions_mapped_df.drop(columns=["region"]).rename(
            columns={
                "parent": "region_code",
                "fullname": "region_name",
                "target": "nombrecorto",
            }
        )

        ### Merging the mapping with the Demanda-GEO table.
        demanda_geo_df = demanda_geo_df.merge(
            regions_mapped_df, how="left", on="nombrecorto"
        )
        len_2 = len(demanda_geo_df)
        check = len_1 == len_2
        print(
            f"{demanda_flow}: All OK after merging demanda_geo and the lima_regions_mapping: {check}"
        )

    ### Ad-hoc tweaks to the GEO-Web "Station Name"  as it might be a bit confusing.
    if demanda_flow == "costa_centro":
        demanda_geo_df[demanda_geo_df["region_code"].isna()][
            "nombrecorto"
        ].unique()  # Power Plants.
        abbr_to_correct = ["mineria", "CUZK", "HAL", "HYDRO", "CHMB"]
        name_corrected = [
            "Provincias Aluminium",
            "Engie Solar",
            "Trujillo Red Station B",
            "Trujillo Red Station A",
            "CHMB",
        ]
        change = dict(zip(abbr_to_correct, name_corrected))
        for key, value in change.items():
            indeces = demanda_geo_df[demanda_geo_df["nombrecorto"] == key].index
            demanda_geo_df.loc[indeces, "nombreestacion"] = value
    if demanda_flow == "selva_sur":
        abbr_to_correct = ["mineria", "CUZK", "HAL", "HYDRO", "CHMB"]
        name_corrected = [
            "Provincias Aluminium",
            "Engie Solar",
            "Trujillo Red Station B",
            "Trujillo Red Station A",
            "CHMB",
        ]
        change = dict(zip(abbr_to_correct, name_corrected))
        for key, value in change.items():
            indeces = demanda_geo_df[demanda_geo_df["nombrecorto"] == key].index
            demanda_geo_df.loc[indeces, "nombreestacion"] = value

    ### Some Substations are not within our Mapping of Substations to Region. So we use the GEO location to assign them to the closest ZONE.
    # GEO Data to get "approx" coordinates of the Regiones, so we can link the closest nombreestacions without an assigned Region to them.
    if demanda_flow in ["costa_centro", "selva_sur"]:
        direc = os.path.join(GEO_DIR, "selva_sur", "final")
        file = "subestacion.csv"
        tempo = pd.read_csv(os.path.join(direc, file))
        target = (
            tempo[["nombrecorto", "x", "y"]]
            .groupby("nombrecorto")[["x", "y"]]
            .mean()
            .reset_index()
            .rename(columns={"nombrecorto": "nombrecorto"})
        )
        target["location"] = target.apply(
            lambda row: Point((row["x"], row["y"])), axis=1
        )
        geo_gdf = gpd.GeoDataFrame(target, geometry="location")
        geo_gdf = geo_gdf.drop(columns=["x", "y"])
        geo_gdf.set_crs(epsg=4326, inplace=True)
        map_df = pd.read_csv(
            os.path.join(DATA_EXTRA_DIR, "lima_mapping.csv"), index_col=0
        )
        map_df["fullname"] = map_df["fullname"].str.strip()
        map_df["fullname"] = map_df["fullname"].map(
            dict(
                zip(
                    map_df["fullname"].unique().tolist(),
                    [
                        " ".join(x.split(" ")[:-1])
                        for x in map_df["fullname"].unique().tolist()
                    ],
                )
            )
        )
        map_df = map_df.drop(columns=["region"]).rename(
            columns={
                "parent": "region_code",
                "fullname": "region_name",
                "target": "nombrecorto",
            }
        )
        map_df = map_df[["region_code", "region_name"]].drop_duplicates()
        map_df = map_df.merge(
            geo_gdf.rename(columns={"nombrecorto": "region_code"}),
            how="left",
            on="region_code",
        )
        map_df = gpd.GeoDataFrame(map_df, geometry="location")
        missing_region_df = pd.DataFrame(
            {
                "missing": demanda_geo_df[demanda_geo_df["region_code"].isna()][
                    "nombrecorto"
                ].unique()
            }
        )
        missing_region_df = missing_region_df[missing_region_df["missing"] != "TRU1J"]
        missing_region_df = missing_region_df.merge(
            geo_gdf.rename(columns={"nombrecorto": "missing"}), how="left", on="missing"
        )
        missing_region_df = missing_region_df[~missing_region_df["location"].isna()]
        missing_region_df = gpd.GeoDataFrame(missing_region_df, geometry="location")
        to_fill_df = gpd.sjoin_nearest(
            missing_region_df, map_df, how="left", distance_col="distances"
        )
        to_fill_df = to_fill_df[["missing", "region_code", "region_name"]]
        for abr, code, name in to_fill_df.values:
            indeces = demanda_geo_df[demanda_geo_df["nombrecorto"] == abr].index
            demanda_geo_df.loc[indeces, "region_code"] = code
            demanda_geo_df.loc[indeces, "region_name"] = name
        if demanda_flow == "costa_centro":
            indeces = demanda_geo_df[demanda_geo_df["nombrecorto"] == "TRU1J"].index
            demanda_geo_df.loc[indeces, "region_code"] = "sur_costa"
            demanda_geo_df.loc[indeces, "region_name"] = "Colombia Electricity"

    demanda_geo_df.to_csv(os.path.join(OUTPUTS_DIR, f"{demanda_flow}_geo_region.csv"))
    demanda_geo_df.to_parquet(
        os.path.join(OUTPUTS_DIR, f"{demanda_flow}_geo_region.parquet")
    )
    print(f"Saved {demanda_flow} Region Mapped.")


def add_region_codes_to_minera_chica():
    # We get the COORDINATES by doing an applymap of the crypto_names to the costa_centro substations.
    # Then we map those coordinates to the closest selva_sur substation >> So we can get them under a region_code.
    minera_chica = pd.read_parquet(
        os.path.join(MINERIA_CHICA_DIR, "minera_chica_table.parquet")
    )
    geo_costa_centro = pd.read_csv(
        os.path.join(GEO_DIR, "costa_centro", "final", "Substation.csv"), index_col=0
    )
    df_tempo = pd.DataFrame()
    for item in minera_chica["hacienda"].unique():
        aux = geo_costa_centro[
            geo_costa_centro.applymap(lambda x: item.lower() in str(x).lower()).any(
                axis=1
            )
        ]
        aux.loc[:, "hacienda"] = item
        df_tempo = pd.concat([df_tempo, aux])
    minera_chica = minera_chica.merge(
        df_tempo[["hacienda", "x", "y"]], how="left", on="hacienda"
    )
    minera_chica["location"] = minera_chica.apply(
        lambda row: Point((row["x"], row["y"])), axis=1
    )
    minera_chica["hacienda"] = (
        minera_chica["variable_symb"] + ":" + minera_chica["hacienda"]
    )
    minera_chica = minera_chica.drop(columns=["variable_symb"])
    minera_chica = gpd.GeoDataFrame(minera_chica, geometry="location")
    minera_chica.set_crs(epsg=4326, inplace=True)

    direc = os.path.join(GEO_DIR, "selva_sur", "final")
    file = "subestacion.csv"
    tempo = pd.read_csv(os.path.join(direc, file))
    substation_geo = (
        tempo[
            [
                "nombrecorto",
                "IDUBICACION",
                "ESTADOPLANIFICACION",
                "REGION",
                "nombreestacion",
                "x",
                "y",
            ]
        ]
        .groupby(
            by=[
                "nombrecorto",
                "IDUBICACION",
                "ESTADOPLANIFICACION",
                "REGION",
                "nombreestacion",
            ]
        )[["x", "y"]]
        .mean()
        .reset_index()
        .rename(
            columns={
                "nombrecorto": "nombrecorto",
                "IDUBICACION": "ubicacionid",
                "ESTADOPLANIFICACION": "planningstatus",
                "REGION": "region",
                "nombreestacion": "nombreestacion",
            }
        )
    )
    substation_geo["location"] = substation_geo.apply(
        lambda row: Point((row["x"], row["y"])), axis=1
    )
    geo_selva_sur_gfd = gpd.GeoDataFrame(substation_geo, geometry="location")
    geo_selva_sur_gfd = geo_selva_sur_gfd.drop(columns=["x", "y"])
    geo_selva_sur_gfd.set_crs(epsg=4326, inplace=True)

    minera_chica_final = gpd.sjoin_nearest(
        minera_chica, geo_selva_sur_gfd, how="left", distance_col="distance"
    )
    minera_chica_final = minera_chica_final.drop(
        columns=["index_right", "location", "distance"]
    ).rename(
        columns={
            "datetime": "date_time",
            "hacienda": "variables",
            "excel filename": "excel_file",
            "excel sheet": "excel_sheet",
        }
    )
    minera_chica_final["demanda_stream"] = "minera_chica"
    minera_chica_final["nominal"] = minera_chica_final["nominal"] * -1000
    regions_mapping = pd.read_csv(
        os.path.join(DATA_EXTRA_DIR, "lima_mapping.csv"), index_col=0
    )
    regions_mapping["fullname"] = regions_mapping["fullname"].str.strip()
    regions_mapping["fullname"] = regions_mapping["fullname"].map(
        dict(
            zip(
                regions_mapping["fullname"].unique().tolist(),
                [
                    " ".join(x.split(" ")[:-1])
                    for x in regions_mapping["fullname"].unique().tolist()
                ],
            )
        )
    )
    regions_mapping = regions_mapping.drop(columns=["region"]).rename(
        columns={
            "parent": "region_code",
            "fullname": "region_name",
            "target": "nombrecorto",
        }
    )
    minera_chica_final = minera_chica_final.merge(
        regions_mapping, how="left", on="nombrecorto"
    ).reset_index(drop=True)
    minera_chica_final = minera_chica_final.drop_duplicates()
    minera_chica_final.to_csv(os.path.join(OUTPUTS_DIR, "minera_chica_geo_region.csv"))
    minera_chica_final.to_parquet(
        os.path.join(OUTPUTS_DIR, "minera_chica_geo_region.parquet")
    )
    print(f"Saved Mineria_chica Region Mapped.")
