import os
import pandas as pd
from collections import defaultdict

from .config import GEO_DIR


def get_substations(departamento):

    ## Get Substations csv
    file = "subestacion.csv"
    substations = pd.read_csv(os.path.join(GEO_DIR, "selva_sur", "final", file))
    for col in substations.select_dtypes("object").columns:
        substations[col] = substations[col].str.strip()
    if departamento == "lima":
        substations_400kv = (
            substations[
                (substations["V1LEVEL"] == "400 kV")
                & (~substations["REGION"].isin(["Cajamarca", "Iquitos", "Arequipa"]))
            ]
            .groupby(
                by=[
                    "REGION",
                    "nombrecorto",
                    "FULLNAME",
                    "ESTADOPLANIFICACION",
                    "ESTADOMX",
                ]
            )["nombreestacion"]
            .count()
            .reset_index()
        )
        ss_to_loop = (
            substations[(substations["V1LEVEL"] == "400 kV")]
            .groupby(
                by=[
                    "REGION",
                    "nombrecorto",
                    "FULLNAME",
                    "ESTADOPLANIFICACION",
                    "ESTADOMX",
                ]
            )["nombreestacion"]
            .count()
            .reset_index()["nombrecorto"]
            .unique()
            .tolist()
        )
    else:
        # This is atually only the Supply Point substations.
        substations_400kv = (
            substations[
                (substations["REGION"].isin(["Cajamarca", "Iquitos", "Arequipa"]))
            ]
            .groupby(
                by=[
                    "REGION",
                    "nombrecorto",
                    "FULLNAME",
                    "ESTADOPLANIFICACION",
                    "ESTADOMX",
                ]
            )["nombreestacion"]
            .count()
            .reset_index()
        )
        ss_to_loop = (
            substations[
                (substations["REGION"].isin(["Cajamarca", "Iquitos", "Arequipa"]))
            ]
            .groupby(
                by=[
                    "REGION",
                    "nombrecorto",
                    "FULLNAME",
                    "ESTADOPLANIFICACION",
                    "ESTADOMX",
                ]
            )["nombreestacion"]
            .count()
            .reset_index()["nombrecorto"]
            .unique()
            .tolist()
        )

    return substations, substations_400kv, ss_to_loop


def lines_connectivity():

    ## Get OverHeadLine csv
    file = "LineasAltas.csv"
    over_head_lines = pd.read_csv(os.path.join(GEO_DIR, "selva_sur", "final", file))
    over_head_lines = over_head_lines[
        [
            "IDUBICACION",
            "UBICACIONMX",
            "FUENTECONECCION",
            "DESTINOCONECCION",
            "NOMINALVOLTAGE",
            "ESTADOPLANIFICACION",
            "REGION",
            "OWNER",
        ]
    ]

    ## Get JointPit csv
    file = "UnionLineas.csv"
    joint_pit = pd.read_csv(os.path.join(GEO_DIR, "selva_sur", "final", file))
    joint_pit = joint_pit[["IDUBICACION", "UBICACIONMX"]]
    joint_pit["FUENTECONECCION"] = joint_pit["IDUBICACION"].str.split("-", expand=True)[
        2
    ]
    joint_pit["DESTINOCONECCION"] = joint_pit["IDUBICACION"].str.split(
        "-", expand=True
    )[3]
    joint_pit["NOMINALVOLTAGE"] = (
        joint_pit["IDUBICACION"].str.split("-", expand=True)[1] + " kV"
    )

    lines_source_destination = pd.concat(
        [
            over_head_lines[
                [
                    "IDUBICACION",
                    "UBICACIONMX",
                    "FUENTECONECCION",
                    "DESTINOCONECCION",
                    "NOMINALVOLTAGE",
                ]
            ],
            joint_pit,
        ]
    )
    lines_source_destination = lines_source_destination.drop_duplicates()
    for col in lines_source_destination.columns:
        lines_source_destination[col] = lines_source_destination[col].str.strip()

    return lines_source_destination


def three_levels_iteration_mapping(
    lines_source_destination, ss_to_loop, substations_400kv
):

    ad_mapping = {}

    # First Layer, by ZONE (400kV Substations): SOURCE == region
    for region in ss_to_loop:
        filters = (lines_source_destination["FUENTECONECCION"] == region) & (
            lines_source_destination["NOMINALVOLTAGE"] != "400 kV"
        )
        ad_mapping[region] = defaultdict(
            int,
            lines_source_destination[filters]["DESTINOCONECCION"]
            .value_counts()
            .to_dict(),
        )

        # Second Layer by Substations at 200kV
        for item in (
            lines_source_destination[filters]["DESTINOCONECCION"].unique().tolist()
        ):
            filters = (
                lines_source_destination.applymap(lambda x: x == item).any(axis=1)
            ) & (lines_source_destination["NOMINALVOLTAGE"] != "400 kV")
            temp = (
                lines_source_destination[filters]["FUENTECONECCION"]
                .value_counts()
                .to_dict()
            )
            for key, value in temp.items():
                ad_mapping[region][key] += value
            temp = (
                lines_source_destination[filters]["DESTINOCONECCION"]
                .value_counts()
                .to_dict()
            )
            for key, value in temp.items():
                ad_mapping[region][key] += value

            # Third Layer by Substations at 200kV
            for item in (
                lines_source_destination[filters]["FUENTECONECCION"].unique().tolist()
            ):
                filters = (
                    lines_source_destination.applymap(lambda x: x == item).any(axis=1)
                ) & (lines_source_destination["NOMINALVOLTAGE"] != "400 kV")
                temp = (
                    lines_source_destination[filters]["DESTINOCONECCION"]
                    .value_counts()
                    .to_dict()
                )
                for key, value in temp.items():
                    ad_mapping[region][key] += value
                temp = (
                    lines_source_destination[filters]["FUENTECONECCION"]
                    .value_counts()
                    .to_dict()
                )
                for key, value in temp.items():
                    ad_mapping[region][key] += value
            for item in (
                lines_source_destination[filters]["DESTINOCONECCION"].unique().tolist()
            ):
                filters = (
                    lines_source_destination.applymap(lambda x: x == item).any(axis=1)
                ) & (lines_source_destination["NOMINALVOLTAGE"] != "400 kV")
                temp = (
                    lines_source_destination[filters]["DESTINOCONECCION"]
                    .value_counts()
                    .to_dict()
                )
                for key, value in temp.items():
                    ad_mapping[region][key] += value
                temp = (
                    lines_source_destination[filters]["FUENTECONECCION"]
                    .value_counts()
                    .to_dict()
                )
                for key, value in temp.items():
                    ad_mapping[region][key] += value

    # First Layer, by ZONE (400kV Substations): DESTINATION == region
    for region in ss_to_loop:
        filters = (lines_source_destination["DESTINOCONECCION"] == region) & (
            lines_source_destination["NOMINALVOLTAGE"] != "400 kV"
        )
        temp2 = defaultdict(
            int,
            lines_source_destination[filters]["FUENTECONECCION"]
            .value_counts()
            .to_dict(),
        )
        for key, value in temp2.items():
            ad_mapping[region][key] += value

        # Second Layer by Substations at 200kV
        for item in (
            lines_source_destination[filters]["FUENTECONECCION"].unique().tolist()
        ):
            filters = (
                lines_source_destination.applymap(lambda x: x == item).any(axis=1)
            ) & (lines_source_destination["NOMINALVOLTAGE"] != "400 kV")
            temp = (
                lines_source_destination[filters]["DESTINOCONECCION"]
                .value_counts()
                .to_dict()
            )
            for key, value in temp.items():
                ad_mapping[region][key] += value
            temp = (
                lines_source_destination[filters]["FUENTECONECCION"]
                .value_counts()
                .to_dict()
            )
            for key, value in temp.items():
                ad_mapping[region][key] += value

            # Third Layer by Substations at 200kV
            for item in (
                lines_source_destination[filters]["FUENTECONECCION"].unique().tolist()
            ):
                filters = (
                    lines_source_destination.applymap(lambda x: x == item).any(axis=1)
                ) & (lines_source_destination["NOMINALVOLTAGE"] != "400 kV")
                temp = (
                    lines_source_destination[filters]["DESTINOCONECCION"]
                    .value_counts()
                    .to_dict()
                )
                for key, value in temp.items():
                    ad_mapping[region][key] += value
                temp = (
                    lines_source_destination[filters]["FUENTECONECCION"]
                    .value_counts()
                    .to_dict()
                )
                for key, value in temp.items():
                    ad_mapping[region][key] += value
            for item in (
                lines_source_destination[filters]["DESTINOCONECCION"].unique().tolist()
            ):
                filters = (
                    lines_source_destination.applymap(lambda x: x == item).any(axis=1)
                ) & (lines_source_destination["NOMINALVOLTAGE"] != "400 kV")
                temp = (
                    lines_source_destination[filters]["DESTINOCONECCION"]
                    .value_counts()
                    .to_dict()
                )
                for key, value in temp.items():
                    ad_mapping[region][key] += value
                temp = (
                    lines_source_destination[filters]["FUENTECONECCION"]
                    .value_counts()
                    .to_dict()
                )
                for key, value in temp.items():
                    ad_mapping[region][key] += value

    rows = []
    for parent, children in ad_mapping.items():
        for child, value in children.items():
            rows.append((parent, child, value))
    # Create the DataFrame
    ad_links_count_geo_only = pd.DataFrame(rows, columns=["Parent", "Target", "Value"])

    ad_links_count_geo_only = ad_links_count_geo_only.merge(
        ad_links_count_geo_only.groupby("Target")["Value"]
        .max()
        .reset_index()
        .rename(columns={"Value": "Max"}),
        how="left",
        on="Target",
    )
    ad_links_count_geo_only["flag"] = (
        ad_links_count_geo_only["Value"] == ad_links_count_geo_only["Max"]
    )
    ad_links_count_geo_only = ad_links_count_geo_only[
        ad_links_count_geo_only["flag"] == True
    ]
    ad_links_count_geo_only = ad_links_count_geo_only.drop(columns=["Max", "flag"])
    ad_links_count_geo_only["count"] = ad_links_count_geo_only.groupby("Target")[
        "Target"
    ].transform("count")
    ad_links_count_geo_only = ad_links_count_geo_only.reset_index(drop=True)
    ad_links_count_geo_only.sort_values("count")

    ad_links_count_geo_only = ad_links_count_geo_only.merge(
        substations_400kv.rename(columns={"nombrecorto": "Parent"}).drop(
            columns="nombreestacion"
        ),
        how="left",
        on="Parent",
    )
    ad_links_count_geo_only = ad_links_count_geo_only[
        ~ad_links_count_geo_only["REGION"].isna()
    ]
    ad_links_count_geo_only["key"] = (
        ad_links_count_geo_only["Parent"] + ad_links_count_geo_only["Target"]
    )

    return ad_links_count_geo_only


def three_levels_iteration_mapping_wo_400kv(
    lines_source_destination, ss_to_loop, substations_400kv, substations
):

    prov_mapping = {}

    # First Layer, by ZONE (400kV Substations):
    for region in ss_to_loop:
        filters = lines_source_destination["FUENTECONECCION"] == region
        prov_mapping[region] = defaultdict(
            int,
            lines_source_destination[filters]["DESTINOCONECCION"]
            .value_counts()
            .to_dict(),
        )

        # Second Layer by Substations at 200kV
        for item in (
            lines_source_destination[filters]["DESTINOCONECCION"].unique().tolist()
        ):
            filters = lines_source_destination.applymap(lambda x: x == item).any(axis=1)
            temp = (
                lines_source_destination[filters]["FUENTECONECCION"]
                .value_counts()
                .to_dict()
            )
            for key, value in temp.items():
                prov_mapping[region][key] += value
            temp = (
                lines_source_destination[filters]["DESTINOCONECCION"]
                .value_counts()
                .to_dict()
            )
            for key, value in temp.items():
                prov_mapping[region][key] += value

            # Third Layer by Substations at 200kV
            for item in (
                lines_source_destination[filters]["FUENTECONECCION"].unique().tolist()
            ):
                filters = lines_source_destination.applymap(lambda x: x == item).any(
                    axis=1
                )
                temp = (
                    lines_source_destination[filters]["DESTINOCONECCION"]
                    .value_counts()
                    .to_dict()
                )
                for key, value in temp.items():
                    prov_mapping[region][key] += value
                temp = (
                    lines_source_destination[filters]["FUENTECONECCION"]
                    .value_counts()
                    .to_dict()
                )
                for key, value in temp.items():
                    prov_mapping[region][key] += value
            for item in (
                lines_source_destination[filters]["DESTINOCONECCION"].unique().tolist()
            ):
                filters = lines_source_destination.applymap(lambda x: x == item).any(
                    axis=1
                )
                temp = (
                    lines_source_destination[filters]["DESTINOCONECCION"]
                    .value_counts()
                    .to_dict()
                )
                for key, value in temp.items():
                    prov_mapping[region][key] += value
                temp = (
                    lines_source_destination[filters]["FUENTECONECCION"]
                    .value_counts()
                    .to_dict()
                )
                for key, value in temp.items():
                    prov_mapping[region][key] += value

    # First Layer, by ZONE (400kV Substations):
    for region in ss_to_loop:
        filters = lines_source_destination["DESTINOCONECCION"] == region
        temp2 = defaultdict(
            int,
            lines_source_destination[filters]["FUENTECONECCION"]
            .value_counts()
            .to_dict(),
        )
        for key, value in temp2.items():
            prov_mapping[region][key] += value

        # Second Layer by Substations at 200kV
        for item in (
            lines_source_destination[filters]["FUENTECONECCION"].unique().tolist()
        ):
            filters = lines_source_destination.applymap(lambda x: x == item).any(axis=1)
            temp = (
                lines_source_destination[filters]["DESTINOCONECCION"]
                .value_counts()
                .to_dict()
            )
            for key, value in temp.items():
                prov_mapping[region][key] += value
            temp = (
                lines_source_destination[filters]["FUENTECONECCION"]
                .value_counts()
                .to_dict()
            )
            for key, value in temp.items():
                prov_mapping[region][key] += value

            # Third Layer by Substations at 200kV
            for item in (
                lines_source_destination[filters]["FUENTECONECCION"].unique().tolist()
            ):
                filters = lines_source_destination.applymap(lambda x: x == item).any(
                    axis=1
                )
                temp = (
                    lines_source_destination[filters]["DESTINOCONECCION"]
                    .value_counts()
                    .to_dict()
                )
                for key, value in temp.items():
                    prov_mapping[region][key] += value
                temp = (
                    lines_source_destination[filters]["FUENTECONECCION"]
                    .value_counts()
                    .to_dict()
                )
                for key, value in temp.items():
                    prov_mapping[region][key] += value
            for item in (
                lines_source_destination[filters]["DESTINOCONECCION"].unique().tolist()
            ):
                filters = lines_source_destination.applymap(lambda x: x == item).any(
                    axis=1
                )
                temp = (
                    lines_source_destination[filters]["DESTINOCONECCION"]
                    .value_counts()
                    .to_dict()
                )
                for key, value in temp.items():
                    prov_mapping[region][key] += value
                temp = (
                    lines_source_destination[filters]["FUENTECONECCION"]
                    .value_counts()
                    .to_dict()
                )
                for key, value in temp.items():
                    prov_mapping[region][key] += value

    rows = []
    for parent, children in prov_mapping.items():
        for child, value in children.items():
            rows.append((parent, child, value))
    # Create the DataFrame
    ne_links = pd.DataFrame(rows, columns=["Parent", "Target", "Value"])
    ne_links = ne_links[["Parent", "Target"]].drop_duplicates()
    ne_links = ne_links.merge(
        substations_400kv.rename(columns={"nombrecorto": "Parent"}).drop(
            columns="nombreestacion"
        ),
        how="left",
        on="Parent",
    )
    ne_links = (
        ne_links.groupby(by=["Parent", "Target", "REGION"])
        .agg({"FULLNAME": "| ".join})
        .reset_index()
    )
    ne_links.columns = ne_links.columns.str.lower()
    ne_links = ne_links.merge(
        substations.groupby(by=["nombrecorto", "REGION"])["OBJECTID"]
        .count()
        .reset_index()
        .drop(columns=["OBJECTID"])
        .rename(columns={"nombrecorto": "target", "REGION": "target_region"}),
        how="left",
        on="target",
    )

    return ne_links


def manual_tweaks_and_others(ad_links_count_geo_only):

    ## Rows to drop:
    to_drop = [
        "TPPAjbn",
        "TPPAdnc",
        "MDDIOCLM",
        "MDDIOIQQ",
        "CJARMTH",
        "CJADBYA",
        "CPTLSLTC",
        "PERAIRP",
        "PERJULX",
        "GIGNHDA",
        "GIGMRFA",
        "JULIHMMO",
        "JULIKLL",
        "TYLCJA",
    ]
    to_drop = to_drop + [
        "TPPPRSP",
        "TPPNZAK",
        "TPPAHYG",
        "ASWGSLMG",
        "JULXAIRP",
        "PERMHWG",
        "PERMKAA",
        "PERMKBB",
        "CPTLMOS1",
        "CPTLSTDM",
        "TYLNARP",
        "JULIASAB",
        "JULIASBS",
        "JULIASGS",
        "JULINKHL",
        "JULICUZ",
        "CJACNTG",
        "JULIELXR",
        "JULIMRPS",
        "TPPRWSG",
    ]
    to_drop = to_drop + ["CJADBYB", "JULILWST", "MRPSLWST"]

    ad_links_count_geo_only = ad_links_count_geo_only[
        ~ad_links_count_geo_only["key"].isin(to_drop)
    ]

    ## Rows to add:
    data = [
        ["MDDIO", "Adnc", "Arequipa", "Pradera 400kV"],
        ["MDDIO", "Ajbn", "Arequipa", "Pradera 400kV"],
        ["TPP", "CLM", "Arequipa", "Ecuador 400kV"],
        ["TPP", "IQQ", "Arequipa", "Ecuador 400kV"],
        ["JULX", "SLTC", "Sierra", "Hechizo 400kV"],
    ]

    columns = ["Parent", "Target", "REGION", "FULLNAME"]
    ad_links_count_geo_only = pd.concat(
        [ad_links_count_geo_only, pd.DataFrame(data, columns=columns)]
    )

    data = [
        ["CJA", "CJA2", "Sierra", "CJA Red (400/220/33)KV"],
        ["CJA", "MOSF2", "Sierra", "CJA Red (400/220/33)KV"],
        ["CPTL", "WATHB", "Sierra", "Capital Distrito 400kV"],
        ["GIG", "HFMO", "Sierra", "Janeiro Costa 400kV"],
        ["GIG", "1GRU", "Sierra", "Janeiro Costa 400kV"],
        ["GIG", "2GRU", "Sierra", "Janeiro Costa 400kV"],
        ["CJS", "CJS", "Sierra", "CJAeelah 400kV"],
        ["CJS", "EZE", "Sierra", "CJAeelah 400kV"],
        ["CJS", "CJA", "Sierra", "CJAeelah 400kV"],
        ["CJS", "CJB", "Sierra", "CJAeelah 400kV"],
        ["CJS", "TANE", "Sierra", "CJAeelah 400kV"],
        ["CJS", "TWGS", "Sierra", "CJAeelah 400kV"],
        ["MRPS", "DBYB", "Costa", "Equitako 400kV"],
        ["MRPS", "MRFA", "Costa", "Equitako 400kV"],
        ["CUZ", "QSWR", "Costa", "Costa Cuzco 400kV"],
        ["TPP", "GCSL", "Costa", "Palmera 400kV"],
        ["TPP", "RUWA", "Costa", "Palmera 400kV"],
    ]
    columns = ["Parent", "Target", "REGION", "FULLNAME"]
    ad_links_count_geo_only = pd.concat(
        [ad_links_count_geo_only, pd.DataFrame(data, columns=columns)]
    )

    data = [
        ["QURM", "PRSP", "Lima Island", "Sao (E19) 400kV"],
        ["ASWG", "NZAK", "Arequipa", "Arequipa South-West 400kV"],
        ["MDDIO", "AHYG", "Arequipa", "Pradera 400kV"],
        ["MDDIO", "SLMG", "Arequipa", "Pradera 400kV"],
        ["TYL", "AIRP", "Sierra", "Bahia 400kV"],
        ["CPTL", "MHWG", "Sierra", "Capital Distrito 400kV"],
        ["JULX", "MKAA", "Sierra", "Hechizo 400kV"],
        ["JULX", "MKBB", "Sierra", "Hechizo 400kV"],
        ["JULX", "MOS1", "Sierra", "Hechizo 400kV"],
        ["JULX", "STDM", "Sierra", "Hechizo 400kV"],
        ["GRUH", "NARP", "Sierra", "Elopez 400kV"],
        ["CUZ", "ASAB", "Costa", "Costa Cuzco 400kV"],
        ["CUZ", "ASBS", "Costa", "Costa Cuzco 400kV"],
        ["CUZ", "ASGS", "Costa", "Costa Cuzco 400kV"],
        ["CUZ", "NKHL", "Costa", "Costa Cuzco 400kV"],
        ["CUZ", "CUZ", "Costa", "Costa Cuzco 400kV"],
        ["MRPS", "CNTG", "Costa", "Equitako 400kV"],
        ["MRPS", "ELXR", "Costa", "Equitako 400kV"],
        ["MRPS", "MRPS", "Costa", "Equitako 400kV"],
        ["RWSG", "RWSG", "Costa", "Etico 400kV"],
    ]
    columns = ["Parent", "Target", "REGION", "FULLNAME"]
    ad_links_count_geo_only = pd.concat(
        [ad_links_count_geo_only, pd.DataFrame(data, columns=columns)]
    )

    ad_links_count_geo_only = ad_links_count_geo_only.drop(
        columns=["ESTADOPLANIFICACION", "ESTADOMX", "key", "Value", "count"]
    )
    ad_links_count_geo_only.columns = ad_links_count_geo_only.columns.str.lower()
    for col in ad_links_count_geo_only:
        ad_links_count_geo_only[col] = ad_links_count_geo_only[col].str.strip()
    ad_links_count_geo_only = ad_links_count_geo_only.drop_duplicates()

    return ad_links_count_geo_only
