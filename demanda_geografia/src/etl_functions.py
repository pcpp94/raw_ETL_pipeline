import os
import re
import pandas as pd
import datetime
from thefuzz import process, fuzz

from .config import DATA_EXTRA_DIR, DEMANDA_OUTPUTS_DIR, OUTPUTS_DIR

transfomer_cols = [
    "FECHACREADA",
    "FECHAMODIFCJAA",
    "IDEQUIPO",
    "FECHADECOMISIONAMIENTO",
    "IDUBICACION",
    "NUMERODEPROYECTO",
    "TITULODEPROYECTO",
    "DESIGNACIONTR",
    "IDINSTALACION",
    "SSIDUBICACION",
    "ESTADOPLANIFICACION",
    "FECHADEENERGIZACION",
    "NUMERODEBAHIA",
    "UBICACIONMX",
    "ESTADOMX",
    "x",
    "y",
]
substation_cols = [
    "FECHACREADA",
    "FECHAMODIFCJAA",
    "IDEQUIPO",
    "FECHADECOMISIONAMIENTO",
    "IDUBICACION",
    "nombreestacion",
    "nombrecorto",
    "ESTADOPLANIFICACION",
    "REGION",
    "UBICACIONMX",
    "ESTADOMX",
    "x",
    "y",
]
fuentes = [
    "costa_centro",
    "costa_norte",
    "sierra_norte",
    "costa_sur",
    "sierra_sur",
    "selva_norte",
    "selva_sur",
]


def join_with_space(series):
    return " ".join(series.astype(str))


def get_geo_files(directory):
    geo_files = get_files_path(directory)
    geo_files = [x for x in geo_files if ("transformador" in x) or ("subestacion" in x)]
    return geo_files


def get_demanda_files(directory):
    demanda_files = get_files_path(directory)
    demanda_files = [x for x in demanda_files if ("gold" in x) and (".parquet" in x)]
    return demanda_files


def get_fuentes_with_modified_time():
    demanda_list = get_demanda_files(DEMANDA_OUTPUTS_DIR)
    current_files_list = [x for x in os.listdir(OUTPUTS_DIR) if x[-3:] != "csv"]
    df = pd.DataFrame({"path": demanda_list})

    for fuente in fuentes:
        path = [x for x in demanda_list if fuente in x.lower()][0]
        index_ = df[df["path"] == path].index
        df.loc[index_, "fuente"] = fuente
        df.loc[index_, "modified"] = datetime.datetime.fromtimestamp(
            os.path.getmtime(path)
        )
        file = [x for x in current_files_list if fuente in x][0]
        df.loc[index_, "current_file_mod"] = datetime.datetime.fromtimestamp(
            os.path.getmtime(os.path.join(OUTPUTS_DIR, file))
        )
        df.loc[index_, "to_change"] = (
            df.loc[index_, "current_file_mod"] <= df.loc[index_, "modified"]
        )

    df = df.dropna().reset_index(drop=True)
    return df


def get_files_needing_update():
    df = get_fuentes_with_modified_time()
    df = df[df["to_change"] == True]
    files = df["fuente"].tolist()
    return files


def get_files_path(directory):
    """
    Getting the files' PATHS in the directory and subdirectories.
    Args:
        directory (str): Path to Directory
    Returns:
        file_paths (list): List with all the files' paths.
    """
    file_paths = []  # List to store file paths
    for root, dirs, files in os.walk(directory):
        for file in files:
            file_path = os.path.join(root, file)
            file_paths.append(file_path)
    print("Files with respective paths retrieved.")
    return file_paths


def adhoc_variables_etl(fuente_variables, fuente):

    if fuente in ["costa_norte", "costa_centro"]:
        return fuente_variables

    if fuente in ["sierra_norte"]:
        fuente_variables["ss"] = fuente_variables["variables"].str.split(
            "|", expand=True
        )[0]
        fuente_variables["ss"] = fuente_variables["ss"].str.strip()
        fuente_variables["ss"] = fuente_variables["ss"].str.replace("Import", "")
        fuente_variables["ss"] = fuente_variables["ss"].str.strip()
        fuente_variables["ss"] = fuente_variables["ss"].str.replace("Export", "")
        fuente_variables["ss"] = fuente_variables["ss"].str.strip()
        fuente_variables["ss"] = fuente_variables["ss"].str.replace(
            "at ", "", regex=False
        )
        fuente_variables["ss"] = fuente_variables["ss"].str.strip()
        fuente_variables["ss"] = fuente_variables["ss"].str.replace(
            "\s+", " ", regex=True
        )
        fuente_variables["ss"] = fuente_variables["ss"].str.strip()
        fuente_variables["ss"] = fuente_variables["ss"].apply(
            lambda x: remove_text_within_parentheses(x)
        )
        fuente_variables["ss"] = fuente_variables["ss"].str.strip()
        fuente_variables["ss"] = fuente_variables["ss"].apply(
            lambda x: remove_duplicate_substrings(x)
        )
        fuente_variables["ss"] = fuente_variables["ss"].str.strip()
        return fuente_variables

    if fuente in ["sierra_sur"]:
        fuente_variables["ss"] = fuente_variables["variables"].str.split(
            "|", expand=True
        )[1]
        fuente_variables["ss"] = fuente_variables["ss"].str.strip()
        fuente_variables["ss"] = fuente_variables["ss"].str.replace("Fase", "")
        fuente_variables["ss"] = fuente_variables["ss"].str.strip()
        fuente_variables["ss"] = fuente_variables["ss"].str.replace(
            "at ", "", regex=False
        )
        fuente_variables["ss"] = fuente_variables["ss"].str.strip()
        fuente_variables["ss"] = fuente_variables["ss"].str.replace(
            "\s+", " ", regex=True
        )
        fuente_variables["ss"] = fuente_variables["ss"].str.strip()
        fuente_variables["ss"] = fuente_variables["ss"].apply(
            lambda x: remove_text_within_parentheses(x)
        )
        fuente_variables["ss"] = fuente_variables["ss"].str.strip()
        fuente_variables["ss"] = fuente_variables["ss"].apply(
            lambda x: remove_duplicate_substrings(x)
        )
        fuente_variables["ss"] = fuente_variables["ss"].str.strip()
        return fuente_variables

    if fuente in ["selva_norte"]:
        fuente_variables["ss"] = fuente_variables["variables"].str.split(
            "|", expand=True
        )[0]
        fuente_variables["ss"] = fuente_variables["ss"].str.strip()
        fuente_variables["ss"] = fuente_variables["ss"].str.replace("Import", "")
        fuente_variables["ss"] = fuente_variables["ss"].str.strip()
        fuente_variables["ss"] = fuente_variables["ss"].str.replace("Export", "")
        fuente_variables["ss"] = fuente_variables["ss"].str.strip()
        fuente_variables["ss"] = fuente_variables["ss"].str.replace(
            "at ", "", regex=False
        )
        fuente_variables["ss"] = fuente_variables["ss"].str.strip()
        fuente_variables["ss"] = fuente_variables["ss"].str.replace(
            "\s+", " ", regex=True
        )
        fuente_variables["ss"] = fuente_variables["ss"].str.strip()
        fuente_variables["ss"] = fuente_variables["ss"].apply(
            lambda x: remove_text_within_parentheses(x)
        )
        fuente_variables["ss"] = fuente_variables["ss"].str.strip()
        fuente_variables["ss"] = fuente_variables["ss"].apply(
            lambda x: remove_duplicate_substrings(x)
        )
        fuente_variables["ss"] = fuente_variables["ss"].str.strip()
        return fuente_variables

    else:
        return fuente_variables


def get_demanda_df(demanda_list, fuente, geo_df):
    demanda_df = pd.read_parquet([x for x in demanda_list if fuente in x.lower()])

    if fuente in ["selva_sur"]:
        demanda_df["variables"] = demanda_df["variables"].str.strip()
        demanda_df["variables"] = demanda_df["variables"].str.replace("Fase", "")
        demanda_df["variables"] = demanda_df["variables"].str.strip()
        selva_sur_dict = {
            "HYDRO": "HYDRO",
            "HAL": "HAL",
            "Tarapoto  1": "TPP",
            "Tarapoto  2": "TPP",
            "CJA A2": "CJA",
            "CJAB IB": "CJB",
            "CJAB NBE": "CJB",
            "JUL": "JULX",
        }
        demanda_df["variables"] = demanda_df["variables"].map(selva_sur_dict)

    fuente_variables = (
        demanda_df[["excel_sheet", "variables"]]
        .drop_duplicates()
        .reset_index(drop=True)
    )

    if fuente in ["costa_norte", "costa_centro"]:
        fuente_dict = pd.read_csv(os.path.join(DATA_EXTRA_DIR, f"{fuente}_dic.csv"))
    else:
        fuente_dict = make_geo_dict(geo_df)

    fuente_variables = adhoc_variables_etl(fuente_variables, fuente)

    return demanda_df, fuente_variables, fuente_dict


def get_geo_df(geo_list, fuente):
    geo_t = pd.read_csv(
        [x for x in geo_list if "transformador" in x][0],
        parse_dates=["FECHACREADA", "FECHAMODIFCJAA", "FECHADECOMISIONAMIENTO"],
    )
    geo_ss = pd.read_csv(
        [x for x in geo_list if "subestacion" in x][0],
        parse_dates=["FECHACREADA", "FECHAMODIFCJAA", "FECHADECOMISIONAMIENTO"],
    )
    geo_t = geo_t[transfomer_cols]
    geo_ss = geo_ss[substation_cols]
    geo_df = (
        geo_ss.groupby(by=["nombrecorto"])
        .agg(
            {
                "FECHACREADA": "min",
                "FECHAMODIFCJAA": "min",
                "IDEQUIPO": join_with_space,
                "FECHADECOMISIONAMIENTO": "min",
                "IDUBICACION": join_with_space,
                "nombreestacion": join_with_space,
                "ESTADOPLANIFICACION": join_with_space,
                "REGION": join_with_space,
                "UBICACIONMX": join_with_space,
                "ESTADOMX": join_with_space,
                "x": "mean",
                "y": "mean",
            }
        )
        .reset_index()
    )

    geo_df.columns = geo_df.columns.str.lower()
    geo_df = geo_df.applymap(
        lambda x: remove_duplicate_strings(x) if isinstance(x, str) else x
    )

    if fuente in ["sierra_norte"]:
        geo_df[geo_df.applymap(lambda x: "dhaid" in str(x).lower())["nombreestacion"]]

    return geo_df


def make_geo_dict(geo_df):
    one = geo_df[["nombrecorto", "nombrecorto"]]
    one.columns = ["id", "nombrecorto"]
    two = geo_df[["nombreestacion", "nombrecorto"]]
    two.columns = ["id", "nombrecorto"]
    three = geo_df[["ubicacionid", "nombrecorto"]]
    three.columns = ["id", "nombrecorto"]
    fuente_dict = pd.concat([one, two, three])
    return fuente_dict


def dict_match(fuente_variables, fuente_dict):
    dict_match = fuente_variables.merge(
        fuente_dict[["Outstation TAG", "SS Code"]].drop_duplicates(),
        how="left",
        left_on="variables",
        right_on="Outstation TAG",
        indicator=True,
    )
    missing_match = dict_match[dict_match["_merge"] == "left_only"][
        ["excel_sheet", "variables"]
    ]
    dict_match = dict_match[dict_match["_merge"] == "both"][
        ["excel_sheet", "variables", "SS Code"]
    ]
    return dict_match, missing_match


def merge_etl(demanda_df, match_df, geo_df, fuente):
    if fuente in ["costa_centro", "costa_norte"]:
        demanda_geo_df = demanda_df.merge(
            match_df, how="left", on=["excel_sheet", "variables"]
        ).rename(columns={"SS Code": "nombrecorto"})
    else:
        demanda_geo_df = demanda_df.merge(
            match_df, how="left", on=["excel_sheet", "variables"]
        )

    demanda_geo_df = demanda_geo_df.merge(
        geo_df[
            [
                "nombrecorto",
                "ubicacionid",
                "nombreestacion",
                "planningstatus",
                "region",
                "x",
                "y",
            ]
        ],
        how="left",
        on="nombrecorto",
    )
    return demanda_geo_df


########################################################## The Fuzz:
def find_best_match(row, column_to_match, choices, scorer):
    """
    Define a function to find the best match for each row of fuente_variables['variables'] against geo_df['ubicacionid'] or the dictionary again.
    """
    best_match = process.extractOne(row[column_to_match], choices, scorer=scorer)
    return best_match if best_match else None


def fuzz_match_dict(missing_match, fuente_dict, fuente):

    if fuente in ["costa_centro", "costa_norte"]:
        choices = fuente_dict["Outstation TAG"].dropna().unique().tolist()
        to_merge = fuente_dict[~fuente_dict["Outstation TAG"].isna()][
            ["Outstation TAG", "SS Code"]
        ]
        merge_col = "Outstation TAG"
        ss_col = "SS Code"
    else:
        choices = fuente_dict["id"].dropna().unique().tolist()
        to_merge = fuente_dict[~fuente_dict["id"].isna()][
            ["id", "nombrecorto"]
        ].drop_duplicates()
        merge_col = "id"
        ss_col = "nombrecorto"

    missing_match[["BestMatch", "Score"]] = missing_match.apply(
        find_best_match,
        column_to_match="variables",
        choices=choices,
        scorer=fuzz.partial_ratio,
        axis=1,
    ).tolist()

    dict_fuzz_match = missing_match.merge(
        to_merge.drop_duplicates(), left_on="BestMatch", right_on=merge_col, how="left"
    )
    if fuente in ["costa_sur"]:
        indices = dict_fuzz_match[
            dict_fuzz_match["variables"].str.contains("TPP")
        ].index
        dict_fuzz_match.loc[indices, "id"] = "Kuelap Cajamarca"
        dict_fuzz_match.loc[indices, "nombrecorto"] = "TRPP"
    dict_fuzz_match = dict_fuzz_match[["excel_sheet", "variables", ss_col]].reset_index(
        drop=True
    )
    return dict_fuzz_match


########################################################## Ad-hoc ETL functions:
def remove_duplicate_strings(input_string):
    # Split the string into words based on any non-word character using regex
    words = re.split(r"\s+", input_string)
    seen = set()
    unique_words = []
    for word in words:
        # Check if the word has been seen before, case-insensitive comparison
        if word.lower() not in seen:
            unique_words.append(word)
            seen.add(word.lower())
    # Reconstruct the string from unique words
    # This will not preserve original delimiters like "|", consider if this is acceptable
    result_string = " ".join(unique_words)
    return result_string


def remove_duplicate_substrings(input_string):
    # Split the string into words based on any non-word character using regex
    words = re.split(r"\W+", input_string)

    seen = set()
    unique_words = []
    for word in words:
        # Check if the word has been seen before, case-insensitive comparison
        if word.lower() not in seen:
            unique_words.append(word)
            seen.add(word.lower())

    # Reconstruct the string from unique words
    # This will not preserve original delimiters like "|", consider if this is acceptable
    result_string = " ".join(unique_words)

    return result_string


def remove_text_within_parentheses(text):
    # This regex matches content within parentheses, including nested ones
    # It looks for an opening parenthesis, followed by any characters that are not a closing parenthesis (non-greedy), and a closing parenthesis
    return re.sub(r"\([^()]*\)", "", text)
