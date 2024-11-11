import os
import pandas as pd
import datetime
from .config import COMPILED_OUTPUTS_DIR, DEMANDA_GEO_DIR

fuentes = [
    "costa_centro",
    "costa_norte",
    "sierra_norte",
    "costa_sur",
    "sierra_sur",
    "selva_norte",
    "selva_sur",
]


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


def get_demanda_files(directory):
    demanda_files = get_files_path(directory)
    demanda_files = [x for x in demanda_files if (".parquet" in x)]
    return demanda_files


def get_fuentes_with_modified_time():
    demanda_list = get_demanda_files(DEMANDA_GEO_DIR)
    current_files_list = [
        x for x in os.listdir(COMPILED_OUTPUTS_DIR) if x[-3:] != "csv"
    ]
    df = pd.DataFrame({"path": demanda_list})

    for fuente in fuentes:
        path = [x for x in demanda_list if fuente in x.lower()][0]
        index_ = df[df["path"] == path].index
        df.loc[index_, "fuente"] = fuente
        df.loc[index_, "modified"] = datetime.datetime.fromtimestamp(
            os.path.getmtime(path)
        )

    df["current_file_mod"] = datetime.datetime.fromtimestamp(
        os.path.getmtime(os.path.join(COMPILED_OUTPUTS_DIR, current_files_list[0]))
    )
    df["to_change"] = df["current_file_mod"] <= df["modified"]
    df = df.dropna().reset_index(drop=True)

    return df


def get_files_needing_update():
    df = get_fuentes_with_modified_time()
    df = df[df["to_change"] == True]
    files = df["fuente"].tolist()
    return files
