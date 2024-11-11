import os
import warnings
import sys

sys.path.insert(0, "\\".join(os.path.dirname(__file__).split("\\")[:-1]))
warnings.filterwarnings("ignore")

from src.pcweather_f_client import pcweather_f_Client
from src.compiling import compile_all
import warnings

warnings.filterwarnings("ignore")


def run_etl():
    pcweather_f_client = pcweather_f_Client()
    pcweather_f_client.api_download_all()
    compile_all()


if __name__ == "__main__":
    run_etl()
