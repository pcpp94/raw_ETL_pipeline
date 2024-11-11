import subprocess
import os
import glob
import re
import logging

BASE_DIR = os.path.abspath(os.path.dirname(os.path.abspath(__file__)))

# Configure the logging module
logging.basicConfig(
    filename=os.path.join(
        BASE_DIR, "etl_pipeline.log"
    ),  # Log file where messages will be stored.
    level=logging.ERROR,  # Only log errors and above.
    format="%(asctime)s - %(levelname)s - %(message)s",
)


# Function to use subprocess "run" function.
def run_script(script, name):
    print(f"Starting to run script: {name}")
    result = subprocess.run(["python", script], capture_output=True, text=True)
    # capture_output=True: captures the standard output and standard error >> outputs/errors printed by the scripts.
    # text=True so we get strings instead of bytes.

    # Print standard output
    if result.stdout:
        print(f"Output of {name}:\n{result.stdout}")

    # Print standard error if there's any error
    if result.stderr:
        print(f"Error in {name}:\n{result.stderr}")

    print(f"Finished running script: {name}\n{'-'*40}")
    return result.stdout, result.stderr


# This needs to be manually set:
etl_dir = os.path.dirname(__file__)
dir_name = os.path.dirname(__file__).split("\\")[-1]
all_etl_scripts = glob.glob(
    os.path.join(etl_dir, "*", "**", "run_etl_pipeline.py"), recursive=True
)


level_1 = [
    "meteostat",
    "web_scraping_jwt_complex_auth",
    "webscraping_asp.net_form",
    "webscraping_asp.net_form_water",
    "pcweather_f",
]

level_2 = []

etl_level_1 = {
    x: re.search(rf"({dir_name}\\.*)", x).group(0).split("\\")[1]
    for x in all_etl_scripts
    if re.search(rf"({dir_name}\\.*)", x).group(0).split("\\")[1] in level_1
}
etl_level_2 = {
    x: re.search(rf"({dir_name}\\.*)", x).group(0).split("\\")[1]
    for x in all_etl_scripts
    if re.search(rf"({dir_name}\\.*)", x).group(0).split("\\")[1] in level_2
}

if __name__ == "__main__":

    for script, name in etl_level_1.items():
        try:
            run_script(script=script, name=name)
        except Exception as e:
            logging.error(
                f"An error occurred while running etl_level_1: {name}.", exc_info=True
            )

    for script, name in etl_level_2.items():
        try:
            run_script(script=script, name=name)
        except Exception as e:
            logging.error(
                f"An error occurred while running etl_level_2: {name}.", exc_info=True
            )
