import re
import os
import shutil
import glob
import webbrowser
import time
from tqdm import tqdm
from kedro.pipeline import Pipeline, node, pipeline
from typing import Dict
from typing import List
from datetime import datetime

BAR_FORMAT = "{l_bar}{bar:10}{r_bar}{bar:-10b}"


def download_process_move_raw_pipeline(**kwargs) -> Pipeline:
    return pipeline(
        [
            node(
                func=create_code_folder_raw,
                inputs=["params:categories", "params:stocks", "params:main_folders"],
                outputs=None,
                name="create_code_folder_raw",
            ),
            node(
                func=download_raw,
                inputs=["params:categories", "params:stocks"],
                outputs=None,
                name="download_raw",
            ),
            node(
                func=move_process_remove_raw,
                inputs=["params:categories", "params:stocks"],
                outputs="dummy_raw_pipeline_sequencer",
                name="process_raw",
            ),
        ]
    )


def create_code_folder_raw(
    parameter_categories: List[str],
    parameter_stock: Dict[str, str],
    main_folders: List[str],
) -> None:
    """
    Creates required folders for each stock in each category.

    This function iterates over categories and stocks, and creates a folder for each stock in the main folders.

    Args:
        parameter_categories (List[str]): A list of categories.
        parameter_stock (Dict[str, str]): A dictionary of stocks.
        main_folders (List[str]): A list of main folders where the new folders should be created.

    Returns:
        None
    """
    print("=== Creating Required Folders ===")

    for category in parameter_categories:
        for stock in tqdm(parameter_stock[category], bar_format=BAR_FORMAT):
            code_full = _get_stock_code_full(stock["name"])
            code = code_full.split(".")[0]
            _create_code_folder(code, main_folders)

            print(f"=== Created Required Folders: {stock['name']} ===")

    # create Downlaods folder if not exist
    dir_path = os.path.join("..", "Downloads")
    if not os.path.exists(dir_path):
        os.makedirs(dir_path)

    print("=== Created All Required Folders ===")


def download_raw(
    parameter_categories: List[str], parameter_stock: Dict[str, str]
) -> None:
    """
    Downloads raw files for each stock in each category.

    This function iterates over categories and stocks, constructs URLs for the dividend and price files of each stock,
    and downloads these files. It also includes a delay to allow time for each download to complete.

    Args:
        parameter_categories (List[str]): A list of categories.
        parameter_stock (Dict[str, str]): A dictionary of stocks.

    Returns:
        None
    """
    print("=== Downloading Raw Files ===")

    for category in parameter_categories:
        for stock in tqdm(parameter_stock[category], bar_format=BAR_FORMAT):
            code_full = _get_stock_code_full(stock["name"])
            code = code_full.split(".")[0]

            stock_max_period1 = stock["yahoo_max_period1"]
            raw_div_file_url = _raw_div_file_url(code_full, stock_max_period1)
            _download_raw_file(f"{code}_div.csv", raw_div_file_url)
            # allow time for download to complete
            time.sleep(3)

            raw_price_file_url = _raw_price_file_url(code_full, stock_max_period1)
            _download_raw_file(f"{code}.csv", raw_price_file_url)
            # allow time for download to complete
            time.sleep(3)

            print(f"=== Downloaded Raw File: {stock['name']} ===")

    print("=== Downloaded All Raw Files ===")


def move_process_remove_raw(
    parameter_categories: List[str], parameter_stock: Dict[str, str]
) -> str:
    """
    Moves, processes, and removes raw files.

    This function moves downloaded files to a data folder, processes all primary and intermediate raw files,
    and removes the downloaded files. It iterates over categories and stocks to perform these operations.

    Args:
        parameter_categories (List[str]): A list of categories.
        parameter_stock (Dict[str, str]): A dictionary of stocks.

    Returns:
        str: An empty string. The function does not return any meaningful value but performs file operations as a side effect.
    """
    _move_download_files_to_data_folder()

    print("=== Moving Files to Respective Folders ===")

    for category in parameter_categories:
        for stock in tqdm(parameter_stock[category], bar_format=BAR_FORMAT):
            code_full = _get_stock_code_full(stock["name"])
            code = code_full.split(".")[0]

            _process_all_primary_raw_files(code)
            _process_all_intermediate_raw_files(code)
            print(f"=== Moved Files to Respective Folders: {stock['name']} ===")

        _remove_download_files()

    print("=== Moved All Files to Respective Folders ===")

    return ""


def _get_stock_code_full(stock_name: str) -> str:
    """Return the stock code from input stock name
    The code of stock name are in the format eg H02.SI, 0468.HK

    Args:
        stock_name (str): description of stock in the format name(code)

    Returns:
        _type_: extracted code
    """
    match = re.search(r"\((.*?)\)", stock_name)
    if match:
        code = match.group(1)
    else:
        code = "No match"

    # return only the code
    return str(code)


def _create_code_folder(code: str, main_folders: List[str]) -> None:
    """Create new folder with code as name if its not exist yet.

    Args:
        code (str): stock code
        main_folders (str): list of main folder to create subfolder named code
    """

    for folder in main_folders:
        sub_folder = os.path.join(folder, code)

        # Check if the folder exists
        if not os.path.exists(sub_folder):
            # If the folder doesn't exist, create it
            os.makedirs(sub_folder)
            print(f"Created folder: {sub_folder}")
        else:
            print(f"{sub_folder} folder already exists.")


def _download_raw_file(filename: str, raw_file_url: str) -> None:
    """Download raw files from input url

    Args:
        raw_file_url (str): raw file url
        filename (str): filename to save in Downloads folder
    """
    # print(raw_file_url)
    webbrowser.open(raw_file_url)


def _raw_div_file_url(code_full: str, stock_max_period1: str) -> str:
    """Return url to download raw file based on input stock code.

    Args:
        code_full (str): full stock code of raw file to download eg H78.SI or 0101.HK
        stock_max_period1 (str): stock max period starting date in unix timestamp
        current_date_unix_timestamp (str): current date in unix timestamp
    Returns:
        url (str): url of stock div raw file to download
    """

    current_date_unix_timestamp = _get_current_unix_timestamp()

    url = f"https://query1.finance.yahoo.com/v7/finance/download/{code_full}?period1={stock_max_period1}&period2={current_date_unix_timestamp}&interval=1mo&events=div&includeAdjustedClose=true"
    return url


def _get_current_unix_timestamp() -> str:
    """Return unix timestamp of current date 08:00"""
    # Get the current date with no time
    current_date = datetime.now()
    current_date = current_date.replace(hour=8, minute=0, second=0)

    # Convert the date to Unix timestamp format
    current_date_unix_timestamp = time.mktime(current_date.timetuple())

    return str(int(current_date_unix_timestamp))


def _raw_price_file_url(code_full: str, stock_max_period1: str) -> str:
    """Return url to download raw file based on input stock code.

    Args:
        code_full (str): full stock code of raw file to download eg H78.SI or 0101.HK
        stock_max_period1 (str): stock max period starting date in unix timestamp
        current_date_unix_timestamp (str): current date in unix timestamp
    Returns:
        url (str): url of stock price raw file to download
    """

    current_date_unix_timestamp = _get_current_unix_timestamp()

    url = f"https://query1.finance.yahoo.com/v7/finance/download/{code_full}?period1={stock_max_period1}&period2={current_date_unix_timestamp}&interval=1d&includeAdjustedClose=true"

    return url


def _move_download_files_to_data_folder() -> None:
    """Move all files in Downloads to data/02_intermediate"""

    # specify the directories
    source_dir_path = os.path.join("..", "..", "Downloads")
    destination_directory_path = os.path.join("data", "02_intermediate")

    # List all files in the source folder
    files = os.listdir(source_dir_path)

    # Move each file to the target folder
    for _file in files:
        src_file = os.path.join(source_dir_path, _file)
        os.path.join(destination_directory_path, _file)
        shutil.move(src_file, destination_directory_path)


def _process_all_primary_raw_files(code: str) -> None:
    """Copy all info files to folder ../02_intermediate

    Args:
        code (str): stock code
    """

    # specify the directories
    source_dir_path = os.path.join("data", "01_raw")
    destination_directory_path = os.path.join("data", "02_intermediate")

    # use glob to match the file pattern code
    files = glob.glob(os.path.join(source_dir_path, code + ".info.csv"))

    for _file in files:
        # make a copy of the file
        shutil.copy(_file, destination_directory_path)


def _process_all_intermediate_raw_files(code: str) -> None:
    """Copy file to folder ../02_intermediate and rename from <code>.info.csv to <code>_info.csv
    Copy file to folder ../03_primary and rename from <code>.SI.csv to <code>.csv
    Copy file to folder ../03_primary and rename from <code>(1).SI.csv to <code>_div.csv
    Remove all files from ../02_intermediate

    Args:
        code (str): stock code
    """
    dict_raw_mapping = {
        "SI": "_div.csv",
        "SI(1)": ".csv",
        "HK": "_div.csv",
        "HK(1)": ".csv",
        "info": "_info.csv",
    }

    # specify the directories
    source_dir_path = os.path.join("data", "02_intermediate")
    destination_directory_path = os.path.join("data", "03_primary", code)

    # use glob to match the file pattern code
    files = glob.glob(os.path.join(source_dir_path, code + "*"))

    for _file in files:
        raw_file_name = _file.split("/")[-1]
        file_identifier = raw_file_name.split(".")[1]
        file_rename_end = dict_raw_mapping[file_identifier]
        new_filename = code + file_rename_end

        # make a copy of the file
        shutil.copy(_file, destination_directory_path)
        original_file_path = os.path.join(source_dir_path, raw_file_name)
        copied_file_path = os.path.join(destination_directory_path, raw_file_name)
        renamed_file_path = os.path.join(destination_directory_path, new_filename)

        # rename copied file
        os.rename(copied_file_path, renamed_file_path)

        # Use os.remove() to delete the file
        if os.path.isfile(original_file_path):
            os.remove(original_file_path)


def _remove_download_files() -> None:
    """Remove all files in Downloads folder"""

    # specify the directories
    source_dir_path = os.path.join("..", "..", "Downloads")

    # List all files in the source folder
    files = os.listdir(source_dir_path)

    for _file in files:
        src_file = os.path.join(source_dir_path, _file)

        # Use os.remove() to delete the file
        os.remove(src_file)
