import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
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


def download_process_move_raw_pipeline(**kwargs) -> Pipeline:
    return pipeline(
        [
            node(
                func=create_code_folder_raw,
                inputs=["params:sg_reits"],
                outputs=None,
                name="create_code_folder_raw_sg_reits",
            ),
            node(
                func=download_raw,
                inputs=["params:sg_reits"],
                outputs=None,
                name="download_raw_sg_reits",
            ),
            node(
                func=move_process_remove_raw,
                inputs=["params:sg_reits"],
                outputs=None,
                name="process_raw_sg_reits",
            ),
        ]
    )


def create_code_folder_raw(parameter_stock: Dict[str, str]) -> None:
    for stock in tqdm(parameter_stock):
        code_full = _get_stock_code_full(stock["name"])
        code = code_full.split(".")[0]
        _create_code_folder(code)


def download_raw(parameter_stock: Dict[str, str]) -> None:
    for stock in tqdm(parameter_stock):
        code_full = _get_stock_code_full(stock["name"])
        code = code_full.split(".")[0]

        stock_max_period1 = stock["yahoo_max_period1"]
        _download_raw_file(
            f"{code}_div.csv", _raw_div_file_url(code_full, stock_max_period1)
        )
        # allow time for download to complete
        time.sleep(3)

        _download_raw_file(
            f"{code}.csv", _raw_price_file_url(code_full, stock_max_period1)
        )
        # allow time for download to complete
        time.sleep(3)


def move_process_remove_raw(parameter_stock: Dict[str, str]) -> None:
    _move_download_files_to_data_folder()

    for stock in tqdm(parameter_stock):
        code_full = _get_stock_code_full(stock["name"])
        code = code_full.split(".")[0]

        # csv_files.append(os.path.join("..", "..", "data", "02_intermediate", code, code + ".csv"))
        # info_files.append(os.path.join("..", "..", "data", "02_intermediate", code, code + "_info.csv"))
        # div_files.append(os.path.join("..", "..", "data", "02_intermediate", code, code + "_div.csv"))

        _process_all_raw_files(code)

    _remove_download_files()


def _get_stock_code_full(stock_name: str) -> str:
    """Function will return the stock code from input stock name
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


def _create_code_folder(code: str):
    """Function will create new folder with code as name if its not exist yet.

    Args:
        code (str): stock code
    """
    main_folders = [
        "data/02_intermediate",
        "data/07_model_artifacts",
        "data/08_reporting",
    ]

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
    print(raw_file_url)
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


def _move_download_files_to_data_folder():
    # specify the directories
    source_dir_path = os.path.join("..", "..", "Downloads")
    destination_directory_path = os.path.join("data", "01_raw")

    # List all files in the source folder
    files = os.listdir(source_dir_path)

    # Move each file to the target folder
    for _file in files:
        src_file = os.path.join(source_dir_path, _file)
        dst_file = os.path.join(destination_directory_path, _file)
        shutil.move(src_file, destination_directory_path)


def _process_all_raw_files(code: str):
    """Copy file to folder ../02_intermediate and rename from <code>.SI.csv to <code>.csv
    Copy file to folder ../02_intermediate and rename from <code>(1).SI.csv to <code>_div.csv

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
    source_dir_path = os.path.join("data", "01_raw")
    destination_directory_path = os.path.join("data", "02_intermediate", code)

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


def _remove_download_files():
    # specify the directories
    source_dir_path = os.path.join("..", "..", "Downloads")
    destination_directory_path = os.path.join("data", "01_raw")

    # List all files in the source folder
    files = os.listdir(source_dir_path)

    for _file in files:
        src_file = os.path.join(source_dir_path, _file)
        # Use os.remove() to delete the file
        os.remove(src_file)
