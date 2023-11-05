import os
import pandas as pd
import json
from kedro.pipeline import Pipeline, node, pipeline
from typing import Dict, List
from datetime import datetime
from stock_analysis.pipelines.download_process_raw.download_raw_nodes import (
    _get_stock_code_full,
)

BAR_FORMAT = "{l_bar}{bar:10}{r_bar}{bar:-10b}"


def process_info_pipeline(**kwargs) -> Pipeline:
    return pipeline(
        [
            node(
                func=merge_info,
                inputs=[
                    "params:primary_data_folder",
                    "params:feature_data_folder",
                    "params:years_window",
                    "params:stock_date_format",
                    "dummy_raw_pipeline_sequencer",
                ],
                outputs="dummy_process_info_node_sequencer",
                name="merge_info",
            ),
            node(
                func=update_stock_info_dict,
                inputs=[
                    "params:feature_data_folder",
                    "params:categories",
                    "params:stocks",
                    "info_dict_5",
                    "info_dict_10",
                    "dummy_process_info_node_sequencer",
                ],
                outputs=[
                    "update_info_dict_5",
                    "update_info_dict_10",
                    "dummy_update_stock_node_sequencer",
                ],
                name="update_stock_info_dict",
            ),
        ]
    )


def merge_info(
    primary_data_folder: str,
    feature_data_folder: str,
    years_window: List[str],
    stock_date_format: str,
    dummy_sequencer: str = "",
) -> str:
    """Merges information from different data sources.

    This function walks through the primary data folder and identifies csv files with specific endings.
    It then merges the identified files and saves the merged data into a new csv file in the feature data folder.

    Args:
        primary_data_folder (str): The path to the primary data folder.
        feature_data_folder (str): The path to the feature data folder.
        years_window (List[str]): A list of year periods for which the merging should be done.
        stock_date_format (str): The date format used in the stock data.
        dummy_sequencer (str, optional): A dummy sequencer to ensure pipeline runs in sequence. Defaults to "".

    Returns:
        str: An empty string. The function does not return any meaningful value but performs merging operation as a side effect.
    """
    print("=== Merging Info ===")
    for year_period in years_window:
        for root, dirs, files in os.walk(primary_data_folder):
            div_file = ""
            info_file = ""
            csv_file = ""

            for filename in files:
                file_path = os.path.join(root, filename)
                if filename.endswith("_div.csv"):
                    div_file = file_path
                elif filename.endswith("_info.csv"):
                    info_file = file_path
                    stock = filename.split("_")[0]
                else:
                    csv_file = file_path

            if csv_file != "":
                df_merged = _df_merging(
                    csv_file=csv_file,
                    info_file=info_file,
                    div_file=div_file,
                    date_format=stock_date_format,
                    years_window=year_period,
                )
                merged_file_path = os.path.join(
                    feature_data_folder,
                    stock,
                    stock + "_merged_" + str(year_period) + ".csv",
                )
                df_merged.to_csv(merged_file_path, index=False)

                print(f"=== Merged Info {merged_file_path} ===")

    print("=== Merging Info Completed ===")
    return ""


def _df_merging(
    csv_file: str, info_file: str, div_file: str, date_format: str, years_window: int
) -> pd.DataFrame:
    """Function will return merged dataframe based on input csv file, nav file and div file.

    Args:
        csv_file (str): csv file containing daily closing price
        info_file (str): csv file containing yearly financial related values (retrieved from financial report)
        div_file (str): csv file containing yearly dividend value (retrieved from financial report)
        date_format (str): date format of csv file
        years_window (int): time window from current year

    Returns:
        pd.DataFrame: contain info date, price, Volume, NAV, price_book
    """

    df = pd.read_csv(csv_file)
    df["Date"] = pd.to_datetime(df["Date"], format="%Y-%m-%d")
    df = df.rename(columns={"Date": "date", "Adj Close": "price"})
    df = df.drop(["Open", "High", "Low", "Close"], axis=1)
    df["year"] = df["date"].dt.year

    df_div = pd.read_csv(div_file)
    df_div["Date"] = pd.to_datetime(df_div["Date"], format=date_format)
    df_div = df_div.rename(columns={"Dividends": "DIV"})
    df_div["year"] = df_div["Date"].dt.year
    df_div_year = df_div.groupby("year")["DIV"].sum().reset_index()

    df_info = pd.read_csv(info_file)
    # df_nav = df_info[['year','NAV']]

    df_merged = df.merge(df_info, left_on="year", right_on="year", how="left")
    df_merged = df_merged.merge(
        df_div_year, left_on="year", right_on="year", how="left"
    )
    df_merged.ffill(inplace=True)

    df_merged["div_yield"] = df_merged["DIV"] / df_merged["price"] * 100

    df_merged["price_book"] = df_merged["price"] / df_merged["NAV"]

    # Filter rows that are within years_window years from the current year
    current_year = pd.to_datetime("today").year
    df_filtered = df_merged[df_merged["year"] > current_year - years_window]

    return df_filtered


def update_stock_info_dict(
    feature_data_folder: str,
    categories: str,
    stocks: Dict[str, str],
    info_dict_5: Dict[str, str],
    info_dict_10: Dict[str, str],
    dummy_sequencer: str = "",
):
    """
    Updates a dictionary with stock information.

    This function iterates over categories and stocks, reads merged data for each stock from a csv file,
    and updates the information dictionary for each stock.

    Args:
        feature_data_folder (str): The path to the feature data folder.
        categories (str): The categories of stocks.
        stocks (Dict[str, str]): A dictionary of stocks.
        info_dict (Dict[str, str]): A dictionary to be updated with stock information.
        dummy_sequencer (str, optional): A dummy sequencer to ensure pipeline runs in sequence. Defaults to "".

    Returns:
        Dict[str, str]: The updated dictionary with stock information.
    """
    print("=== Updating Stock Info ===")

    for category in categories:
        for stock in stocks[category]:
            stock_name = stock["name"]
            code_full = _get_stock_code_full(stock_name)
            code = code_full.split(".")[0]

            print(f"=== Updating Stock Info (5 years): {stock_name} ===")

            df_merged_path_5 = os.path.join(
                feature_data_folder, code, code + "_merged_5.csv"
            )
            df_merged_5 = pd.read_csv(df_merged_path_5)

            updated_info_dict_5 = _stock_info(df_merged_5, stock_name, info_dict_5)

            print(f"=== Updating Stock Info (10 years): {stock_name} ===")

            df_merged_path_10 = os.path.join(
                feature_data_folder, code, code + "_merged_10.csv"
            )
            df_merged_10 = pd.read_csv(df_merged_path_10)

            updated_info_dict_10 = _stock_info(df_merged_10, stock_name, info_dict_10)

    print("=== Updated Stock Info ===")

    return updated_info_dict_5, updated_info_dict_10, ""


def _stock_info(
    df_merged: pd.DataFrame, stock_name: str, info_dict: Dict[str, int]
) -> Dict[str, str]:
    """Function will generate dictionary containing info for each stock name

    Args:
        df_merged (pd.DataFrame): dataframe containing info date, price, Volume, NAV, price_book of stock
        stock_name (str): name of stock
        info_dict (Dict): existing dict if any

    Returns:
        Dict[str]: updated info_dict
    """
    # create new dictionary if stock has no existing info
    if stock_name not in info_dict.columns:
        info_dict[stock_name] = {}

    info_dict[stock_name]["last_updated"] = datetime.now().strftime("%Y-%m-%d, %H:%M")
    # print(info_dict[stock_name]["last_updated"])

    df_merged["date"] = pd.to_datetime(df_merged["date"])

    # Find the highest and lowest price-to-book ratio
    info_dict[stock_name]["max_pb"] = df_merged["price_book"].max()
    info_dict[stock_name]["min_pb"] = df_merged["price_book"].min()

    # Find the highest and lowest dividend yield
    info_dict[stock_name]["max_dy"] = df_merged["div_yield"].max()
    info_dict[stock_name]["min_dy"] = df_merged["div_yield"].min()

    # Find the dates for the highest and lowest price-to-book ratio
    info_dict[stock_name]["max_date_pb"] = df_merged["date"][
        df_merged["price_book"].idxmax()
    ].strftime("%Y-%m-%d")
    info_dict[stock_name]["min_date_pb"] = df_merged["date"][
        df_merged["price_book"].idxmin()
    ].strftime("%Y-%m-%d")

    # Find the dates for the highest and lowest dividend yield
    info_dict[stock_name]["max_date_dy"] = df_merged["date"][
        df_merged["div_yield"].idxmax()
    ].strftime("%Y-%m-%d")
    info_dict[stock_name]["min_date_dy"] = df_merged["date"][
        df_merged["div_yield"].idxmin()
    ].strftime("%Y-%m-%d")

    # Find the prices for the highest and lowest price-to-book ratio
    info_dict[stock_name]["max_price_pb"] = df_merged["price"][
        df_merged["price_book"].idxmax()
    ]
    info_dict[stock_name]["min_price_pb"] = df_merged["price"][
        df_merged["price_book"].idxmin()
    ]

    # Find the prices for the highest and lowest dividend yield
    info_dict[stock_name]["max_price_dy"] = df_merged["price"][
        df_merged["div_yield"].idxmax()
    ]
    info_dict[stock_name]["min_price_dy"] = df_merged["price"][
        df_merged["div_yield"].idxmin()
    ]

    # Find current date and price for price-to-book ratio and dividend yield
    # info_dict[stock_name]["cur_date"] = df_merged["date"].max().strftime("%Y-%m-%d")
    info_dict[stock_name]["cur_date"] = df_merged["date"].max()
    info_dict[stock_name]["cur_price"] = df_merged["price"][df_merged["date"].idxmax()]
    info_dict[stock_name]["cur_pb"] = df_merged["price_book"][
        df_merged["date"].idxmax()
    ]
    info_dict[stock_name]["cur_dy"] = df_merged["div_yield"][df_merged["date"].idxmax()]

    # Calculate the median, 1 standard deviation, and 2 standard deviations for pb
    median_pb = df_merged["price_book"].median()
    std_pb = df_merged["price_book"].std()
    info_dict[stock_name]["median_pb"] = median_pb
    info_dict[stock_name]["std_pb"] = std_pb
    info_dict[stock_name]["one_std_above_pb"] = median_pb + std_pb
    info_dict[stock_name]["one_std_below_pb"] = median_pb - std_pb
    info_dict[stock_name]["two_std_above_pb"] = median_pb + 2 * std_pb
    info_dict[stock_name]["two_std_below_pb"] = median_pb - 2 * std_pb

    # Calculate the median, 1 standard deviation, and 2 standard deviations for dy
    median_dy = df_merged["div_yield"].median()
    std_dy = df_merged["div_yield"].std()
    info_dict[stock_name]["median_dy"] = median_dy
    info_dict[stock_name]["std_dy"] = std_dy
    info_dict[stock_name]["one_std_above_dy"] = median_dy + std_dy
    info_dict[stock_name]["one_std_below_dy"] = median_dy - std_dy
    info_dict[stock_name]["two_std_above_dy"] = median_dy + 2 * std_dy
    info_dict[stock_name]["two_std_below_dy"] = median_dy - 2 * std_dy

    # Target price based on -2sd * NAV or lowest pb whichever lower
    info_dict[stock_name]["lowest_target_pb"] = max(
        info_dict[stock_name]["min_pb"], info_dict[stock_name]["two_std_below_pb"]
    )
    info_dict[stock_name]["target_price"] = (
        info_dict[stock_name]["lowest_target_pb"]
        * df_merged["NAV"][df_merged["date"].idxmax()]
    )

    info_dict[stock_name]["nav"] = df_merged["NAV"][df_merged["date"].idxmax()]

    return info_dict
