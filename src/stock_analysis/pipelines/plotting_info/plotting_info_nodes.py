import pandas as pd
import matplotlib.pyplot as plt
import os
from kedro.pipeline import Pipeline, node, pipeline
from typing import Dict, List

from datetime import datetime
from matplotlib.ticker import MaxNLocator
from stock_analysis.pipelines.download_process_raw.download_raw_nodes import (
    _get_stock_code_full,
)

BAR_FORMAT = "{l_bar}{bar:10}{r_bar}{bar:-10b}"


def plotting_info_pipeline(**kwargs) -> Pipeline:
    return pipeline(
        [
            node(
                func=plot_info,
                inputs=[
                    "params:feature_data_folder",
                    "params:report_folder",
                    "params:stocks",
                    "params:categories",
                    "params:years_window",
                    "update_info_dict_5",
                    "update_info_dict_10",
                    "dummy_update_stock_node_sequencer",
                ],
                outputs="dummy_plot_info_pipeline_sequencer",
                name="plot_info",
            ),
        ]
    )


def plot_info(
    feature_data_folder: str,
    report_folder: str,
    stocks: Dict[str, str],
    categories: List[str],
    years_window: List[str],
    update_info_dict_5: Dict[str, str],
    update_info_dict_10: Dict[str, str],
    dummy_sequencer: str = "",
):
    print("=== Plotting Info ===")

    for year_period in years_window:
        for category in categories:
            for stock in stocks[category]:
                stock_name = stock["name"]
                code_full = _get_stock_code_full(stock["name"])
                code = code_full.split(".")[0]

                info_code_path = os.path.join(feature_data_folder, code)
                plot_code_path = os.path.join(report_folder, code)
                merged_info_path = os.path.join(
                    info_code_path, code + "_merged_" + str(year_period) + ".csv"
                )

                df_merged = pd.read_csv(merged_info_path)

                if year_period == 5:
                    update_info_dict = update_info_dict_5
                if year_period == 10:
                    update_info_dict = update_info_dict_10

                # saving plots showing prices and dividend yield
                _plot_price_dividend_yield(
                    info_code_path,
                    plot_code_path,
                    df_merged,
                    stock_name,
                    year_period,
                    update_info_dict,
                )

                # saving plots showing yearly dividend distribution
                _plot_yearly_dividend(
                    info_code_path,
                    plot_code_path,
                    df_merged,
                    stock_name,
                    update_info_dict,
                )

                # saving plots showing price book of stock
                _plot_price_book(
                    info_code_path,
                    plot_code_path,
                    df_merged,
                    stock_name,
                    year_period,
                    update_info_dict,
                )

                # saving plots showing NAV
                _plot_price_nav(
                    info_code_path,
                    plot_code_path,
                    df_merged,
                    stock_name,
                    year_period,
                    update_info_dict,
                )

    print("=== Plotted All Info ===")

    return ""


def _plot_price_dividend_yield(
    info_code_path: str,
    plot_code_path: str,
    df_merged: pd.DataFrame,
    stock_name: str,
    years_window: int,
    info_dict: Dict[str, int],
) -> None:
    """Function will saving plots showing prices and dividend yield of stock

    Args:
        info_code_path(str): path to save plot
        df_merged (pd.DataFrame): contain info date, price, Volume, NAV, price_book
        stock_name (str): name of stock
        info_dict (Dict): existing dict if any
        years_window (int): time window from current year
    """

    # Find the highest and lowest dividend yield
    max_dy = info_dict[stock_name]["max_dy"]
    min_dy = info_dict[stock_name]["min_dy"]

    # Find the dates for the highest and lowest dividend yield
    max_date_dy = pd.to_datetime(info_dict[stock_name]["max_date_dy"])
    min_date_dy = pd.to_datetime(info_dict[stock_name]["min_date_dy"])

    # Find the prices for the highest and lowest dividend yield
    max_price_dy = info_dict[stock_name]["max_price_dy"]
    min_price_dy = info_dict[stock_name]["min_price_dy"]

    # Find current date and price for price-to-book ratio and dividend yield
    cur_date = datetime.fromtimestamp(info_dict[stock_name]["cur_date"] / 1000)
    cur_price = info_dict[stock_name]["cur_price"]
    cur_dy = info_dict[stock_name]["cur_dy"]

    # # Calculate the median, 1 standard deviation, and 2 standard deviations for pb
    # median_pb = info_dict[stock_name]["median_pb"]
    # std_pb = info_dict[stock_name]["std_pb"]
    # two_std_below_pb = median_pb - 2 * std_pb

    # Calculate the median, 1 standard deviation, and 2 standard deviations for dy
    median_dy = info_dict[stock_name]["median_dy"]
    std_dy = info_dict[stock_name]["std_dy"]
    one_std_above_dy = median_dy + std_dy
    one_std_below_dy = median_dy - std_dy
    two_std_above_dy = median_dy + 2 * std_dy
    two_std_below_dy = median_dy - 2 * std_dy

    fig, ax2 = plt.subplots(1, 1, figsize=(20, 8))

    # Plot dividend yield graph
    # Convert date strings (in the format YYYY-MM-DD) to datetime
    dates = [datetime.strptime(d, "%Y-%m-%d") for d in df_merged["date"]]
    ax2.plot(dates, df_merged["div_yield"], label="Dividend Yield")

    # Mark the highest and lowest dividend yield with a red dot
    ax2.plot(max_date_dy, max_dy, "ro")
    ax2.plot(min_date_dy, min_dy, "ro")

    # Mark the current price with a red dot
    ax2.plot(cur_date, cur_dy, "ro")

    # Set the labels and title
    ax2.set_xlabel("Date")
    ax2.set_ylabel("Dividend Yield Ratio")
    ax2.set_title(f"{stock_name} Dividend Yield Plot ({years_window} years data)")

    # Annotate the highest and lowest dividend yield
    ax2.annotate(
        f"Max Yield: {max_dy:.2f}\nPrice: {max_price_dy:.2f}\nDate: {max_date_dy.strftime('%Y-%m-%d')}",
        xy=(max_date_dy, max_dy),
        xytext=(max_date_dy, max_dy * 0.98),
    )
    ax2.annotate(
        f"Min Yield: {min_dy:.2f}\nPrice: {min_price_dy:.2f}\nDate: {min_date_dy.strftime('%Y-%m-%d')}",
        xy=(min_date_dy, min_dy),
        xytext=(min_date_dy, min_dy * 0.99),
    )

    # Annotate the current price
    ax2.annotate(
        f"Current Yield: {cur_dy:.2f}\nPrice: {cur_price:.2f}\nDate: {cur_date.strftime('%Y-%m-%d')}",
        xy=(cur_date, cur_dy),
        xytext=(cur_date, cur_dy * 0.95),
    )

    # Plot the median, 1 standard deviation, and 2 standard deviations with a grey dashed line
    ax2.axhline(median_dy, color="skyblue", linestyle=":", label="Median")
    ax2.axhline(one_std_above_dy, color="skyblue", linestyle=":", label="+1sd")
    ax2.axhline(one_std_below_dy, color="skyblue", linestyle=":", label="-1sd")
    ax2.axhline(two_std_above_dy, color="skyblue", linestyle=":", label="+2sd")
    ax2.axhline(two_std_below_dy, color="skyblue", linestyle=":", label="-2sd")

    # Annotate the lines
    ax2.annotate(
        f"Median: {median_dy: .2f}",
        xy=(dates[1], median_dy),
        xytext=(-80, 2),
        textcoords="offset points",
    )
    ax2.annotate(
        f"+1sd: {one_std_above_dy: .2f}",
        xy=(dates[1], one_std_above_dy),
        xytext=(-80, 2),
        textcoords="offset points",
    )
    ax2.annotate(
        f"-1sd: {one_std_below_dy: .2f}",
        xy=(dates[1], one_std_below_dy),
        xytext=(-80, 2),
        textcoords="offset points",
    )
    ax2.annotate(
        f"+2sd: {two_std_above_dy: .2f}",
        xy=(dates[1], two_std_above_dy),
        xytext=(-80, 2),
        textcoords="offset points",
    )
    ax2.annotate(
        f"-2sd: {two_std_below_dy: .2f}",
        xy=(dates[1], two_std_below_dy),
        xytext=(-80, 2),
        textcoords="offset points",
    )

    # Add 10% padding to the x-axis and 10% padding to the y-axis
    ax2.margins(x=0.15, y=0.1)

    # Get stock code
    code_full = _get_stock_code_full(stock_name)
    code = code_full.split(".")[0]

    # Save plot
    plt.savefig(
        os.path.join(plot_code_path, code + "_div_" + str(years_window) + ".png")
    )
    plt.close()

    print(f"=== Saved Prices with Dividend Yield Plot: {stock_name} ===")


def _plot_yearly_dividend(
    info_code_path: str,
    plot_code_path: str,
    df_merged: pd.DataFrame,
    stock_name: str,
    info_dict: Dict[str, int],
) -> None:
    """Function will save the plot of the yealy dividend distribution record of stock

    Args:
        info_code_path(str): path to save plot
        df_merged (pd.DataFrame): contain info date, price, Volume, NAV, price_book
        stock_name (str): name of stock
        info_dict (Dict): existing dict if any
    """

    fig, ax1 = plt.subplots(1, 1, figsize=(20, 8))

    # Plot dividend yield graph
    ax1.bar(df_merged["year"], df_merged["DIV"], label="Dividend")

    # Set the labels and title
    ax1.set_xlabel("Year")
    ax1.set_ylabel("Dividends")
    ax1.set_title(f"{stock_name} Dividend Records")

    # Add annotations
    for year, div in zip(df_merged["year"], df_merged["DIV"]):
        plt.text(year, div * 1.05, str(round(div, 2)), ha="center")

    # Set x-axis to only use integer values
    ax1.xaxis.set_major_locator(MaxNLocator(integer=True))

    # Add 10% padding to the x-axis and 10% padding to the y-axis
    ax1.margins(x=0.15, y=0.1)

    # Create a second y-axis that shares the same x-axis
    # ax2 = ax1.twinx()

    # Get stock code
    code_full = _get_stock_code_full(stock_name)
    code = code_full.split(".")[0]

    # Add a legend
    plt.legend(loc="best")

    # Save plot
    plt.savefig(os.path.join(plot_code_path, code + "_yearly_dpu.png"))
    plt.close()

    print(f"=== Saved Yearly Dividend Distribution Plot: {stock_name} ===")


def _plot_price_book(
    info_code_path: str,
    plot_code_path: str,
    df_merged: pd.DataFrame,
    stock_name: str,
    years_window: int,
    info_dict: Dict[str, int],
) -> None:
    """Function will save the plot of the price-book ratio of stock

    Args:
        info_code_path(str): path to save plot
        df_merged (pd.DataFrame): contain info date, price, Volume, NAV, price_book
        stock_name (str): name of stock
        info_dict (Dict): existing dict if any
        years_window (int): time window from current year
    """

    # Find the highest and lowest price-to-book ratio
    max_pb = info_dict[stock_name]["max_pb"]
    min_pb = info_dict[stock_name]["min_pb"]

    # Find the dates for the highest and lowest price-to-book ratio
    max_date_pb = pd.to_datetime(info_dict[stock_name]["max_date_pb"])
    min_date_pb = pd.to_datetime(info_dict[stock_name]["min_date_pb"])

    # Find the prices for the highest and lowest price-to-book ratio
    max_price_pb = info_dict[stock_name]["max_price_pb"]
    min_price_pb = info_dict[stock_name]["min_price_pb"]

    # Find current date and price for price-to-book ratio and dividend yield
    cur_date = datetime.fromtimestamp(info_dict[stock_name]["cur_date"] / 1000)
    cur_price = info_dict[stock_name]["cur_price"]
    cur_pb = info_dict[stock_name]["cur_pb"]

    # Target price based on -2sd * NAV or lowest pb whichever lower
    target_price = info_dict[stock_name]["target_price"]
    nav = info_dict[stock_name]["nav"]
    target_pb = target_price / nav

    # Calculate the median, 1 standard deviation, and 2 standard deviations for pb
    median_pb = info_dict[stock_name]["median_pb"]
    std_pb = info_dict[stock_name]["std_pb"]
    one_std_above_pb = median_pb + std_pb
    one_std_below_pb = median_pb - std_pb
    two_std_above_pb = median_pb + 2 * std_pb
    two_std_below_pb = median_pb - 2 * std_pb

    fig, ax1 = plt.subplots(1, 1, figsize=(20, 8))

    # Plot 'price-book' against 'date'
    # Convert date strings (in the format YYYY-MM-DD) to datetime
    dates = [datetime.strptime(d, "%Y-%m-%d") for d in df_merged["date"]]
    ax1.plot(dates, df_merged["price_book"], label="Price-Book")

    # Mark the highest and lowest price with a red dot
    ax1.plot(max_date_pb, max_pb, "ro")
    ax1.plot(min_date_pb, min_pb, "ro")

    # Mark the current price with a red dot
    ax1.plot(cur_date, cur_pb, "ro")

    # Mark the target price with a red dot
    ax1.plot(cur_date, target_pb, marker="*", markersize=10, color="orange")

    # Annotate the highest and lowest price
    ax1.annotate(
        f"Max PB: {max_pb:.2f}\nPrice: {max_price_pb:.2f}\nDate: {max_date_pb.strftime('%Y-%m-%d')}",
        xy=(max_date_pb, max_pb),
        xytext=(max_date_pb, max_pb * 0.98),
    )
    ax1.annotate(
        f"Min PB: {min_pb:.2f}\nPrice: {min_price_pb:.2f}\nDate: {min_date_pb.strftime('%Y-%m-%d')}",
        xy=(min_date_pb, min_pb),
        xytext=(min_date_pb, min_pb * 0.99),
    )

    # Annotate the current price
    ax1.annotate(
        f"Current PB: {cur_pb:.2f}\nPrice: {cur_price:.2f}\nDate: {cur_date.strftime('%Y-%m-%d')}",
        xy=(cur_date, cur_pb),
        xytext=(cur_date, cur_pb * 1.05),
    )

    # Annotate the target price
    ax1.annotate(
        f"Target PB: {target_pb:.2f}\nPrice: {target_price:.2f}",
        xy=(cur_date, target_pb),
        xytext=(cur_date, target_pb * 0.9),
    )

    # Plot the median, 1 standard deviation, and 2 standard deviations with a grey dashed line
    ax1.axhline(median_pb, color="skyblue", linestyle=":", label="Median")
    ax1.axhline(one_std_above_pb, color="skyblue", linestyle=":", label="+1sd")
    ax1.axhline(one_std_below_pb, color="skyblue", linestyle=":", label="-1sd")
    ax1.axhline(two_std_above_pb, color="skyblue", linestyle=":", label="+2sd")
    ax1.axhline(two_std_below_pb, color="skyblue", linestyle=":", label="-2sd")

    # Annotate the lines
    ax1.annotate(
        f"Median: {median_pb: .2f}",
        xy=(dates[1], median_pb),
        xytext=(-80, 2),
        textcoords="offset points",
    )
    ax1.annotate(
        f"+1sd: {one_std_above_pb: .2f}",
        xy=(dates[1], one_std_above_pb),
        xytext=(-80, 2),
        textcoords="offset points",
    )
    ax1.annotate(
        f"-1sd: {one_std_below_pb: .2f}",
        xy=(dates[1], one_std_below_pb),
        xytext=(-80, 2),
        textcoords="offset points",
    )
    ax1.annotate(
        f"+2sd: {two_std_above_pb: .2f}",
        xy=(dates[1], two_std_above_pb),
        xytext=(-80, 2),
        textcoords="offset points",
    )
    ax1.annotate(
        f"-2sd: {two_std_below_pb: .2f}",
        xy=(dates[1], two_std_below_pb),
        xytext=(-80, 2),
        textcoords="offset points",
    )

    # Set the labels and title
    ax1.set_xlabel("Date")
    ax1.set_ylabel("Price-to-Book Ratio")
    ax1.set_title(f"{stock_name} Price-Book Plot ({years_window} years data)")

    # # Add 10% padding to the x-axis and 10% padding to the y-axis
    ax1.margins(x=0.15, y=0.1)

    # Get stock code
    code_full = _get_stock_code_full(stock_name)
    code = code_full.split(".")[0]

    # Save plot
    plt.savefig(
        os.path.join(plot_code_path, code + "_pb_" + str(years_window) + ".png")
    )
    plt.close()

    print(f"=== Saved Prices Book Plot: {stock_name} ===")


def _plot_price_nav(
    info_code_path: str,
    plot_code_path: str,
    df_merged: pd.DataFrame,
    stock_name: str,
    years_window: int,
    info_dict: Dict[str, int],
) -> None:
    """Function will save the plot of the price and nav of stock

    Args:
        info_code_path(str): path to save plot
        df_merged (pd.DataFrame): contain info date, price, Volume, NAV, price_book
        stock_name (str): name of stock
        info_dict (Dict): existing dict if any
        years_window (int): time window from current year
    """

    # Find the dates for the highest and lowest price-to-book ratio
    max_date_pb = pd.to_datetime(info_dict[stock_name]["max_date_pb"])
    min_date_pb = pd.to_datetime(info_dict[stock_name]["min_date_pb"])

    # Find the highest and lowest price
    max_price = df_merged[pd.to_datetime(df_merged["date"]) == max_date_pb]["price"]
    min_price = df_merged[pd.to_datetime(df_merged["date"]) == min_date_pb]["price"]

    # Find current date and price for price-to-book ratio and dividend yield
    cur_date = datetime.fromtimestamp(info_dict[stock_name]["cur_date"] / 1000)
    cur_price = info_dict[stock_name]["cur_price"]

    # NAV
    cur_nav = info_dict[stock_name]["nav"]

    # Target price based on -2sd * NAV or lowest pb whichever lower
    target_price = info_dict[stock_name]["target_price"]

    fig, ax1 = plt.subplots(1, 1, figsize=(20, 8))

    # Plot 'price' against 'date'
    # Convert date strings (in the format YYYY-MM-DD) to datetime
    dates = [datetime.strptime(d, "%Y-%m-%d") for d in df_merged["date"]]
    ax1.plot(dates, df_merged["price"], label="Price")

    # Plot 'price' against 'date'
    ax1.plot(dates, df_merged["NAV"], color="orange", label="NAV")

    # Mark the highest and lowest price with a red dot
    ax1.plot(max_date_pb, max_price, "ro")
    ax1.plot(min_date_pb, min_price, "ro")

    # Mark the current price with a red dot
    ax1.plot(cur_date, cur_price, "ro")

    # Mark the target price with a red dot
    ax1.plot(cur_date, target_price, marker="*", markersize=10, color="orange")

    # Annotate the current price
    ax1.annotate(
        f"Current Price: {cur_price:.2f}\nDate: {cur_date.strftime('%Y-%m-%d')}",
        xy=(cur_date, cur_price),
        xytext=(cur_date, cur_price * 1.05),
    )

    # Annotate the target price
    ax1.annotate(
        f"Target Price: {target_price:.2f}",
        xy=(cur_date, target_price),
        xytext=(cur_date, target_price * 0.9),
    )

    # Set the labels and title
    ax1.set_xlabel("Date")
    ax1.set_ylabel("Price, NAV")
    # ax2.set_ylabel("NAV")
    ax1.set_title(f"{stock_name} Prices with NAV Plot ({years_window} years data)")

    # # Add 10% padding to the x-axis and 10% padding to the y-axis
    ax1.margins(x=0.15, y=0.1)

    # Add a legend
    plt.legend(loc="best")

    # Get stock code
    code_full = _get_stock_code_full(stock_name)
    code = code_full.split(".")[0]

    # Save plot
    plt.savefig(
        os.path.join(plot_code_path, code + "_price_nav_" + str(years_window) + ".png")
    )
    plt.close()

    print(f"=== Saved Prices with NAV Plot: {stock_name} ===")
