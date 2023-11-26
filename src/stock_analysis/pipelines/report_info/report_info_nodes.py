import os
import pandas as pd
import subprocess
from subprocess import CalledProcessError
from kedro.pipeline import Pipeline, node, pipeline
from typing import Dict, List
from datetime import datetime
from stock_analysis.pipelines.download_process_raw.download_raw_nodes import (
    _get_stock_code_full,
)

BAR_FORMAT = "{l_bar}{bar:10}{r_bar}{bar:-10b}"


def report_info_pipeline(**kwargs) -> Pipeline:
    return pipeline(
        [
            node(
                func=report_md,
                inputs=[
                    "params:report_folder",
                    "params:stocks",
                    "params:categories",
                    "params:years_window",
                    "update_info_dict_5",
                    "update_info_dict_10",
                    "dummy_plot_info_pipeline_sequencer",
                ],
                outputs="dummy_report_info_pipeline_sequencer",
                name="report_md",
            ),
            node(
                func=update_toc_yml,
                inputs=[
                    "params:report_folder",
                    "params:stocks",
                    "params:categories",
                    "params:years_window",
                    "params:report_types",
                    "dummy_report_info_pipeline_sequencer",
                ],
                outputs="dummy_update_toc_yml_pipeline_sequencer",
                name="update_toc_yml",
            ),
            node(
                func=jupyter_book_build_node,
                inputs=[
                    "params:documentation",
                    "dummy_update_toc_yml_pipeline_sequencer",
                ],
                outputs=None,
                name="jupyter_book_build_node",
            ),
        ]
    )


def jupyter_book_build_node(
    params: Dict[str, str],
    dummy_sequencer: str = "",
) -> None:
    # If the 'publish' key is True, build the Jupyter book
    if params["publish"]:
        command = ["jupyter-book", "build", "./docs/stockinfo"]
        subprocess.run(command, check=True)

    # If the 'view_html' key is True, run the built Jupyter book in a Docker container
    if params["view_html"]:
        # Get the absolute path of the folder containing the built HTML files
        docker_mounting_html_folder = os.path.abspath(params["folder_to_view"])

        # Define the Docker container directory as read-only
        docker_container_dir_read_only = "/usr/share/nginx/html:ro"

        # Define the Docker mount as read-only
        docker_mount_read = (
            f"{docker_mounting_html_folder}:{docker_container_dir_read_only}"
        )

        # Define the Docker command to run the Docker container
        docker_command = [
            "docker",
            "run",
            "-d",
            "-p",
            "8080:80",
            "-v",
            docker_mount_read,
            "nginx:latest",
        ]

        # Run the Docker command
        try:
            subprocess.run(docker_command, check=True)
        except CalledProcessError:
            print("Opening firefox browser")

        # launch firefox
        subprocess.run(["firefox", "http://localhost:8080"], check=True)


def update_toc_yml(
    report_folder: str,
    stocks: Dict[str, str],
    categories: List[str],
    years_window: List[str],
    report_types: Dict[str, str],
    dummy_sequencer: str = "",
) -> str:
    content_toc_string = _return_content_toc_string(
        categories, stocks, years_window, report_types
    )

    print(content_toc_string)
    _save_toc_yml_file(report_folder, content_toc_string)

    return ""


def _return_content_toc_string(categories, stocks, years_window, report_types):
    content_string = f"""format: jb-book
root: index
parts:
    """

    for category in categories:
        content_string = (
            content_string
            + f"""
  - caption: {category}
    numbered: true
    chapters:
    """
        )

        for stock in stocks[category]:
            stock_name = stock["name"]
            code_full = _get_stock_code_full(stock["name"])
            code = code_full.split(".")[0]

            for report_type_key, report_type_value in report_types.items():
                for year_period in years_window:
                    content_string = (
                        content_string
                        + f"""
        - title: {stock_name} ({report_type_value}, {year_period} years)
          file: {code}/{code}_{report_type_key}_{year_period}_years.md
    """
                    )
    return content_string


def _save_toc_yml_file(report_folder: str, content_toc_string: str):
    filename = os.path.join(report_folder, "_toc.yml")

    # Write the string to a .md file
    with open(filename, "w") as f:
        f.write(content_toc_string)


def report_md(
    report_folder: str,
    stocks: Dict[str, str],
    categories: List[str],
    years_window: List[str],
    update_info_dict_5: Dict[str, str],
    update_info_dict_10: Dict[str, str],
    dummy_sequencer: str = "",
) -> str:
    """Returns the price-book info summary of stock_name saved in .md format

    Args:
        stock_name (str): stock name
        info_dict (Dict[str, float]): info dictionary containing info of stock
        years_window (int): time window from current year
    """
    for year_period in years_window:
        for category in categories:
            for stock in stocks[category]:
                stock_name = stock["name"]
                code_full = _get_stock_code_full(stock["name"])
                code = code_full.split(".")[0]

                if year_period == 5:
                    info_dict = update_info_dict_5
                if year_period == 10:
                    info_dict = update_info_dict_10

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
                cur_date = datetime.fromtimestamp(
                    info_dict[stock_name]["cur_date"] / 1000
                )
                cur_price = info_dict[stock_name]["cur_price"]
                cur_pb = info_dict[stock_name]["cur_pb"]

                # Calculate the median, 1 standard deviation, and 2 standard deviations for pb
                median_pb = info_dict[stock_name]["median_pb"]
                std_pb = info_dict[stock_name]["std_pb"]
                two_std_below_pb = median_pb - 2 * std_pb

                # Target price based on -2sd * NAV or lowest pb whichever lower
                lowest_target_pb = info_dict[stock_name]["lowest_target_pb"]
                target_price = info_dict[stock_name]["target_price"]

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
                cur_dy = info_dict[stock_name]["cur_dy"]

                # Calculate the median, 1 standard deviation, and 2 standard deviations for dy
                median_dy = info_dict[stock_name]["median_dy"]
                std_dy = info_dict[stock_name]["std_dy"]
                two_std_above_dy = median_dy + 2 * std_dy

                # Target price based on +2sd or max div yield whichever higher
                highest_target_dy = min(max_dy, two_std_above_dy)

                img_path_pb = os.path.join(code + "_pb_" + str(year_period) + ".png")
                img_path_price_nav = os.path.join(
                    code + "_price_nav_" + str(year_period) + ".png"
                )

                img_path_div_yield = os.path.join(
                    code + "_div_" + str(year_period) + ".png"
                )
                img_path_yearly_dpu = os.path.join(code + "_yearly_dpu.png")

                content_md_pb = _return_content_md_pb_string(
                    stock_name,
                    year_period,
                    lowest_target_pb,
                    target_price,
                    cur_pb,
                    cur_price,
                    cur_date,
                    min_pb,
                    min_price_pb,
                    min_date_pb,
                    max_pb,
                    max_price_pb,
                    max_date_pb,
                    img_path_pb,
                    img_path_price_nav,
                )
                _save_indv_md_file(
                    report_folder,
                    code,
                    stock_name,
                    content_md_pb,
                    year_period,
                    type="pb",
                )

                content_md_dy = _return_content_md_dy_string(
                    stock_name,
                    year_period,
                    highest_target_dy,
                    cur_dy,
                    cur_price,
                    cur_date,
                    max_dy,
                    max_price_dy,
                    max_date_dy,
                    min_dy,
                    min_price_dy,
                    min_date_dy,
                    img_path_div_yield,
                    img_path_yearly_dpu,
                )
                _save_indv_md_file(
                    report_folder,
                    code,
                    stock_name,
                    content_md_dy,
                    year_period,
                    type="dy",
                )
    return ""


def _return_content_md_dy_string(
    stock_name: str,
    year_period: str,
    highest_target_dy: str,
    cur_dy: str,
    cur_price: str,
    cur_date: str,
    max_dy: str,
    max_price_dy: str,
    max_date_dy: str,
    min_dy: str,
    min_price_dy: str,
    min_date_dy: str,
    img_path_div_yield: str,
    img_path_yearly_dpu: str,
) -> str:
    content_string = f"""# {stock_name} Dividend Yield ({year_period} years data)\n
|     | Yield   | Price | Date       |
|-----|---------|-------|------------|
| Target | {highest_target_dy:.2f} |  |  |
| Current | {cur_dy:.2f} | {cur_price:.2f}  | {cur_date.strftime('%Y-%m-%d')} |
| Max | {max_dy:.2f} | {max_price_dy:.2f}  | {max_date_dy.strftime('%Y-%m-%d')} |
| Min | {min_dy:.2f} | {min_price_dy:.2f}  | {min_date_dy.strftime('%Y-%m-%d')} |

![Plot of Dividend Yield for {stock_name}]({img_path_div_yield})

![Plot of Annual Dividend Per Unit for {stock_name}]({img_path_yearly_dpu})
    """
    return content_string


def _return_content_md_pb_string(
    stock_name: str,
    year_period: str,
    lowest_target_pb: str,
    target_price: str,
    cur_pb: str,
    cur_price: str,
    cur_date: str,
    min_pb: str,
    min_price_pb: str,
    min_date_pb: str,
    max_pb: str,
    max_price_pb: str,
    max_date_pb: str,
    img_path_pb: str,
    img_path_price_nav: str,
) -> str:
    content_string = f"""# {stock_name} Price-Book ({year_period} years data)\n
|     | PB   | Price | Date       |
|-----|------|-------|------------|
| Target | {lowest_target_pb:.2f} | {target_price:.2f}  |  |
| Current | {cur_pb:.2f} | {cur_price:.2f}  | {cur_date.strftime('%Y-%m-%d')} |
| Min | {min_pb:.2f} | {min_price_pb:.2f}  | {min_date_pb.strftime('%Y-%m-%d')} |
| Max | {max_pb:.2f} | {max_price_pb:.2f}  | {max_date_pb.strftime('%Y-%m-%d')} |

Last updated: {cur_date.strftime('%Y-%m-%d')}

![Plot of Price-Book ratio for {stock_name}]({img_path_pb})

![Plot of Price with NAV for {stock_name}]({img_path_price_nav})
    """
    return content_string


def _save_indv_md_file(
    report_folder: str,
    code: str,
    stock_name: str,
    content: str,
    years_window: int,
    type: str,
):
    """Function will save the content into .md file

    Args:
        stock_name (str): name of stock
        content (str): content to be saved as .md file
        years_window (int): duration of the year window
    """

    filename = os.path.join(
        report_folder,
        code,
        code + "_" + str(type) + "_" + str(years_window) + "_years" + ".md",
    )

    # Write the string to a .md file
    with open(filename, "w") as f:
        f.write(content)
