"""
This is a boilerplate pipeline 'download_raw_pipeline'
generated using Kedro 0.18.13
"""

from kedro.pipeline import Pipeline, pipeline
from .download_raw_nodes import download_process_move_raw_pipeline


def create_pipeline(**kwargs) -> Pipeline:
    merged_pipeline = download_process_move_raw_pipeline()
    return merged_pipeline
