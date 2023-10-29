from kedro.pipeline import Pipeline
from .download_raw_nodes import download_process_move_raw_pipeline


def create_pipeline(**kwargs) -> Pipeline:
    merged_pipeline = download_process_move_raw_pipeline()
    return merged_pipeline
