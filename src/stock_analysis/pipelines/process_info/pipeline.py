from kedro.pipeline import Pipeline
from .process_info_nodes import process_info_pipeline


def create_pipeline(**kwargs) -> Pipeline:
    return process_info_pipeline()
