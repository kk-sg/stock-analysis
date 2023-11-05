from kedro.pipeline import Pipeline
from .plotting_info_nodes import plotting_info_pipeline


def create_pipeline(**kwargs) -> Pipeline:
    merged_pipeline = plotting_info_pipeline()
    return merged_pipeline
