from kedro.pipeline import Pipeline
from .report_info_nodes import report_info_pipeline


def create_pipeline(**kwargs) -> Pipeline:
    return report_info_pipeline()
