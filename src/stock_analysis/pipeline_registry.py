"""Project pipelines."""
from __future__ import annotations

from kedro.framework.project import find_pipelines
from kedro.pipeline import Pipeline

from stock_analysis.pipelines.download_process_raw import download_raw_nodes as dr
from stock_analysis.pipelines.process_info import process_info_nodes as pi
from stock_analysis.pipelines.plotting_info import plotting_info_nodes as pl
from stock_analysis.pipelines.report_info import report_info_nodes as ri


def register_pipelines() -> dict[str, Pipeline]:
    """Register the project's pipelines.

    Returns:
        A mapping from pipeline names to ``Pipeline`` objects.
    """
    pipelines = find_pipelines()

    return {
        "__default__": sum(pipelines.values()),
        "raw": dr.download_process_move_raw_pipeline(),
        "process_info": pi.process_info_pipeline(),
        "plot_info": pl.plotting_info_pipeline(),
        "report_info": ri.report_info_pipeline(),
    }
