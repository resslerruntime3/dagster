# type: ignore
from solids import example_one_solid

from dagster._legacy import pipeline


@pipeline
def example_one_pipeline():
    example_one_solid()
