import dagster as dg

from sts_pipeline.assets import bronze, silver
from sts_pipeline.spark_resource import SparkResource

defs = dg.Definitions(
    assets=[bronze.bronze_runs, silver.silver_picks, silver.silver_card_offers],
    resources={"spark": SparkResource()},
)
