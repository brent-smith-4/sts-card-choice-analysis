import dagster as dg

from sts_pipeline.assets import bronze, gold, silver
from sts_pipeline.spark_resource import SparkResource

defs = dg.Definitions(
    assets=[
        bronze.bronze_runs,
        silver.silver_picks,
        silver.silver_card_offers,
        gold.gold_run_summary,
        gold.gold_card_choice_events,
        gold.gold_relic_offers,
    ],
    resources={"spark": SparkResource()},
)
