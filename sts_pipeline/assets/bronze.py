import dagster as dg
from pyspark.sql.functions import input_file_name

from sts_pipeline.spark_resource import SparkResource

LANDING_DIR = "raw_data/landing"
BRONZE_RUNS_DIR = "raw_data/bronze/runs"


@dg.asset(description="Raw Slay the Spire run records ingested from raw_data/landing, one row per run.")
def bronze_runs(context: dg.AssetExecutionContext, spark: SparkResource) -> dg.MaterializeResult:
    session = spark.get_spark()

    df = (
        session.read.option("multiLine", "true")
        .option("recursiveFileLookup", "true")
        .json(LANDING_DIR)
    )

    if "event" not in df.columns:
        raise ValueError(f"Expected top-level 'event' field in source JSON, got columns: {df.columns}")

    events = df.select("event.*").withColumn("_source_file", input_file_name())

    # Some months' landing data has monthly rollup files (e.g. november.json) that re-contain
    # runs already present as individual per-run/per-batch files under a dated subdirectory —
    # same play_id, identical content, different _source_file. Keep one copy per play_id.
    events = events.dropDuplicates(["play_id"])

    events.write.format("delta").mode("overwrite").save(BRONZE_RUNS_DIR)

    row_count = session.read.format("delta").load(BRONZE_RUNS_DIR).count()
    context.log.info(f"Wrote {row_count} rows to {BRONZE_RUNS_DIR}")

    session.stop()

    return dg.MaterializeResult(metadata={"row_count": row_count, "table_path": BRONZE_RUNS_DIR})
