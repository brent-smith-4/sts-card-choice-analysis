import dagster as dg


class SparkResource(dg.ConfigurableResource):
    """Creates a local Spark session configured for Delta Lake reads/writes.

    master/max_result_size/arrow_enabled default to the Dagster assets' historical behavior
    (full local parallelism, no result-size cap, Arrow off) - the notebook path constructs this
    with different values (see analysis/win_rate_modeling.py) rather than changing the
    defaults here, so bronze/silver/gold are unaffected.
    """

    app_name: str = "sts-pipeline"
    driver_memory: str = "16g"
    shuffle_partitions: int = 100
    master: str = "local[*]"
    max_result_size: str | None = None
    arrow_enabled: bool = False

    def get_spark(self):
        from delta import configure_spark_with_delta_pip
        from pyspark.sql import SparkSession

        builder = (
            SparkSession.builder.master(self.master)
            .appName(self.app_name)
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .config("spark.driver.memory", self.driver_memory)
            .config("spark.sql.shuffle.partitions", str(self.shuffle_partitions))
        )
        if self.max_result_size is not None:
            builder = builder.config("spark.driver.maxResultSize", self.max_result_size)
        if self.arrow_enabled:
            builder = builder.config("spark.sql.execution.arrow.pyspark.enabled", "true")
        return configure_spark_with_delta_pip(builder).getOrCreate()
