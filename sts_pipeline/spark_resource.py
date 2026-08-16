import dagster as dg


class SparkResource(dg.ConfigurableResource):
    """Creates a local Spark session configured for Delta Lake reads/writes."""

    app_name: str = "sts-pipeline"
    driver_memory: str = "8g"
    shuffle_partitions: int = 32

    def get_spark(self):
        from delta import configure_spark_with_delta_pip
        from pyspark.sql import SparkSession

        builder = (
            SparkSession.builder.master("local[*]")
            .appName(self.app_name)
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .config("spark.driver.memory", self.driver_memory)
            .config("spark.sql.shuffle.partitions", str(self.shuffle_partitions))
        )
        return configure_spark_with_delta_pip(builder).getOrCreate()
