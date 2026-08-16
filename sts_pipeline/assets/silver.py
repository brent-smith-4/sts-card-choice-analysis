import dagster as dg
from pyspark.sql import functions as F

from sts_pipeline.assets.bronze import BRONZE_RUNS_DIR, bronze_runs
from sts_pipeline.spark_resource import SparkResource

SILVER_PICKS_DIR = "raw_data/silver/picks"
SILVER_CARD_OFFERS_DIR = "raw_data/silver/card_offers"

FLOOR_REACHED_MIN = 5
FLOOR_REACHED_MAX = 57  # Act 4 Heart kill

CARD_OFFERS_CONFOUNDER_COLS = [
    "floor_reached", "victory", "character_chosen", "ascension_level",
    "current_hp", "max_hp", "relic_count", "neow_bonus", "neow_cost",
    "player_experience", "floors_gained",
]


@dg.asset(
    deps=[bronze_runs],
    description="One row per card-reward pick-event, filtered to standard runs and enriched with confounders.",
)
def silver_picks(context: dg.AssetExecutionContext, spark: SparkResource) -> dg.MaterializeResult:
    session = spark.get_spark()

    runs = session.read.format("delta").load(BRONZE_RUNS_DIR)

    # Drop early-quit / non-standard-mode runs (see 00_problem_definition.ipynb: abandoned-run issue).
    filtered = runs.filter(
        (~F.col("is_endless"))
        & (~F.col("is_daily"))
        & (~F.col("is_trial"))
        & (~F.col("chose_seed"))
        & (F.col("floor_reached") >= FLOOR_REACHED_MIN)
    )

    # Drop custom/non-standard-mode runs: floor_reached above the max legitimate floor, or a
    # card_choices floor beyond floor_reached (both catch the same class of anomalous runs).
    max_pick_floor = F.array_max(F.transform("card_choices", lambda x: x["floor"]))
    anomalous = (F.col("floor_reached") > FLOOR_REACHED_MAX) | (max_pick_floor > F.col("floor_reached"))
    filtered = filtered.filter(~anomalous)

    exploded = filtered.select(
        "play_id", "floor_reached", "victory", "character_chosen", "ascension_level",
        "current_hp_per_floor", "max_hp_per_floor", "relics_obtained",
        "neow_bonus", "neow_cost", "player_experience",
        F.posexplode("card_choices").alias("pick_num", "event"),
    ).filter(F.col("event.floor").isNotNull())

    f = F.col("event.floor").cast("int")
    is_skip = F.col("event.picked") == "SKIP"

    def hp_at_floor(arr_col):
        # 0-based index anchored to floor_reached from the array's end: some HP arrays are
        # shorter than floor_reached with their *last* entry still aligned to it.
        idx = f - F.col("floor_reached") + F.size(arr_col) - 1
        in_bounds = (idx >= 0) & (idx < F.size(arr_col))
        return F.when(in_bounds, F.element_at(arr_col, (idx + 1).cast("int"))).otherwise(F.lit(None))

    picks = exploded.select(
        "play_id",
        "pick_num",
        F.when(~is_skip, F.col("event.picked")).otherwise(F.lit(None)).alias("card"),
        (~is_skip).alias("is_selected"),
        f.alias("choice_floor"),
        F.col("event.not_picked").alias("not_picked_options"),
        "floor_reached",
        "victory",
        "character_chosen",
        "ascension_level",
        hp_at_floor(F.col("current_hp_per_floor")).alias("current_hp"),
        hp_at_floor(F.col("max_hp_per_floor")).alias("max_hp"),
        F.size(F.filter("relics_obtained", lambda r: r["floor"] <= f)).alias("relic_count"),
        "neow_bonus",
        "neow_cost",
        "player_experience",
    )

    picks = picks.withColumn("floors_gained", F.col("floor_reached") - F.col("choice_floor"))

    picks.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(SILVER_PICKS_DIR)

    row_count = session.read.format("delta").load(SILVER_PICKS_DIR).count()
    context.log.info(f"Wrote {row_count} rows to {SILVER_PICKS_DIR}")

    session.stop()

    return dg.MaterializeResult(metadata={"row_count": row_count, "table_path": SILVER_PICKS_DIR})


@dg.asset(
    deps=[silver_picks],
    description=(
        "One row per card offered in a card-reward screen, picked or not — long/tidy format for "
        "choice modeling. `play_id` + `pick_num` identifies the choice occasion: exactly one row "
        "per occasion has was_picked=True, unless was_skip (the occasion was a SKIP) is True, in "
        "which case every row for that occasion has was_picked=False."
    ),
)
def silver_card_offers(context: dg.AssetExecutionContext, spark: SparkResource) -> dg.MaterializeResult:
    session = spark.get_spark()

    picks = session.read.format("delta").load(SILVER_PICKS_DIR)

    was_skip = ~F.col("is_selected")

    picked = picks.filter(F.col("is_selected")).select(
        "play_id", "pick_num",
        F.col("card").alias("card_name"),
        F.lit(True).alias("was_picked"),
        was_skip.alias("was_skip"),
        F.col("choice_floor").alias("floor"),
        *CARD_OFFERS_CONFOUNDER_COLS,
    )

    not_picked = picks.select(
        "play_id", "pick_num",
        F.explode("not_picked_options").alias("card_name"),
        F.lit(False).alias("was_picked"),
        was_skip.alias("was_skip"),
        F.col("choice_floor").alias("floor"),
        *CARD_OFFERS_CONFOUNDER_COLS,
    )

    offers = picked.unionByName(not_picked)

    offers.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(SILVER_CARD_OFFERS_DIR)

    row_count = session.read.format("delta").load(SILVER_CARD_OFFERS_DIR).count()
    context.log.info(f"Wrote {row_count} rows to {SILVER_CARD_OFFERS_DIR}")

    session.stop()

    return dg.MaterializeResult(metadata={"row_count": row_count, "table_path": SILVER_CARD_OFFERS_DIR})
