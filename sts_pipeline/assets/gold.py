import dagster as dg
from pyspark.sql import functions as F

from sts_pipeline.assets.bronze import BRONZE_RUNS_DIR, bronze_runs
from sts_pipeline.assets.silver import (
    SILVER_CARD_OFFERS_DIR,
    SILVER_PICKS_DIR,
    silver_card_offers,
    silver_picks,
)
from sts_pipeline.spark_resource import SparkResource

GOLD_RUN_SUMMARY_DIR = "raw_data/gold/run_summary"
GOLD_CARD_CHOICE_EVENTS_DIR = "raw_data/gold/card_choice_events"
GOLD_RELIC_OFFERS_DIR = "raw_data/gold/relic_offers"

RELIC_OFFERS_CONFOUNDER_COLS = [
    "floor_reached", "victory", "character_chosen", "ascension_level",
    "neow_bonus", "neow_cost", "player_experience",
]


@dg.asset(
    deps=[bronze_runs, silver_picks],
    description=(
        "One row per run (play_id) — restricted to the same standard-run population as "
        "silver_picks, with run-level outcome fields plus derived path/pick counts."
    ),
)
def gold_run_summary(context: dg.AssetExecutionContext, spark: SparkResource) -> dg.MaterializeResult:
    session = spark.get_spark()

    runs = session.read.format("delta").load(BRONZE_RUNS_DIR)
    picks = session.read.format("delta").load(SILVER_PICKS_DIR)

    # Restrict to silver_picks' standard-run population without re-deriving its filter logic.
    valid_play_ids = picks.select("play_id").distinct()
    runs = runs.join(valid_play_ids, "play_id", "left_semi")

    pick_stats = picks.groupBy("play_id").agg(
        F.count("*").alias("pick_count"),
        F.sum(F.col("is_selected").cast("int")).alias("cards_picked"),
        F.sum((~F.col("is_selected")).cast("int")).alias("skip_count"),
    )

    summary = runs.select(
        "play_id",
        "victory",
        "floor_reached",
        "character_chosen",
        "ascension_level",
        "score",
        "playtime",
        "gold",
        "neow_bonus",
        "neow_cost",
        "player_experience",
        "killed_by",
        F.size("relics").alias("relic_count"),
        F.size(F.filter("path_taken", lambda x: x == "E")).alias("num_elites_fought"),
        F.size(F.filter("path_taken", lambda x: x == "?")).alias("num_events"),
        F.size(F.filter("path_taken", lambda x: x == "T")).alias("num_treasures"),
        F.size(F.filter("path_taken", lambda x: x == "$")).alias("num_shops"),
        "campfire_rested",
        "campfire_upgraded",
        F.size("potions_obtained").alias("num_potions_obtained"),
    ).join(pick_stats, "play_id", "left")

    summary.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(GOLD_RUN_SUMMARY_DIR)

    row_count = session.read.format("delta").load(GOLD_RUN_SUMMARY_DIR).count()
    context.log.info(f"Wrote {row_count} rows to {GOLD_RUN_SUMMARY_DIR}")

    session.stop()

    return dg.MaterializeResult(metadata={"row_count": row_count, "table_path": GOLD_RUN_SUMMARY_DIR})


@dg.asset(
    deps=[silver_card_offers],
    description=(
        "Gold-layer copy of silver_card_offers — the analysis-ready table for 'how do card "
        "choices impact run outcome': one row per card offered per pick-event (was_picked as "
        "the choice-model target, floors_gained/victory as the outcome-impact target)."
    ),
)
def gold_card_choice_events(context: dg.AssetExecutionContext, spark: SparkResource) -> dg.MaterializeResult:
    session = spark.get_spark()

    offers = session.read.format("delta").load(SILVER_CARD_OFFERS_DIR)

    offers.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(GOLD_CARD_CHOICE_EVENTS_DIR)

    row_count = session.read.format("delta").load(GOLD_CARD_CHOICE_EVENTS_DIR).count()
    context.log.info(f"Wrote {row_count} rows to {GOLD_CARD_CHOICE_EVENTS_DIR}")

    session.stop()

    return dg.MaterializeResult(metadata={"row_count": row_count, "table_path": GOLD_CARD_CHOICE_EVENTS_DIR})


@dg.asset(
    deps=[bronze_runs, silver_picks],
    description=(
        "One row per boss relic offered (picked or not) across the up-to-3 boss-relic choice "
        "screens in a run — long/tidy format mirroring gold_card_choice_events, but for boss "
        "relics. boss_relics isn't tracked in relics_obtained so no per-choice floor is "
        "available; boss_relic_tier (0/1/2 = after the Act 1/2/3 boss) stands in for it."
    ),
)
def gold_relic_offers(context: dg.AssetExecutionContext, spark: SparkResource) -> dg.MaterializeResult:
    session = spark.get_spark()

    runs = session.read.format("delta").load(BRONZE_RUNS_DIR)
    picks = session.read.format("delta").load(SILVER_PICKS_DIR)

    # Restrict to silver_picks' standard-run population without re-deriving its filter logic.
    valid_play_ids = picks.select("play_id").distinct()
    runs = runs.join(valid_play_ids, "play_id", "left_semi").withColumn("relic_count", F.size("relics"))

    # boss_relic_tier <= 2: a standard run has at most 3 boss-relic screens (Act 1/2/3 bosses);
    # any higher tier is a leftover anomaly even within the standard-run population.
    exploded = runs.select(
        "play_id",
        F.posexplode("boss_relics").alias("boss_relic_tier", "event"),
        "relic_count",
        *RELIC_OFFERS_CONFOUNDER_COLS,
    ).filter(F.col("boss_relic_tier") <= 2)

    was_skip = F.col("event.picked").isNull()

    picked = exploded.filter(~was_skip).select(
        "play_id", "boss_relic_tier",
        F.col("event.picked").alias("relic_name"),
        F.lit(True).alias("was_picked"),
        was_skip.alias("was_skip"),
        "relic_count",
        *RELIC_OFFERS_CONFOUNDER_COLS,
    )

    not_picked = exploded.select(
        "play_id", "boss_relic_tier",
        F.explode("event.not_picked").alias("relic_name"),
        F.lit(False).alias("was_picked"),
        was_skip.alias("was_skip"),
        "relic_count",
        *RELIC_OFFERS_CONFOUNDER_COLS,
    )

    offers = picked.unionByName(not_picked)

    offers.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(GOLD_RELIC_OFFERS_DIR)

    row_count = session.read.format("delta").load(GOLD_RELIC_OFFERS_DIR).count()
    context.log.info(f"Wrote {row_count} rows to {GOLD_RELIC_OFFERS_DIR}")

    session.stop()

    return dg.MaterializeResult(metadata={"row_count": row_count, "table_path": GOLD_RELIC_OFFERS_DIR})
