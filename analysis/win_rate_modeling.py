"""Reusable data-collection and per-group model-fitting helpers for the analysis notebooks.

Kept separate from pipeline/assets/ - these aren't Dagster-materialized pipeline assets,
they're helpers the analysis notebooks call directly, so the notebook itself can stay a
readable record of what was decided and why rather than the mechanics of how the data got
collected and fit.
"""
import os
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow.parquet as pq
import statsmodels.formula.api as smf
from sklearn.metrics import roc_auc_score
from statsmodels.stats.multitest import multipletests


def _configure_spark_env(project_root: Path) -> None:
    """Set the JAVA_HOME/HADOOP_HOME/PYSPARK_* env vars Spark needs, before any pyspark import."""
    os.environ["JAVA_HOME"] = str(project_root / ".jdk17" / "jdk-17.0.20+8")
    os.environ["HADOOP_HOME"] = r"C:\hadoop"
    os.environ["PATH"] = str(project_root / ".venv" / "Scripts") + os.pathsep + r"C:\hadoop\bin" + os.pathsep + os.environ["PATH"]
    os.environ["PYSPARK_PYTHON"] = str(project_root / ".venv" / "Scripts" / "python.exe")
    os.environ["PYSPARK_DRIVER_PYTHON"] = str(project_root / ".venv" / "Scripts" / "python.exe")


def collect_win_rate_regression_input(project_root: Path, gold_path: str, output_parquet: str) -> pd.DataFrame:
    """Collect gold_card_choice_events into a pandas DataFrame, caching it to output_parquet.

    Collects per character rather than in one toPandas() call - a single collect over every
    card blows past spark.driver.maxResultSize and, past that, the JVM driver heap itself
    (collect() stages every row as boxed Java objects, far heavier than the on-disk Parquet
    size). Card pools are already partitioned by character_chosen, so chunking on it is free.

    Stops Spark before returning - nothing past this collection needs it, and leaving the JVM
    driver resident during the memory-heavy fit step downstream is what caused a real
    disk-swap incident (see FIXLOG.md 4.8).
    """
    _configure_spark_env(project_root)
    from pyspark.sql import functions as F

    from pipeline.spark_resource import SparkResource

    # local[4] rather than SparkResource's local[*] default - full parallelism here competes
    # with the memory-heavy per-character collection loop below for the same machine (see
    # FIXLOG.md 4.8). maxResultSize/arrow are notebook-specific too; none of these three
    # override the Dagster assets' defaults, which stay on SparkResource's class defaults.
    spark = SparkResource(
        app_name="win-rate-logistic-regression",
        driver_memory="16g",
        shuffle_partitions=100,
        master="local[4]",
        max_result_size="6g",
        arrow_enabled=True,
    ).get_spark()
    try:
        df = spark.read.format("delta").load(gold_path)
        print(f"Loaded {gold_path}")

        characters = sorted(
            row["character_chosen"]
            for row in df.select("character_chosen").distinct().collect()
            if row["character_chosen"] is not None
        )
        print("Characters:", characters)

        chunks = []
        for character in characters:
            chunk = (
                df.filter(F.col("character_chosen") == character)
                .select("character_chosen", "card_name", "was_picked", "victory", "floor",
                        "current_hp", "max_hp", "relic_count", "ascension_level")
                .na.drop()
                .toPandas()
            )
            print(f"{character}: collected {len(chunk)} rows")
            chunks.append(chunk)
    finally:
        spark.stop()

    regression_pd = pd.concat(chunks, ignore_index=True)
    print("Collected rows:", len(regression_pd))
    print("Distinct (character, card) pairs:",
          regression_pd[["character_chosen", "card_name"]].drop_duplicates().shape[0])

    regression_pd.to_parquet(output_parquet, index=False)
    return regression_pd


def load_win_rate_regression_input(parquet_path: str) -> pd.DataFrame:
    """Load and downcast the cached regression input, deriving hp_ratio if not already present.

    Reads via pyarrow directly with self_destruct=True rather than pd.read_parquet - the
    pandas wrapper builds a full Arrow Table and then converts it, holding both copies in
    memory at once during that conversion; self_destruct frees each column's Arrow buffer as
    soon as it's converted instead (see FIXLOG.md 4.8's ArrowMemoryError follow-on).
    """
    table = pq.read_table(parquet_path)
    regression_pd = table.to_pandas(self_destruct=True)
    del table

    regression_pd["was_picked"] = regression_pd["was_picked"].astype(int)
    regression_pd["victory"] = regression_pd["victory"].astype(int)

    if "hp_ratio" not in regression_pd.columns:
        regression_pd = regression_pd[regression_pd["max_hp"] > 0]
        regression_pd["hp_ratio"] = regression_pd["current_hp"] / regression_pd["max_hp"]
        regression_pd = regression_pd.drop(columns=["current_hp", "max_hp"])

    # Default dtypes (object strings, int64 everywhere) put this 230M-row frame at ~20GB in
    # memory - close enough to the RAM ceiling that the fit step downstream can start swapping
    # to disk instead of actually computing.
    regression_pd["character_chosen"] = regression_pd["character_chosen"].astype("category")
    regression_pd["card_name"] = regression_pd["card_name"].astype("category")
    regression_pd["was_picked"] = regression_pd["was_picked"].astype("int8")
    regression_pd["victory"] = regression_pd["victory"].astype("int8")
    regression_pd["floor"] = regression_pd["floor"].astype("int16")
    regression_pd["relic_count"] = regression_pd["relic_count"].astype("int8")
    regression_pd["ascension_level"] = regression_pd["ascension_level"].astype("int8")
    regression_pd["hp_ratio"] = regression_pd["hp_ratio"].astype("float32")
    return regression_pd


_EMPTY_LOGIT_RESULT = {
    "n": None,
    "error": None,
    "converged": None,
    "was_picked_coef": None,
    "was_picked_pvalue": None,
    "odds_ratio": None,
    "odds_ratio_ci_low": None,
    "odds_ratio_ci_high": None,
    "pseudo_r2": None,
    "auc": None,
}


def fit_card_logit(group: pd.DataFrame, formula: str, min_rows: int) -> pd.Series:
    """Fit one (character, card) group's logistic regression, with convergence/fit diagnostics.

    groupby().apply() needs every group to return a Series with the same keys - a group
    returning fewer keys than another (e.g. just n/error on failure) makes pandas fall back to
    a stacked long-format result instead of one row per group, so every branch here fills the
    full set of keys even when most are None.
    """
    result = dict(_EMPTY_LOGIT_RESULT)
    if len(group) < min_rows:
        result["n"] = len(group)
        result["error"] = "too few rows"
        return pd.Series(result)
    try:
        model = smf.logit(formula, data=group).fit(disp=0)
    except Exception as exc:
        result["n"] = len(group)
        result["error"] = str(exc)
        return pd.Series(result)

    coef = model.params["was_picked"]
    ci_low, ci_high = model.conf_int().loc["was_picked"]
    # Small, lopsided groups (a card picked in nearly every winning run and almost never in a
    # losing one) can hit quasi/complete separation: the optimizer keeps pushing was_picked's
    # coefficient toward infinity without the likelihood actually converging. statsmodels
    # doesn't raise for this - .fit() just returns whatever it had on its last iteration, with
    # a wide, unstable confidence interval. mle_retvals["converged"] is the authoritative flag.
    converged = bool(model.mle_retvals.get("converged", False))
    result.update({
        "n": len(group),
        "error": None,
        "converged": converged,
        "was_picked_coef": coef,
        "was_picked_pvalue": model.pvalues["was_picked"],
        "odds_ratio": np.exp(coef),
        "odds_ratio_ci_low": np.exp(ci_low),
        "odds_ratio_ci_high": np.exp(ci_high),
        "pseudo_r2": model.prsquared,
        # In-sample (scored on the same rows the model was fit on), so this reads as fit
        # quality, not held-out predictive performance.
        "auc": roc_auc_score(group["victory"], model.predict()),
    })
    return pd.Series(result)


def fit_all_card_logits(regression_pd: pd.DataFrame, formula: str, min_rows: int = 200) -> pd.DataFrame:
    """Fit fit_card_logit for every (character_chosen, card_name) group, adding FDR q-values.

    Thousands of independent regressions filtered at raw p < 0.05 would let ~5% of cards with
    no true effect cross significance by chance alone. Benjamini-Hochberg controls the expected
    proportion of false discoveries among the fitted, converged models instead - non-converged
    fits are excluded from the correction since they're excluded from any downstream decision
    regardless of p-value.
    """
    results_pd = (
        regression_pd.groupby(["character_chosen", "card_name"], observed=True)
        .apply(lambda g: fit_card_logit(g, formula, min_rows), include_groups=False)
        .reset_index()
    )
    results_pd["converged"] = results_pd["converged"].astype("boolean")

    testable = results_pd["error"].isna() & results_pd["converged"].fillna(False)
    results_pd["was_picked_qvalue"] = np.nan
    results_pd.loc[testable, "was_picked_qvalue"] = multipletests(
        results_pd.loc[testable, "was_picked_pvalue"], method="fdr_bh"
    )[1]
    return results_pd
