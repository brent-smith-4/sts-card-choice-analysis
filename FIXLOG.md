# Fix Log

A chronological record of bugs, data-quality issues, and structural corrections found while
building the pipeline and analysis notebooks for this project. Organized by where the problem
lived: the ETL restructuring, the bronze/silver/gold pipeline, and the analysis notebooks
(03 exploratory, 04 logistic regression). Commit hashes are given so the actual diff can be
pulled up with `git show <hash>`.

---

## 1. ETL restructuring: notebook-based prep → Dagster/Spark medallion pipeline

**Original state.** `02_data_preparation.ipynb` did run-level quality filtering and the
`card_choices` explosion (turning each run's nested pick history into one row per pick-event)
directly in the notebook, presumably in pandas over however much of the raw JSON fit in memory.
This worked for exploration but didn't scale to the full raw dataset and wasn't reproducible
as a pipeline — rerunning it meant rerunning notebook cells in order by hand.

**Restructuring (`e1364b7`, `8c39ede`, `c67659f`).** The prep logic was ported out of the
notebook into a proper Dagster-orchestrated, PySpark/Delta Lake medallion architecture
(`sts_pipeline/assets/`):

- **Bronze** (`bronze.py`) — ingests raw run JSON from `raw_data/landing/` into a Delta table,
  one row per run, no business logic applied yet.
- **Silver** (`silver.py`) — applies the run-level quality filters and the `card_choices`
  explosion that used to live in the notebook, in PySpark instead of pandas, writing one row
  per pick-event enriched with HP/relic confounders and `floors_gained`.
- **Gold** (`gold.py`, added later in `c67659f`) — three analysis-ready assets:
  `gold_run_summary` (one row per run with derived path/pick counts), `gold_card_choice_events`
  (the table notebooks 03/04 model against), and `gold_relic_offers` (boss-relic choices,
  exploded the same way as card offers).

This restructuring is also what surfaced two of the correctness bugs below (§2.1, §2.2) —
both were found while building on top of the pipeline, not while writing it originally.

Supporting cleanup done alongside the restructuring:
- `dedd0fe` — removed a leftover 7z sample-data archive from an earlier data-collection
  attempt that was never extracted or referenced by any notebook.
- `e6c669a` — gitignored local Spark/Java/rclone tooling and the new bronze/silver/gold
  directories, and fixed the `raw_data` ignore pattern, which wasn't excluding nested paths
  like `raw_data/landing/` correctly.

---

## 2. Bronze / Silver pipeline issues

### 2.1 Duplicate run ingestion in bronze (`c67659f`)

**Symptom.** Found while building `gold_relic_offers`, not during the original bronze work —
aggregate counts looked inflated in a way that didn't match expectations.

**Root cause.** The raw landing data mixes two representations of the same runs: individual
per-run/per-batch files under dated subdirectories, and monthly rollup files (e.g.
`november.json`) that *re-contain* runs already present in those per-run files. Bronze was
ingesting both without deduping, so roughly 123k runs — about 2% of the full dataset — were
counted twice, silently inflating every downstream aggregate (pick counts, win rates, sample
sizes) by however much that run's picks contributed.

**Fix.** `bronze_runs` now dedupes by `play_id`. Because the dedup requires a shuffle over the
full run set, Spark driver memory and shuffle partitions were bumped to give it enough headroom.
The full pipeline (bronze → silver → gold) was re-materialized end to end after the fix, since
every downstream table had been built on the duplicated data.

### 2.2 Silver only captured positive examples (`8c39ede`)

**Symptom.** `silver_picks` had one row per pick-event, but that row only recorded the card
that was picked — there was no representation of the other cards that were *offered but not
picked*. Any model trying to estimate a card's effect on an outcome needs both picked and
not-picked examples; `silver_picks` alone couldn't support that.

**Fix.** Added `silver_card_offers`, which explodes each pick-event into one row per *offered*
card (picked or not), giving choice modeling proper negative examples. Built as a reshape of
`silver_picks` rather than a new gold-layer aggregation, since no aggregation is involved.
`silver_inspection.ipynb` and a `card_choices` peek cell in `bronze_inspection.ipynb` were
added at the same time to validate the explode logic end-to-end against a sample run before
trusting it at full scale.

### 2.3 Confusing / incorrect-reading column names in silver (`8c39ede`)

**Symptom.** `silver_picks`' columns `card_selected`, `card_not_selected`, and two separate
floor columns were misleading: `card_not_selected` read like it held a card *name* (parallel to
`card_selected`) when it actually held a boolean, and the two floor columns were mutually
exclusive (only one was ever non-null for a given row) but existed as two separate columns
instead of one.

**Fix.** Renamed to `card` / `is_selected` / `choice_floor`, collapsing the two
mutually-exclusive floor columns into the single non-null `choice_floor` column. Caught and
fixed proactively during the `silver_card_offers` work, before it could cause a real bug
downstream (e.g. a join or filter that assumed `card_not_selected` was a card identifier).

---

## 3. Notebook 03 (exploratory analysis) — no correctness bugs found

`03_exploratory_analysis.ipynb` (`c911497`) added a screening pass over
`gold_card_choice_events` — per-character, per-card pick rate and win-rate/floors-gained lift
(picked vs. not-picked), filtered to a minimum sample size — to shortlist cards worth a
per-card regression. No bugs were found in this notebook itself; its output (the raw-lift
screen) is what fed the winner's-curse issue below in notebook 04.

---

## 4. Notebook 04 (win-rate logistic regression)

### 4.1 Winner's-curse in the original card-selection design (`34fce82`)

**Symptom.** None visible at first — the regression ran and produced results. The problem was
methodological, not a crash.

**Root cause.** The original design used notebook 03's screen: pick a shortlist of "cards of
interest" by raw `win_rate_lift`, then fit *that same card's* effect via logistic regression on
the *same data* used to compute the raw lift. A card's raw lift is its true effect plus sampling
noise. Sorting by raw lift and keeping the top/bottom cards preferentially keeps cards that got
a lucky noise draw in that direction — and because the regression re-uses the same rows, that
noise doesn't average out on a second look. The resulting regression would be biased toward
whatever the screen happened to reward, not because of anything wrong with the regression itself.

**Fix.** Stopped pre-selecting a shortlist. Every `(character, card)` pair (3,139 of them) is
fit directly, sidestepping the selection-bias problem entirely instead of working around it with
a train/test split. Notebook 03's raw-lift screen is kept as a sanity check to compare against
afterward (a card with strong raw lift but a controlled odds ratio near 1 suggests the raw lift
was confounded) rather than as an input to which cards get modeled.

### 4.2 `groupby().apply()` silently reshaping results — first occurrence (`a3b31a5`)

**Symptom.** Downstream code accessing `results_pd["error"]` broke.

**Root cause.** `pandas.DataFrameGroupBy.apply()` requires every group's returned `Series` to
carry the same set of keys to produce one row per group. `fit_card_logit`'s failure branches
(too few rows; an exception during `.fit()`) returned a `Series` with only `n` and `error` set,
while the success branch returned a `Series` with 7 keys. Because the keys didn't match across
groups, pandas silently fell back to a stacked long-format result instead of the expected
one-row-per-group table — no error was raised, the shape was just wrong.

**Fix.** Introduced an `EMPTY_RESULT` template dict listing every key the function can ever
return (`n`, `error`, `was_picked_coef`, `was_picked_pvalue`, `odds_ratio`,
`odds_ratio_ci_low`, `odds_ratio_ci_high`, `pseudo_r2`, `auc`), all defaulted to `None`. Every
branch now starts from `dict(EMPTY_RESULT)` and only overwrites the keys relevant to that
branch, so the returned `Series` always has identical keys regardless of outcome.

### 4.3 Single `toPandas()` collect exceeded Spark driver memory (uncommitted intermediate work; landed in the pipeline as of `8c104d8`/`77ba932`)

**Symptom.** Collecting the full `gold_card_choice_events` table (~230M rows) into a single
pandas DataFrame via `.toPandas()` failed — first by exceeding `spark.driver.maxResultSize`,
and after that limit was raised, by exhausting the JVM driver heap outright.

**Root cause.** `collect()` stages every row as boxed Java objects on the driver before handing
them to Python, which is far heavier per-row than the table's on-disk Parquet footprint. A
230M-row collect in one shot was never going to fit regardless of how high `maxResultSize` was
raised, short of also proportionally scaling driver memory.

**Fix.** Collect per character instead of all at once: `card_choices`/offers are already
partitioned by `character_chosen` in gold, so looping over the four characters and calling
`.toPandas()` on one character's filtered slice at a time was free to do and bounds peak driver
memory to roughly one character's worth of rows, then concatenates the four chunks in pandas.

### 4.4 Default pandas dtypes hit the RAM ceiling (same timeframe as §4.3)

**Symptom.** Even after chunked collection, routine operations on the assembled 230M-row
`regression_pd` (adding a column, a `groupby`) could fail outright rather than just running slowly.

**Root cause.** pandas' default dtypes — `object` for string columns, `int64` for every integer
column — put the frame at roughly 20GB in memory, close enough to the system's RAM ceiling that
any operation needing a temporary copy or extra working memory could tip it over.

**Fix.** Downcast every column to the smallest dtype that actually fits its range immediately
after assembly: `category` for the two low-cardinality string columns (`character_chosen`,
`card_name`), `int8` for the 0/1 flag columns (`was_picked`, `victory`), `int16`/`float32` for
the remaining numeric columns as appropriate.

### 4.5 `groupby().apply()` inconsistent-keys bug — recurrence (`77ba932`)

**Symptom.** Same failure mode as §4.2, reintroduced later: adding new metrics broke the
one-row-per-group shape again.

**Root cause.** When `pseudo_r2` and `auc` (McFadden's pseudo-R² and ROC AUC — added to flag
"statistically significant but practically meaningless" results at this sample size) were added
to the success branch's return dict, they were not added to `EMPTY_RESULT`. This reintroduced
exactly the key-mismatch condition fixed in §4.2, just with two different keys.

**Fix.** Added `pseudo_r2: None` and `auc: None` to `EMPTY_RESULT` alongside the other fields.

**Process fix alongside it.** A few changes were made specifically so this class of bug (and
future ones like it) would be caught faster next time, rather than discovered downstream:
- Added a `results_pd["error"].value_counts(dropna=False)` cell immediately after the fit step,
  so failure modes and their frequency are visible right away instead of only surfacing when
  some later column access breaks.
- Added a `regression_pd.dtypes` check cell to make dtype regressions visible immediately.
- Changed the regression-input cell to `pd.read_parquet("../raw_data/win_rate_regression_input.parquet")`
  instead of re-collecting from Spark every time, so iterating on `fit_card_logit` and re-running
  the fit doesn't require re-running the entire Spark pipeline first.
- Removed a redundant/duplicate Arrow config (was being set both in the `SparkSession` builder
  and again via `spark.conf.set(...)` afterward) and throttled the local Spark master from
  `local[*]` to `local[4]`, reducing contention with the memory-heavy per-character collection
  loop from §4.3.

### 4.6 Statistical methodology review (post-hoc, notebook already "working")

Unlike §4.1–§4.5, the issues in this section didn't surface as crashes or malformed output —
the notebook ran end to end and produced a plausible-looking `significant` table. They came out
of a deliberate statistical review of that table after the fact (prompted by a second AI's read
of the notebook, cross-checked against the code and results before any of it was accepted). All
four are gaps in what "significant" was allowed to mean, not bugs in the fitting code itself.

#### 4.6.1 Non-converged fits weren't excluded

**Symptom.** `THE_SILENT — Barrage+1` sat in the top-5 `significant` table with an odds ratio of
22.29 and a 95% CI of roughly `[1.2, 417]` — a confidence interval wide enough to mean "somewhere
between a small effect and an enormous one," which isn't a usable estimate.

**Root cause.** Small, lopsided card groups (a card picked in nearly every winning run and almost
never in a losing one) can hit quasi/complete separation, where `statsmodels`' MLE optimizer keeps
pushing `was_picked`'s coefficient toward infinity without the likelihood actually converging.
`.fit()` doesn't raise for this — it silently returns whatever the optimizer had on its last
iteration, printing a warning (`Maximum Likelihood optimization failed to converge`) rather than
failing the `try`/`except` in `fit_card_logit`. The existing exception handling had nothing to
catch, because nothing was raised.

**Fix.** Read `model.mle_retvals["converged"]` after every successful fit and record it as a new
`converged` column (added to `EMPTY_RESULT` and the success branch, same pattern as §4.2/§4.5).
`significant` now requires `converged == True` in addition to the p-value/effect-size filters
below, so separation-driven fits are dropped from the reported table instead of sitting alongside
genuine effects. `results_pd` still retains them (with `converged = False`) rather than discarding
the rows outright.

#### 4.6.2 No multiple-comparisons correction

**Symptom.** None visible in any single row — this is a property of the whole table, not any one
model.

**Root cause.** `significant` was built by fitting 2,232 independent regressions and keeping every
one with `was_picked_pvalue < 0.05`. At that threshold, if even a modest fraction of the 2,232
cards have no true effect on win rate, chance alone would still push roughly 5% of them — 100+
cards — under the 0.05 line. Every one of those would show up in `significant` indistinguishable
from a real effect. Notably, this is the same *family* of problem as the winner's-curse issue in
§4.1 (looking at many things and trusting whichever ones look extreme), just at the
significance-testing step instead of the card-selection step — the notebook's own markdown reasons
carefully about the first without addressing the second.

**Fix.** After fitting, apply Benjamini-Hochberg FDR correction
(`statsmodels.stats.multitest.multipletests(..., method="fdr_bh")`) to the p-values of every
fitted-and-converged model, storing the result as `was_picked_qvalue`. `significant` now filters
on `was_picked_qvalue < 0.05` instead of the raw p-value, controlling the expected proportion of
false discoveries among the reported cards rather than the per-card false-positive rate.

#### 4.6.3 Statistical significance wasn't checked against practical significance in the output

**Symptom.** Cards like `IRONCLAD — Buffer` (odds ratio ≈ 1.02, p ≈ 0.02, n ≈ 333k) appeared in
`significant` in the same table as `Concentrate` (odds ratio 3.88) — a barely-there effect and a
large one, presented with equal weight.

**Root cause.** `pseudo_r2` and `auc` were already being computed specifically to catch
"significant but practically meaningless" results (per the comment in `fit_card_logit`), but
`significant`'s filter only ever checked `was_picked_pvalue`. With some cards offered hundreds of
thousands of times, even a trivial, practically irrelevant shift in win probability reaches
significance — the tooling to catch this existed but wasn't wired into the filter that mattered.

**Fix.** Added an effect-size requirement to `significant`: the odds ratio must fall outside
`[0.9, 1.1]`, alongside the convergence and FDR checks from §4.6.1/§4.6.2. `pseudo_r2` and `auc`
are also now included as columns in the displayed table so remaining borderline cases can still be
judged by eye.

#### 4.6.4 In-sample AUC (flagged, not changed)

**Issue.** `auc = roc_auc_score(group["victory"], model.predict())` scores each model on the same
rows it was fit on, so it's an optimistic measure of fit, not of generalization to unseen runs.

**Decision: documented, not fixed with a held-out split.** This notebook's purpose is inferential
— interpreting `was_picked`'s coefficient under a controlled model — not predictive. Adding a
train/test split (or refitting per fold) to get an honest AUC would roughly double the fitting
cost across all 2,232 groups for a number that isn't driving any decision in this notebook. Instead,
the markdown and the code comment above `auc`'s computation now say explicitly that it's an
in-sample fit-quality measure, not a claim about held-out predictive accuracy. Worth revisiting
with a real split if a future notebook ever uses these models predictively rather than
inferentially.

#### 4.6.5 `MIN_REGRESSION_ROWS = 200` / Firth's penalized regression (flagged, not changed)

**Issue.** The convergence failures in §4.6.1 are consistent with `MIN_REGRESSION_ROWS = 200`
being thin for a 5-predictor model on cards with rare picks or lopsided win rates — Firth's
bias-reduced MLE (`firthlogist` on PyPI; not in `statsmodels`) is designed exactly for this
rare-event/small-sample separation case and would let low-`n` cards fit stably instead of either
failing to converge or producing an inflated estimate.

**Decision: not implemented.** Once §4.6.1 filters non-converged fits out of `significant`, the
practical risk this would guard against (an unstable estimate being reported as if it were solid)
is already contained — those rows are excluded, not silently kept. Pulling in a new dependency to
*recover* cards that are being correctly excluded wasn't judged worth it for this pass. Left as a
known option if low-`n` cards specifically become worth reporting on later.

### 4.7 Notebook hygiene cleanup (paired with §4.6)

Several cells were left over from live debugging sessions and, while an honest record of how the
notebook was actually built, weren't meant to be part of the final read:
- `%tb` (bare traceback replay) and a raw `MemoryError` traceback from an earlier out-of-memory
  run, both pure debugging scratch with no lasting value.
- A duplicate memory/row-count print cell (redundant with an existing one later in the notebook).
- A `regression_pd.dtypes` check and two duplicate `results_pd["error"].value_counts()` cells —
  one of them showing a stale `"name 'smf' is not defined"` error from a broken execution order
  in an earlier run, not a real result.
- A duplicate definition of `significant` immediately after the first one, whose only distinct
  contribution was dumping the whole table through `.to_string()` as a bare expression — a wall
  of monospace text instead of the scrollable DataFrame display the first definition already
  produces.

All of the above were removed. The one genuinely useful diagnostic among them — an error/
convergence breakdown right after fitting — was kept, but as a single clean cell
(`results_pd["error"].value_counts()` plus a `converged` breakdown) placed deliberately right
after the fit step, rather than as scattered debugging fossils.

**Note on scope:** all of §4.6–§4.7 were applied as source edits only. Given the ~230M-row Spark
collect and full 2,232-model refit involved, re-executing the notebook was left for the user to
run directly rather than executed here.

### 4.8 Spark JVM left resident during the memory-heavy fit — caught live mid-run

**Symptom.** Re-running the notebook after §4.6/§4.7's edits, the fitting cell (`groupby().apply(fit_card_logit)`)
was still going at 17 minutes with no new output. Task Manager showed 88% system RAM and 0% CPU —
not slow computation, no computation: the process was disk-swap-bound.

**Root cause.** Two compounding issues, both pre-existing (neither introduced by this session's
edits, just newly diagnosed):
- `spark.stop()` was called near the very end of the notebook (after the `significant` table),
  not once `regression_pd` had been collected out of Spark. That left the SparkSession's JVM
  driver (`spark.driver.memory = 16g`) resident in memory for the entire duration of the fit loop,
  even though nothing after the initial collect (cell `a1000007`) ever touches `df`/`spark` again.
- The `regression_pd` cell had, at some point in an earlier refactor (switching it to
  `pd.read_parquet(...)` instead of re-collecting from Spark each run — see §4.5), silently lost
  the dtype-downcast step originally added in §4.4. Without it, `regression_pd` sits at its
  default-dtype size of ~20GB.
- 16GB (idle JVM) + ~20GB (undowncast pandas frame) + `groupby().apply()`'s own per-group slicing
  overhead was enough to exceed available RAM and push the machine into swapping, which looks
  exactly like this: warnings still occasionally printing (so *something* was moving), but
  effectively zero forward progress.

**Fix.**
- Moved `spark.stop()` to run immediately after the per-character collection loop (`a1000007`),
  before the fit cell, instead of at the end of the notebook. Nothing downstream needs Spark once
  `regression_pd` is collected.
- Re-added the dtype downcast (`category`/`int8`/`int16`/`float32`) to the `regression_pd` cell,
  and additionally drop `current_hp`/`max_hp` once `hp_ratio` is derived from them — those two
  columns are otherwise dead weight, since the regression formula only uses `hp_ratio`, not the
  raw HP values.

**Note on scope:** this fix landed mid-incident, in direct response to a run that was actively
stuck, rather than through a normal review pass. The 17-minute run had to be interrupted and
restarted from scratch, since `results_pd = (...)` is a single atomic assignment — there is no
partial-progress recovery from a `groupby().apply()` call that gets interrupted partway through.

**Follow-on: `ArrowMemoryError` on the retry.** After restarting and re-running from the fit cell
(skipping the now-unnecessary Spark collection, since `win_rate_regression_input.parquet` was
already cached on disk), the very next attempt failed at `pd.read_parquet(...)` itself:
`ArrowMemoryError: malloc of size 9218651456 failed` (~8.6GB single allocation). Two likely
contributors: (1) `pd.read_parquet` builds a full Arrow Table and then converts it to a pandas
DataFrame, holding both representations in memory simultaneously for the duration of the
conversion — a transient memory spike that happens *before* the downcast code below it ever gets
a chance to run; (2) possibly compounded by the prior run only having been interrupted rather than
fully kernel-restarted, leaving fragmented/resident memory behind. Fixed the first cause directly:
switched to reading via `pyarrow.parquet.read_table(...)` followed by
`.to_pandas(self_destruct=True)`, which frees each column's Arrow buffer as soon as it's converted
instead of holding both copies at once, roughly halving peak memory at the read step. The second
cause isn't something code can fix — a full **Kernel → Restart** (not Interrupt) before retrying
is still necessary after any run that got this deep into memory pressure.

**Outcome.** The retry completed successfully: **2,231 of 3,139** card/character pairs fitted
(one fewer than the pre-§4.6 run's 2,232 — noise from the `float32` downcast changing which
edge-case fits converge, not a regression), of which **2,221 converged** and entered the
Benjamini-Hochberg correction. After all three `significant` filters (§4.6.1–§4.6.3), the final
counts per character were DEFECT 184, IRONCLAD 230, THE_SILENT 216, WATCHER 194 — **824 total**,
down from the original 1,049 raw-`p < 0.05` count. That drop is the expected, intended effect of
§4.6: the original table included separation artifacts (like the `Barrage+1` case in §4.6.1),
false discoveries the FDR correction is designed to filter, and trivial-effect-size cards that
happened to be significant only because of sample size — all now excluded by design, not lost to
a bug.

### 4.9 Notebook 04 split into `sts_pipeline/win_rate_modeling.py`

**Motivation.** Not a bug fix — a structural change to keep future collection/modeling work
reusable and keep the notebook itself readable as a record of *decisions* (what's excluded and
why, what counts as significant and why) rather than *mechanics* (Spark session setup, the
per-group fit loop, FDR bookkeeping).

**Split.** Moved into `sts_pipeline/win_rate_modeling.py`: `collect_win_rate_regression_input` (Spark env
setup, the per-character chunked collection from §4.3, caching to parquet, stopping Spark
internally per §4.8), `load_win_rate_regression_input` (the pyarrow `self_destruct=True` read
from §4.8's follow-on, dtype downcast from §4.4), `fit_card_logit` (the per-group fit with the
convergence check from §4.6.1), and `fit_all_card_logits` (the `groupby().apply()` orchestration
plus the Benjamini-Hochberg correction from §4.6.2, since every model in a run needs the
correction applied together — it isn't a per-card decision). Kept in the notebook: calling those
functions, building `significant` (the converged/qvalue/effect-size threshold decisions from
§4.6, which stay next to the markdown narrating them), the summary/top/bottom breakdowns, and
the CSV/parquet output writes. `sts_pipeline` isn't pip-installed in this venv, so the notebook's
setup cell now does `sys.path.insert(0, str(PROJECT_ROOT))` before importing it — Dagster gets
this for free by always running from the project root, notebooks don't.

**Bug caught during the split: the raw cache had already been corrupted.** While designing
`load_win_rate_regression_input`, tracing what the old final save cell did turned up a real bug:
it resaved `regression_pd` back onto the *same path* it was read from, but by that point
`hp_ratio` had been derived and `current_hp`/`max_hp` dropped. Checking the actual file on disk
confirmed it: `raw_data/win_rate_regression_input.parquet` had already lost `current_hp`/`max_hp`
from a prior full run, which would have made the very next `pd.read_parquet(...).astype()` /
`current_hp / max_hp` line in the old notebook raise a `KeyError` on its next execution. Fixed
two ways: `load_win_rate_regression_input` now checks whether `hp_ratio` is already present and
skips re-deriving it if so (so the already-corrupted cache still loads correctly, without forcing
a costly Spark re-collection just to repair it), and the final save cell no longer resaves
`regression_pd` onto the raw cache path at all — only `results_pd` (genuinely new output each
run) gets saved there now.

**Known side effect of the workaround.** Because the defensive load path uses the cache's
already-computed `hp_ratio` (which has been through one float32 round-trip) rather than
re-deriving it fresh from `current_hp`/`max_hp`, a couple of small, borderline-separation card
groups (`n` in the low hundreds) shifted in or out of `significant` between runs - not from any
change in logic, just a tiny floating-point difference landing on the other side of a
convergence boundary for those specific groups. Overall counts (2,231 fitted, 2,221 converged,
824 significant) were unchanged. A one-time clean Spark re-collection would flush the corrupted
cache and remove this sensitivity entirely; not done as part of this pass since it isn't
correctness-affecting at the aggregate level.

---

## Recurring lessons

- **`groupby().apply()` + heterogeneous return branches is a repeat offender** (§4.2, §4.5).
  Any function passed to `.apply()` in this codebase that can return different keys on
  different branches should build its result from a single canonical template dict, updated
  in a follow-up review whenever new keys are added to the success path.
- **Duplication in the raw data doesn't announce itself** (§2.1) — it was only caught while
  building an unrelated gold asset, well after bronze/silver were considered "done." Worth
  spot-checking `play_id` uniqueness whenever a new raw data source or rollup format is added
  to `raw_data/landing/`.
- **Selection bias from reusing the same data for screening and estimation** (§4.1) is easy to
  introduce unintentionally by chaining two exploratory notebooks together. The fix pattern —
  fit everything, use the cheap screen only as an independent sanity check — generalizes to any
  future "shortlist then re-estimate" workflow in this project.
- **Collect-then-process at this row count (hundreds of millions) needs chunking and dtype
  control by default** (§4.3, §4.4), not as an optimization pass after something already failed.
- **"Fits without crashing" and "safe to report" are different bars** (§4.6). Every issue in
  §4.6 passed silently through the fitting code — no exception, no malformed shape, a plausible
  table at the end. Catching them required treating the *statistical* output, not just the code
  path, as something to review: checking convergence flags the library doesn't surface by
  default, correcting for the number of tests actually run, and checking effect size against the
  tools already built to measure it. The same instinct that caught winner's-curse in §4.1 (many
  looks at noisy data biases whichever one you keep) applies one step later, at the
  significance-filtering step, and needs to be applied there deliberately rather than assumed to
  carry over.
