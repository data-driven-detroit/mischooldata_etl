# Running this project from Dagster

This package is a library that builds tables, not an orchestrator. Dagster wraps it:
each table becomes an asset, Dagster decides when to run them and keeps the
history. Nothing in here imports Dagster, and it shouldn't — keeping the ETL
orchestrator-agnostic means you can still run any module from the command line
(see `CLAUDE.md`).

## The interface Dagster uses

`mischooldata_etl/datasets.py` exposes every **table** this project produces as
an asset:

```python
from mischooldata_etl.datasets import ASSETS

ASSETS["attendance"]                  # Asset(name="attendance", group="attendance", deps=())
ASSETS["school_geocodes"].deps        # ("eem",)
ASSETS["school_geocodes"].materialize()   # rebuild that one table
```

An asset is a thing that exists -- `education.attendance`,
`education.school_geocodes` -- not a step. Transforming and loading are
behaviors *inside* `materialize`; the intermediate CSV under `output/` is a
scratch file, not something Dagster tracks. The one real cross-asset dependency
is `school_geocodes`, which is built from the eem data.

Every `materialize` is a zero-argument callable. Walk this registry rather than
importing each module by hand, so a new table shows up in Dagster as soon as
it's added to the registry. Entries are declared in dependency order.

Importing `mischooldata_etl.datasets` does **not** require `config.toml` to
exist and does not open a database connection. Config and engines are read
lazily, inside `materialize`. That means a Dagster code location loads cleanly
in a container that hasn't been given secrets yet.

## A code location

```python
# defs.py
from dagster import AssetExecutionContext, AssetKey, Definitions, asset

from mischooldata_etl.datasets import ASSETS, SCHEMA


def _build_asset(a):
    @asset(
        key=AssetKey([SCHEMA, a.name]),
        group_name=a.group,
        deps=[AssetKey([SCHEMA, d]) for d in a.deps],
        description=a.description or None,
    )
    def _asset(context: AssetExecutionContext) -> None:
        context.log.info("materializing %s.%s", SCHEMA, a.name)
        a.materialize()

    return _asset


defs = Definitions(assets=[_build_asset(a) for a in ASSETS.values()])
```

```bash
dagster dev -f defs.py
```

That yields 11 assets, one per table, keyed `education/<table>`. Ten are
independent; `education/school_geocodes` depends on `education/eem`.

Keys match the tables, so if you later add downstream assets (dbt models, SQL
views) that read these tables, the lineage lines up without any mapping.

Assets write to the database themselves rather than returning DataFrames, so no
I/O manager is involved and `deps=` rather than Dagster inputs express the
dependency. `school_geocodes` still reads the eem scratch CSV off disk, so it
has to run somewhere that shares a filesystem with the eem materialization.

## Project layout

Keep the Dagster code in its own project that depends on the ETL packages,
rather than adding `dagster` to `pyproject.toml` here. Each source repo stays
runnable and testable without Dagster installed, and the orchestration layer
redeploys on its own schedule.

With several source-specific ETL projects, a sibling layout works well:

```
etl/
    etl_core/                  # the shared contract (see below)
        pyproject.toml
        etl_core/
            __init__.py
            datasets.py        # Asset
            config.py          # get_config()
            db.py              # get_db_engine()
            logging_setup.py   # setup_logging()

    mischooldata_etl/          # this repo -- one source
        pyproject.toml
        mischooldata_etl/
            datasets.py        # ASSETS, built from etl_core's Asset
            pipeline.py        # shared transform/load helpers for THIS source
            attendance/
            eem/
            ...

    census_etl/                # another source, same shape
    permits_etl/               #   "

    pipeline/                  # the Dagster project
        pyproject.toml         # depends on each *_etl package
        dagster.yaml
        pipeline/
            __init__.py
            definitions.py     # Definitions built from every registered source
            assets.py          # the registry -> asset builder
            resources.py
            schedules.py
```

### What belongs in `etl_core`

The `Asset` dataclass currently live in
`mischooldata_etl/datasets.py`. With more than one source they should move to a
shared package, so the pipeline project can treat every source uniformly instead
of relying on each one having independently-defined lookalike classes.

`get_config()`, `get_db_engine()` and `setup_logging()` are worth moving too --
they're identical boilerplate every source would otherwise copy, and
`get_db_engine()` in particular encodes the `URL.create` rule that keeps
passwords with `@` in them working.

Each source package then keeps only what's actually source-specific: its
datasets, its field references, and its own shared transform helpers.

### How the pipeline finds the sources

Explicit imports are fine and obvious:

```python
# pipeline/definitions.py
from mischooldata_etl.datasets import ASSETS as MISCHOOLDATA
from census_etl.datasets import ASSETS as CENSUS

SOURCES = {"mischooldata": MISCHOOLDATA, "census": CENSUS}
```

If you'd rather add a source without editing the pipeline, declare an entry
point in each source's `pyproject.toml`:

```toml
[project.entry-points."etl.sources"]
mischooldata = "mischooldata_etl.datasets:ASSETS"
```

and discover them:

```python
from importlib.metadata import entry_points

def load_sources():
    return {ep.name: ep.load() for ep in entry_points(group="etl.sources")}
```

Installing a new `*_etl` package is then enough for its assets to appear. Prefix
the asset keys with the source name so two sources can both have, say, an
`enrollment` table without colliding:

```python
@asset(key=AssetKey([source_name, a.name]), ...)
```

Start with explicit imports; move to entry points when editing the pipeline for
every new source starts to chafe.

### Config across several sources

Each source currently reads its own `config.toml` from inside its package. That
doesn't scale to several -- you'd be maintaining the same database block in
four places. Once `get_config()` lives in `etl_core`, point it at a single file
via an environment variable (`ETL_CONFIG`) with one section per source:

```toml
[db]                      # shared by every source
user = ""
password = ""
host = ""
name = "data"
port = 5432

[mischooldata]
app_name = "education"
vault_location = ""

[census]
app_name = "census"
vault_location = ""
```

The pipeline project then supplies one config to every source it orchestrates,
and a single mounted secret covers the whole deployment.

## Config and secrets

Today config comes from `mischooldata_etl/config.toml`, read lazily by
`mischooldata_etl.config.get_config()`. For a single source, two ways to supply
it:

- **Mount the file.** Simplest — put `config.toml` in the image or mount it as a
  secret at `mischooldata_etl/config.toml`.
- **Make `get_config()` fall back to the environment.** If you'd rather use
  Dagster's `EnvVar` and your platform's secret store, that's a small change to
  one function, and it's the only place that would need to change.

Once there's more than one source, prefer the single shared config file
described under [Project layout](#config-across-several-sources) — one mounted
secret for the whole deployment rather than one per source package.

Either way, the vault itself must be mounted wherever assets materialize — they read
source files straight off it via `config["vault_location"]`.

## Logging

`setup_logging()` returns early if the root logger already has handlers, so it
won't replace Dagster's logging if something calls it. The assets above use
`context.log` for their own messages.

The modules' own `logging.getLogger(__name__)` calls don't automatically appear
in the Dagster UI. To capture them, set this in `dagster.yaml`:

```yaml
python_logs:
  managed_python_loggers:
    - mischooldata_etl
  python_log_level: INFO
```

## Things that will bite you

These are properties of the ETL as it stands, not of Dagster.

**Materializations self-skip.** Several transforms return early if their output file
already exists (`"Files already compiled..."`), and `generic_transform` queries
the destination table and skips date ranges already present. A Dagster re-run
can therefore succeed while doing nothing. If you want Dagster's materialization
record to mean "this data was rebuilt", these guards need to come out and the
skipping logic needs to become partition selection.

**`TODAY` is captured at import.** `non_resident` and `student_counts` compute
`TODAY = datetime.date.today()` at module level and put it in their filenames. A
long-lived Dagster code server imports once, so a run that happens after
midnight will look for a file stamped with an earlier date. Move those into the
functions before scheduling these two.

**One load path isn't idempotent.** Most loads are safe to retry: the
module-specific ones (`eem`, `early_childhood`, `college_readiness`,
`grad_dropout`) replace on the first chunk and append the rest within a run, and
`non_resident` / `student_counts` do a straight `if_exists="replace"`. But
`generic_load` in `pipeline.py` *appends* whenever the table already exists, so
retrying it after a partial failure duplicates rows. That path is used by
`attendance`, `college_destination`, `college_enrollment` and `student_mobility`
-- check those four before enabling automatic retries.

**Output lands inside the package.** Steps write to
`mischooldata_etl/<dataset>/output/`, resolved from `__file__`. In a container
with the package installed in site-packages that's ephemeral and possibly
read-only. Deploying properly means making the output root configurable.

## Partitioning, if you go further

The natural partition is the school year: every module's
`conf/dataset_years.csv` already lists one row per year with `start_date` /
`end_date`. Mapping that onto a Dagster partition set would give you per-year
backfills, retries, and a real materialization history, and would replace the
ad-hoc "have I already loaded this year?" checks.

It needs an interface change first: `materialize` (and the `transform_*` / `load_*` it
calls) takes no arguments today and always process every row of `dataset_years.csv`. They'd need to accept
a year (or date range) and handle only that one. That's the main piece of work
between "Dagster can run this" and "Dagster is managing this properly".

---

*The code above was checked against Dagster 1.13.24: the code location loads, builds
the per-step asset graph an earlier version of this doc described. The
table-per-asset version above has not been re-run against Dagster. Adjust for your own Dagster version.*
