# mischooldata_etl

ETL for Michigan school data (MI School Data) into the `education` schema of EDW.

## Setup

```bash
uv sync
```

Two prerequisites:

- **`../elote` must be a sibling checkout.** `pyproject.toml` depends on it as an
  editable local path; without it `uv sync` fails with
  `Distribution not found at: .../elote`.
- **`mischooldata_etl/config.toml` must exist.** Copy it from
  `mischooldata_etl/config.toml.example` and fill in the blanks. It is gitignored.
  It holds `vault_location` (top-level) plus the `[app]` and `[db]` tables.

## Running a module

Each dataset is a subpackage with a `process.py` entry point. Run it with `-m`
from the project root:

```bash
uv run python -m mischooldata_etl.eem.process
```

The transform and load halves run the same way, for redoing one step:

```bash
uv run python -m mischooldata_etl.grad_dropout.transform
uv run python -m mischooldata_etl.grad_dropout.load
```

**Always `-m`, never by file path.** Modules import their siblings relatively
(`from ..pipeline import generic_transform`), and a file executed by path has no
package identity, so those imports raise
`ImportError: attempted relative import with no known parent package`.

Modules: `attendance`, `college_destination`, `college_enrollment`,
`college_readiness`, `early_childhood`, `eem`, `grad_dropout`, `non_resident`,
`student_counts`, `student_mobility`.

## Layout

```
mischooldata_etl/
    __init__.py         # deliberately empty
    config.py           # get_config()
    db.py               # get_db_engine()
    logging_setup.py    # setup_logging()
    pipeline.py         # generic_transform / generic_load + shared helpers
    datasets.py         # DATASETS registry: every dataset and its ordered steps
    <dataset>/
        __init__.py
        conf/           # dataset_years.csv, field_reference_*.json
        transform.py    # reads the vault, writes output/
        load.py         # writes to the database
        process.py      # entry point: main() behind a __main__ guard
```

Shared code lives in peer submodules, never in `__init__.py`. Nothing imports
"upward" into the package root.

New modules are scaffolded from the cookiecutter template in
`mischooldata_etl/template/`. Add them to `DATASETS` in `datasets.py` too --
`process.py` and any orchestrator both drive off that registry.

Config and database engines are read lazily, inside the steps, so importing the
package never requires `config.toml` or touches the database. Keep it that way;
see `DAGSTER.md`.

## Conventions

**Source paths.** The `source_file` column in each `conf/dataset_years.csv` is
relative to the vault, with forward slashes. The vault root never appears in the
CSV; transforms join it at read time with
`Path(config["vault_location"]) / year["source_file"]`, which `Path` normalizes
per platform.

**Database.** Build engines with `get_db_engine()`. Connection URLs go through
`sqlalchemy.engine.URL.create()` — never f-string a DSN, or passwords containing
`@` or `:` break the connection.

**Logging.** Each module declares its own logger at the top:

```python
import logging

logger = logging.getLogger(__name__)
```

Never pass a logger as an argument. `process.py` calls `setup_logging()` once
before doing any work; it reads `logging_config.json` and writes to stdout plus a
rotating `mischooldata_etl.log` at the project root.

## Known gaps

- `cohort` is unfinished: `load_cohort.py` is empty, `create_cohort.py` builds a
  DataFrame and discards it. `assessments` has conf but no code yet.
- Eight modules still use `print()` rather than the logger.
- `geopandas`, `pandera`, and `numpy` are imported but not declared in
  `pyproject.toml`; they currently arrive transitively.
- `unwrap_value` / `unwrap_error` in `pipeline.py` are reconstructed, not
  verified against `inequalitytools`.
