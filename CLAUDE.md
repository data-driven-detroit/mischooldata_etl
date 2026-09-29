# mischooldata_etl

ETL for Michigan school data (MI School Data) into the `education` schema of EDW.

## Setup

```bash
uv sync
```

One prerequisite:

- **`mischooldata_etl/config.toml` must exist.** Copy it from
  `mischooldata_etl/config.toml.example` and fill in the blanks. It is gitignored.
  It holds `vault_location` (top-level) plus the `[app]` and `[db]` tables.

## Running a module

Each dataset is a subpackage with a `process.py` entry point. Run it with `-m`
from the project root:

```bash
uv run python -m mischooldata_etl.eem.process
```

`process.py` materializes every table the module owns. The transform and load
halves still run on their own, for debugging one half:

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
    datasets.py         # ASSETS registry: every table and how to build it
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
`mischooldata_etl/template/`. Add their tables to `ASSETS` in `datasets.py` too --
`process.py` and any orchestrator both drive off that registry. An asset is a
table, not a step: its `materialize` runs transform then load. Only add `deps`
for a real table-to-table dependency (e.g. `school_geocodes` on `eem`).

Config and database engines are read lazily, inside `materialize`, so importing the
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

**Schemas.** Validation schemas use pandera's pandas namespace --
`import pandera.pandas as pa` and `from pandera.typing.pandas import Series`, not
the top-level `import pandera as pa`, which is deprecated and warns.

**Full rebuilds.** Every materialization rebuilds its table from the vault. No
"already exists, skipping" guards -- not on output files, not by querying the
table. Transforms overwrite their scratch file; loads replace the table inside a
single `with get_db_engine().begin() as db:` so a failed load rolls back and
leaves the old table intact. Never `connect()` for a load: pandas commits each
chunk on its own there.

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
- `unwrap_value` / `unwrap_error` in `pipeline.py` are reconstructed, not
  verified against `inequalitytools`.
