# Updating education data

## Creating a `config.toml`

The database connection string and the vault path are read from a `config.toml`
that lives in the package directory, `mischooldata_etl/`, next to `common.py`.
This file is gitignored -- don't commit it.

Copy the checked-in example and fill in the blanks, pointing at EDW:

```shell
cp mischooldata_etl/config.toml.example mischooldata_etl/config.toml
```

It should end up with the following structure:

```toml
# Path to the vault server, e.g. "/home/you/vault" or "V:\\".
# Keep this above the table headers -- in TOML, a key written after a
# header belongs to that table.
vault_location = ""

[app]
name = "education"

[db]
user = ""
password = ""
host = ""
name = "data"
port = 5432
```

## Install with uv

This project uses [uv](https://docs.astral.sh/uv/). Install it first if you
don't have it -- see the [install docs](https://docs.astral.sh/uv/getting-started/installation/).

### Prerequisite: the `elote` sibling checkout

`pyproject.toml` depends on `elote` as an editable local path, `../elote`, so it
must be cloned as a sibling of this repo before you sync:

```
0_projects/
    elote/
    mischooldata_etl/   <-- you are here
```

Without it, `uv sync` fails with `Distribution not found at: .../elote`.

### Sync the environment

From the project root:

```shell
uv sync
```

That one command creates `.venv` (using the Python in `.python-version`),
installs every dependency from `uv.lock`, and installs this package itself as
editable -- there's no separate `pip install -e .` step, and no
`requirements.txt`.

### Running commands

`uv run` uses the project environment, so you don't need to activate anything:

```shell
uv run python -c "import mischooldata_etl"
```

If you'd rather activate it the usual way:

```shell
source .venv/bin/activate     # OSX or Linux
.venv\Scripts\Activate.ps1    # Windows PowerShell
```

## When running a file you have to run from the root file

```bash
uv run python ./mischooldata_etl/eem/process_eem.py
```


## Dataset Standards

### Source file paths

The `source_file` column in each module's `conf/dataset_years.csv` is a path
**relative to the vault**, with **forward slashes** as the separator -- e.g.
`DATA/Education/Graduation and drop outs/Data/2007/Raw/graduation_dropout_2007.csv`.

Don't put the vault root (`V:\`, `/home/you/vault`) in the CSV. Transforms join
it on at read time:

```python
Path(config["vault_location"]) / year["source_file"]
```

`Path` translates the forward slashes to the host separator, so the same CSV
works on Windows and Linux. Use forward slashes even if you're on Windows.


### Break out columns

Take care when querying these datasets, because they often include break outs. These breakouts are handled by two columns typically, `report_category` and `report_subgroup`. The following are typical `report_category` values:

- `total`
- `grade`
- `race`
- `equity`
- `gender`

In the raw data the dataset total can live in various `report_category` columns -- no matter what category it comes in as, we set the `report_category` column to `total` and the `report_subgroup` column to `total`.

#### Two-digit grade strings

All grades should be represented as two digit strings. Sometimes these values are included in category breakouts (hence the string) where they are reported as two-digit strings. Even if the grade category does have other report groups on it, the 2-digit string standard should be followed.

Kindergarten is coded as `00`. Prekindergarten is `prek`.

## Release Dates

### Late summer
- *are we able to get preview access?*
- *does the freep?* -- their reports are published Aug 31st

- Update EEM
- Update assessments
    - 3g ELA
    - 8g Math
    - College readiness
- *Others ?*

### Late winter
