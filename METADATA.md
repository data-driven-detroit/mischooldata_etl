# Table and column metadata

How descriptions, units, provenance and constraints for a table get from this
package into the Dagster UI. There are three pieces, and each owns one job:

| Piece | Lives in | Owns |
|---|---|---|
| pandera `DataFrameModel` | `<dataset>/schema.py` | What the table *is*: columns, types, constraints, descriptions, free-form metadata |
| `Asset` registry | `mischooldata_etl/datasets.py` | Which model describes which table |
| Dagster code location | the pipeline project (`defs.py`) | Translating the model into Dagster's metadata vocabulary |

The model is the single source of truth. The same class that validates a
DataFrame at load time is the one Dagster reads to describe the table, so the
docs can't drift from what's actually enforced.

Nothing here changes the rule in `DAGSTER.md`: this package never imports
Dagster. The translation happens entirely on the Dagster side.

> **Status.** `student_counts` and `non_resident` have schemas today. The
> `Asset.schema` field and the Dagster translation below are the proposed design
> and are not wired up yet.

## 1. Describe the table in pandera

Everything goes on the model. `pa.Field` takes `title`, `description` and a
`metadata` dict; `Config` takes the same for the table as a whole.

```python
# mischooldata_etl/student_counts/schema.py
from datetime import date
import pandera.pandas as pa
from pandera.typing.pandas import Series


class StudentCounts(pa.DataFrameModel):
    district_code: str = pa.Field(
        coerce=True,
        description="5-digit MDE district code.",
        metadata={"source_field": "DistrictCode"},
    )
    building_code: str = pa.Field(
        coerce=True,
        description="5-digit MDE building code.",
        metadata={"source_field": "BuildingCode"},
    )
    count: int = pa.Field(
        nullable=False,
        description="Number of students in the subgroup.",
        metadata={"unit": "students", "suppressed_below": 10},
    )
    # ...

    class Config:
        strict = True
        description = "Student headcounts by building, category and subgroup."
        metadata = {"source": "MI School Data", "grain": "building x subgroup x year"}

    @pa.check("district_code")
    def district_code_correct_len(cls, district_code: Series[str]) -> Series[bool]:
        return district_code.str.len() == 5
```

What to put where:

- **`description`**: one sentence a reader of the table needs. It shows up as the
  column description in Dagster.
- **`metadata`**: short, flat key/value facts such as units, the source field name,
  suppression rules or PII flags. Keep the values scalars, since Dagster turns them
  into strings (see below).
- **Constraints** (`nullable`, `unique`, `@pa.check`): keep writing these for
  validation as usual. They come through to Dagster automatically; you don't
  restate them as metadata.

### Reading it back

| Want | Call |
|---|---|
| A column's `title` / `description` | `Model.to_schema().columns[name].title` / `.description` |
| A column's `metadata` dict | `Model.to_schema().columns[name].metadata` |
| A column's `nullable` / `unique` / `dtype` / `checks` | same object, `.nullable`, `.unique`, `.dtype`, `.checks` |
| The table description | `Model.to_schema().description` |
| **The table metadata** | **`Model.get_metadata()[Model.__name__]["dataframe"]`** |

**The one trap:** `Model.to_schema().metadata` returns `None` even when
`Config.metadata` is set. The table-level dict is only reachable through
`get_metadata()`, which returns

```python
{"StudentCounts": {"columns": {"count": {"unit": "students", ...}, ...},
                   "dataframe": {"source": "MI School Data", ...}}}
```

## 2. Point the registry at the model

`Asset` grows one optional field. It holds the model class, which is plain
pandera and needs no Dagster:

```python
# mischooldata_etl/datasets.py
@dataclass(frozen=True)
class Asset:
    name: str
    group: str
    materialize: Callable[[], None]
    deps: tuple[str, ...] = field(default_factory=tuple)
    description: str = ""
    schema: type | None = None  # a pandera DataFrameModel describing the table
```

```python
from .student_counts.schema import StudentCounts
from .non_resident.schema import NonResident

def _simple(name, transform, load, schema=None) -> Asset:
    return Asset(name=name, group=name, materialize=_chain(transform, load), schema=schema)

_ASSETS = [
    ...
    _simple("student_counts", transform_student_counts, load_student_counts, StudentCounts),
    _simple("non_resident", transform_non_resident, load_non_resident, NonResident),
    ...
]
```

Importing the schema modules is cheap and reads no config, so `datasets.py`
still imports cleanly without `config.toml`.

Tables without a model just leave `schema=None` and get no column metadata. Adding
a schema to a module is then all it takes for its columns to show up in Dagster.

## 3. Translate it in the Dagster code location

Dagster describes columns with `TableSchema` / `TableColumn`, attached under
the reserved metadata key `dagster/column_schema`. That powers the asset's
**Columns** tab. The mapping is mechanical:

| pandera | Dagster |
|---|---|
| column name | `TableColumn.name` |
| `col.dtype` | `TableColumn.type` (as a string) |
| `col.description` | `TableColumn.description` |
| `col.metadata` | `TableColumn.tags` (values stringified) |
| `col.nullable`, `col.unique` | `TableColumnConstraints(nullable=, unique=)` |
| `col.checks` | `TableColumnConstraints(other=[check names])` |
| `Config.description` | asset `description` (fallback) |
| `Config.metadata` | top-level asset metadata entries |

```python
# pipeline/assets.py (or defs.py)
from dagster import (
    AssetExecutionContext, AssetKey, TableColumn, TableColumnConstraints,
    TableSchema, asset,
)

from mischooldata_etl.datasets import SCHEMA


def column_schema(model) -> TableSchema:
    schema = model.to_schema()
    return TableSchema(columns=[
        TableColumn(
            name=name,
            type=str(col.dtype),
            description=col.description,
            tags={k: str(v) for k, v in (col.metadata or {}).items()},
            constraints=TableColumnConstraints(
                nullable=col.nullable,
                unique=col.unique,
                other=[c.name for c in col.checks if c.name],
            ),
        )
        for name, col in schema.columns.items()
    ])


def asset_metadata(a) -> dict:
    md = {"dagster/table_name": f"{SCHEMA}.{a.name}"}
    if a.schema is not None:
        md["dagster/column_schema"] = column_schema(a.schema)
        md.update(a.schema.get_metadata()[a.schema.__name__]["dataframe"] or {})
    return md


def asset_description(a) -> str | None:
    if a.description:
        return a.description
    if a.schema is not None:
        return a.schema.to_schema().description
    return None


def build_asset(a):
    @asset(
        key=AssetKey([SCHEMA, a.name]),
        group_name=a.group,
        deps=[AssetKey([SCHEMA, d]) for d in a.deps],
        description=asset_description(a),
        metadata=asset_metadata(a),
    )
    def _asset(context: AssetExecutionContext) -> None:
        context.log.info("materializing %s.%s", SCHEMA, a.name)
        a.materialize()

    return _asset
```

This is definition-time metadata: it describes the table and is the same on
every run. It's computed once when the code location loads, from the class
alone, and doesn't touch the vault or the database.

### Why not `dagster-pandera`?

`dagster-pandera` turns a schema into a `DagsterType` that validates an asset's
*return value*. These assets return `None` and write to the database
themselves, so there is nothing for a `DagsterType` to check. Validation stays
where it already is, in each module's `load.py`. Dagster only needs the
*description*, and the mapping above provides it with no extra dependency.

## Things that will bite you

**Tags are strings.** `TableColumn.tags` accepts only string values; the
translation `str()`s them. Nested dicts or lists in `Field(metadata=...)` will
render as their Python repr, so keep the metadata flat.

**Table metadata only comes from `get_metadata()`.** See above. If a table's
`Config.metadata` doesn't show up in Dagster, this is why.

**Check names, not check logic.** `@pa.check` methods show up in Dagster's
"other" constraints by method name (`district_code_correct_len`). Name them so
the name says what they enforce.

**`strict = True` makes the model the column list.** The transforms already use
`list(Model.to_schema().columns)` to choose their output columns, so the model
Dagster shows is exactly what lands in the table. Keep it that way. Don't
maintain a second column list.

**Metadata keys must not collide with Dagster's.** Anything under `dagster/` is
reserved. Keep `Config.metadata` keys plain (`source`, `grain`).

## Run-time metadata, if you want it

Everything above is static. If you also want per-run facts in the materialization
history, such as row count or years covered, `materialize` would have to return
them, and the asset would yield them:

```python
def _asset(context):
    stats = a.materialize()          # e.g. {"dagster/row_count": 184_213}
    return MaterializeResult(metadata=stats or {})
```

That is an interface change to `Asset.materialize`, which currently returns
`None`, so leave it until someone asks for it.

---

*The pandera calls above were checked against pandera 0.33.1 (`pyproject.toml`
allows `>=0.20.0`; recheck `get_metadata()` on older versions). The Dagster
translation was checked against Dagster 1.13.24 with a stand-in model: the
definitions load and the column schema, tags, constraints and table metadata all
come through. It has not been run against this repo's `ASSETS`, since the
`Asset.schema` field doesn't exist yet.*
