"""The shared transform/load steps the dataset modules build on."""
from pathlib import Path
import json
import logging
import pandas as pd
import pandera.pandas as pa
from sqlalchemy.types import Date
from inequalitytools import parse_to_inequality

from .config import get_config
from .db import get_db_engine


logger = logging.getLogger(__name__)


# `data_type` values allowed in a module's conf/output_schema.csv. A blank
# data_type means "leave the column as pandas reads it": use it only for columns
# whose contents haven't been checked yet, and fill it in once they have.
OUTPUT_TYPES = {
    "str": str,
    "int": "Int64",
    "float": "Float64",
    "date": pa.Date,
}

# Code columns zero-padded to a fixed width. A field reference can replace this
# with its own "padding" mapping when its codes differ.
DEFAULT_PADDING = {"isd_code": 2, "district_code": 5, "building_code": 5}


def load_field_reference(working_dir: Path, field_reference_file: str) -> dict:
    return json.loads((working_dir / "conf" / field_reference_file).read_text())


def load_output_schema(working_dir: Path) -> pd.DataFrame:
    return pd.read_csv(working_dir / "conf" / "output_schema.csv")


def output_dataframe_schema(output_schema: pd.DataFrame) -> pa.DataFrameSchema:
    """The pandera schema a module's output_schema.csv describes.

    Exactly the listed columns, in order, each coerced to its data_type.
    start_date and end_date may never be null; other columns may.
    """
    columns = {}
    for row in output_schema.itertuples():
        data_type = None if pd.isna(row.data_type) else row.data_type
        if data_type is not None and data_type not in OUTPUT_TYPES:
            raise ValueError(
                f"output_schema.csv: unknown data_type {data_type!r} for {row.column_name}; "
                f"expected one of {sorted(OUTPUT_TYPES)} or blank"
            )
        columns[row.column_name] = pa.Column(
            OUTPUT_TYPES.get(data_type),
            coerce=data_type is not None,
            nullable=row.column_name not in ("start_date", "end_date"),
        )
    return pa.DataFrameSchema(columns, strict=True, ordered=True)


def unwrap_value(inequality):
    """The value half of a parsed Inequality -- see transform_process below."""
    value, _ = inequality.unwrap()
    return value


def unwrap_error(inequality):
    """The error half of a parsed Inequality -- see transform_process below."""
    _, error = inequality.unwrap()
    return error


def transform_process(frame, field_reference):
    frame = frame.rename(columns=field_reference["renames"])

    for field in field_reference["suppressed_cols"]:
        frame[[field, f"{field}_error"]] = (
            frame[field]
            .apply(parse_to_inequality)
            .apply(lambda i: i.unwrap())
            .to_list()
        )

    # Numeric columns whose suppressed values ("< 10", "*", ...) should simply
    # become nulls, with no inequalitytools parsing and no `_error` column.
    for field in field_reference.get("coerce_cols", []):
        frame[field] = pd.to_numeric(frame[field], errors="coerce")

    return frame


def check_error_columns(field_reference, output_schema, source):
    """Every suppressed column must keep its `_error` column in the output.

    apply_output_schema keeps only the columns output_schema.csv lists, so an
    `_error` column missing from it would be silently dropped -- and downstream
    work relies on those error bounds.
    """
    listed = set(output_schema["column_name"])
    missing = [
        f"{c}_error" for c in field_reference["suppressed_cols"] if f"{c}_error" not in listed
    ]
    if missing:
        raise ValueError(
            f"{source}: suppressed_cols produce {missing}, but output_schema.csv "
            "doesn't list them, so they'd be dropped. Add them to output_schema.csv."
        )

    # inequalitytools estimates a suppressed value as the midpoint of its range
    # -- "<5" becomes 2.5 +/- 2.5 -- so these columns can't be integers even
    # when the underlying data are counts.
    types = dict(zip(output_schema["column_name"], output_schema["data_type"]))
    parsed = [
        col
        for c in field_reference["suppressed_cols"]
        for col in (c, f"{c}_error")
        if types.get(col) != "float"
    ]
    if parsed:
        raise ValueError(
            f"{source}: {parsed} come from inequalitytools, which can produce "
            "fractional estimates (\"<5\" -> 2.5); set their data_type to float "
            "in output_schema.csv."
        )


def apply_output_schema(frame, output_schema):
    """Add any missing columns with their defaults, then validate against the schema."""
    for row in output_schema.itertuples():
        if row.column_name not in frame.columns:
            default = row.default_value
            frame[row.column_name] = pd.NA if pd.isna(default) or default in ("", "NA") else default

    columns = output_schema["column_name"].tolist()
    return output_dataframe_schema(output_schema).validate(frame[columns])


def apply_padding(frame, padding=None):
    for col, width in (padding or DEFAULT_PADDING).items():
        frame[col] = frame[col].str.zfill(width)

    return frame


def check_source_columns(raw, field_reference, source_path):
    """Fail with the file and its actual headers when expected columns are missing.

    A rename whose source column isn't in the file silently does nothing, so
    without this the failure surfaces later as a bare KeyError on the renamed
    name, with no hint which file or header was to blame.
    """
    renames = field_reference["renames"]
    produced = {renames.get(c, c) for c in raw.columns}
    # Several source names may map to one output column (header spellings that
    # changed between years); only complain when none of them is present.
    missing = {}
    for source, target in renames.items():
        if target not in produced:
            missing.setdefault(target, []).append(source)
    if not missing:
        return

    message = (
        f"{source_path}: no source column found for {sorted(missing)} "
        f"(looked for {missing}). Columns actually in the file: {list(raw.columns)}"
    )
    # The padded code columns are what every row is keyed on; without one the
    # file can't be used at all.
    if any(code in missing for code in field_reference.get("padding", DEFAULT_PADDING)):
        raise KeyError(message)
    logger.warning(message)


def generic_transform(module_name: str, working_dir: Path):
    """Rebuild output/combined_years.csv from every year in dataset_years.csv."""
    config = get_config()

    output_dir = working_dir / "output" / "combined_years.csv"
    output_dir.parent.mkdir(exist_ok=True)

    dataset_years = pd.read_csv(working_dir / "conf" / "dataset_years.csv")
    output_schema = load_output_schema(working_dir)

    logger.info("Processing %s date ranges for %s", len(dataset_years), module_name)

    mode, header = "w", True
    for _, year in dataset_years.iterrows():
        logger.info("Opening %s", year["source_file"])

        field_reference = json.loads(
            (working_dir / "conf" / year["field_reference_file"]).read_text()
        )
        check_error_columns(field_reference, output_schema, year["field_reference_file"])

        source_path = Path(config["vault_location"]) / year["source_file"]
        raw = pd.read_csv(
            source_path,
            dtype=field_reference["in_types"],
            low_memory=False,
        )
        check_source_columns(raw, field_reference, source_path)

        frame = (
            raw
            .rename(columns=field_reference["renames"])
            .pipe(lambda frame: transform_process(frame, field_reference))
            .pipe(apply_padding, field_reference.get("padding"))
            .assign(start_date=year["start_date"], end_date=year["end_date"])
            .pipe(lambda frame: apply_output_schema(frame, output_schema))
        )

        frame.to_csv(output_dir, mode=mode, header=header, index=False)
        mode, header = "a", False


def generic_load(table_name: str, working_dir: Path, special_processing=None):
    output_schema = load_output_schema(working_dir)
    schema = output_dataframe_schema(output_schema)

    # The scratch CSV loses types, so read text columns as text (keeping the
    # codes' leading zeros) and let the schema restore everything else --
    # dates included, which otherwise land in the database as TEXT.
    text_columns = {
        row.column_name: "str" for row in output_schema.itertuples() if row.data_type == "str"
    }
    # Pin the date columns' SQL type too, so it doesn't depend on what pandas
    # infers from the first chunk.
    sql_types = {
        row.column_name: Date() for row in output_schema.itertuples() if row.data_type == "date"
    }

    # Full replace in one transaction: if any chunk fails, the old table survives.
    if_exists = "replace"
    with get_db_engine().begin() as db:
        for i, portion in enumerate(pd.read_csv(
            working_dir / "output" / "combined_years.csv",
            chunksize=20_000,
            dtype=text_columns,
        ), start=1):
            logger.info("Loading chunk %s of %s into database.", i, table_name)

            if special_processing:
                portion = special_processing(portion)

            portion = schema.validate(portion)

            portion.to_sql(
                table_name, db, schema="education", if_exists=if_exists, index=False,
                dtype=sql_types,
            )
            if_exists = "append"
