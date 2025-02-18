import datetime
import click
import pandas as pd
from sqlalchemy.orm import sessionmaker
from pandera.errors import SchemaError, SchemaErrors
import tomli
from inequalitytools import parse_to_inequality
from metadata_audit.capture import record_metadata

from mischooldata_etls import (
    setup_logging,
    db_engine,
    metadata_engine,
    pad_code,
    unwrap_value,
    unwrap_error
)
from mischooldata_etls.schema import SchoolAttendance

logger = setup_logging()

table_name = "eem_schools"

with open("metadata.toml", "rb") as md:
    metadata = tomli.load(md)

category_maps = {
    "Grade": "grade",
    "Ethnicity": "ethnicity",
    "Gender": "gender",
    "Economically Disadvantaged": "economically_disadvantaged",
    "Students with Disabilities": "students_with_disabilities",
    "Homeless": "homeless",
    "English Learners": "english_learners",
    "Migrant Status": "migrant_status",
}

subgroup_maps = {
    "All": "all",
    "Not Migrant": "not_migrant",
    "Not Homeless": "not_homeless",
    "Not English Learners": "not_english_learner",
    "Male": "male",
    "Economically Disadvantaged": "economically_disadvantaged",
    "Female": "female",
    "White, not of Hispanic origin": "white_not_hispanic",
    "Students with Disabilities": "with_disability",
    "Not Economically Disadvantaged": "not_economically_disadvantaged",
    "Students without IEP": "without_iep",
    "Hispanic": "hispanic",
    "Two or More Races": "two_or_more_races",
    "Black, not of Hispanic origin": "black_not_hispanic",
    "Homeless": "homeless",
    "English Learners": "english_learner",
    "Asian": "asian",
    "American Indian or Alaska Native": "american_indian_alaska_native",
    "Native Hawaiian or Other Pacific Islander": "native_hawaiian_pacific_islander",
    "Migrant": "migrant",
}


@click.command()
@click.argument("edition_date")
@click.option("-m", "--metadata_only", is_flag=True, help="Skip uploading dataset.")
def main(edition_date, metadata_only):
    if metadata_only:
        logger.info("Metadata only was selected.")

    edition = metadata["tables"][table_name]["editions"][edition_date]



    result = (
        pd.read_csv(edition["raw_path"])
        .rename(columns={
            'DistrictCode': '__district_code',
            'BuildingCode': '__building_code',
            'ReportCategory': '__report_category',
            'ReportSubGroup': '__report_subgroup',
            'TotalStudents': '__total_students',
            'ChronicallyAbsentCount': '__chronically_absent',
        })
        .assign(
            district_code=lambda df: df["__district_code"].apply(pad_code),
            building_code=lambda df: df["__building_code"].apply(pad_code),
            report_category=lambda df: df["__report_category"].apply(lambda key: category_maps.get(key, key)),
            report_subgroup=lambda df: df["__report_subgroup"].apply(lambda key: subgroup_maps.get(key, key)),
            total_students=lambda df: df["__total_students"].apply(parse_to_inequality).apply(unwrap_value),
            total_students_error=lambda df: df["__total_students"].apply(parse_to_inequality).apply(unwrap_error),
            chronically_absent=lambda df:df["__chronically_absent"].apply(parse_to_inequality).apply(unwrap_value),
            chronically_absent_error=lambda df:df["__chronically_absent"].apply(parse_to_inequality).apply(unwrap_error),
            start=datetime.date.fromisoformat(edition["start"]),
            end=datetime.date.fromisoformat(edition["end"]),
        )
        .astype(
            {
                "total_students": pd.Int64Dtype(),
                "total_students_error": pd.Int64Dtype(),
                "chronically_absent": pd.Int64Dtype(),
                "chronically_absent_error": pd.Int64Dtype(),
            }
        )
        .query("(district_code != '00000') & (building_code != '00000')")[[
            "district_code",
            "building_code",
            "report_category",
            "report_subgroup",
            "total_students",
            "total_students_error",
            "chronically_absent",
            "chronically_absent_error",
            "start",
            "end",
        ]]
    )


    logger.info(result["report_subgroup"].value_counts())
    logger.info(f"Cleaning {table_name} was successful validating schema.")

    # Validate
    try:
        validated = SchoolAttendance.validate(result)
        logger.info(
            f"Validating {table_name} was successful. Recording metadata."
        )
    except (SchemaError, SchemaErrors) as e:
        logger.error(f"Validating {table_name} failed.", e)
        return -1 # Don't continue if you can't validate!

    with metadata_engine.connect() as db:
        logger.info("Connected to metadata schema.")

        record_metadata(
            SchoolAttendance,
            __file__,
            table_name,
            metadata,
            edition_date,
            validated,
            sessionmaker(bind=db)(),
            logger,
        )

        db.commit()
        logger.info("successfully recorded metadata")

    if not metadata_only:
        with db_engine.connect() as db:
            logger.info("Metadata recorded, pushing data to db.")

            validated.to_sql(  # type: ignore
                table_name, db, index=False, schema=metadata["schema"], if_exists="append"
            )
    else:
        logger.info("Metadata only specified, so process complete.")

if __name__ == "__main__":
    main()