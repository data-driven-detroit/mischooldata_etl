import click
import pandas as pd
import pandera as pa
from sqlalchemy.orm import sessionmaker
from pandera.errors import SchemaError, SchemaErrors
import tomli

from metadata_audit.capture import record_metadata

from mischooldata_etls import setup_logging, db_engine, metadata_engine
from mischooldata_etls.schema import SchoolAttendance

logger = setup_logging()

table_name = "school_attendance"

with open("metadata.toml", "rb") as md:
    metadata = tomli.load(md)


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
            'DistrictCode': 'district_code',
            'BuildingCode': 'building_code',
            'ReportCategory': 'report_category',
            'ReportSubGroup': 'report_subgroup',
            'TotalStudents': '__total_students',
            'ChronicallyAbsentCount': '__chronically_absent',
        })
        .assign(
            total_students=lambda df: df["__total_students"],
            total_students_error=lambda df: df["__total_students"],
            chronically_absent=lambda df:df["__chronically_absent"],
            chronically_absent_error=lambda df:df["__chronically_absent"],

        )
    )

    logger.info(result.columns)

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
