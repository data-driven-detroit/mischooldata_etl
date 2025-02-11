import click
import pandas as pd
import pandera as pa
from pandera.typing import Series
from pandera.errors import SchemaError, SchemaErrors
import tomli

from mischooldata_etls import setup_logging, db_engine, metadata_engine
from metadata_audit.capture import record_metadata
from sqlalchemy.orm import sessionmaker

logger = setup_logging()

table_name = "mischooldata_etls"

with open("metadata.toml", "rb") as md:
    metadata = tomli.load(md)


# Every loader script starts with a pydantic model -- This is both to
# validate the clean-up process output and to ensure that fields agree
# with the metadata provided to the metadata system.
class TableSchema(pa.DataFrameModel):
    """
    This is one of two tables that make sense to include
    """

    code: str = pa.Field(unique=True)
    title: str = pa.Field()
    description: str = pa.Field(nullable=True)

    class Config:  # type: ignore
        strict = True
        coerce = True

    @pa.check("description")
    def max_nulls(cls, description: Series[str]) -> bool:
        """
        It's okay for some of these to be null, but if there are too many
        it could indicate a problem.
        """
        return description.isna().sum() < 200


@click.command()
@click.argument("edition_date")
@click.option("-m", "--metadata_only", is_flag=True, help="Skip uploading dataset.")
def main(edition_date, metadata_only):
    if metadata_only:
        logger.info("Metadata only was selected.")

    edition = metadata["tables"][table_name]["editions"][edition_date]

    result = (
        pd.read_csv(edition["raw_path"])
        .rename(
            columns={
                "Code": "code",
                "Title": "title",
                "Description": "description",
            }
        )
        .drop("Unnamed: 0", axis=1)
    )

    logger.info(f"Cleaning {table_name} was successful validating schema.")

    # Validate
    try:
        validated = TableSchema.validate(result)
        logger.info(
            f"Validating {table_name} was successful. Recording metadata."
        )
    except (SchemaError, SchemaErrors) as e:
        logger.error(f"Validating {table_name} failed.", e)
        return -1 # Don't continue if you can't validate!

    with metadata_engine.connect() as db:
        logger.info("Connected to metadata schema.")

        record_metadata(
            TableSchema,
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
