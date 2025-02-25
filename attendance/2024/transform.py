def transform_attendance(logger):
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