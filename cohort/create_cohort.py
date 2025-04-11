from datetime import date
import pandas as pd


def grade_string(number):
    if number == -1:
        return "pre_k"
    return str(number).zfill(2)


rows = []
for year in range(2003, 2097):
    start_year = max(year - 13, 2003)
    cohort_end_date = date(year=year, month=6, day=30)

    for y in range(start_year, year+1):
        rows.append(
            {
                "grade": grade_string(12 - (year - y)),
                "data_end_date": date(year=y, month=6, day=30).isoformat(),
                "cohort_end_date": cohort_end_date
            }
        )


pd.DataFrame(rows)