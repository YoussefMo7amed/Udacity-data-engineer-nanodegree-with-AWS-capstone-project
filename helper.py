from pyspark.sql.functions import col, sum

from pyspark.sql.functions import avg
from pyspark.sql.functions import (
    isnan,
    when,
    count,
    col,
    sum,
    udf,
    dayofmonth,
    dayofweek,
    month,
    year,
    weekofyear,
)
from pyspark.sql.functions import monotonically_increasing_id
from pyspark.sql.types import *


def thousands_separator(number):
    return "{:,}".format(number)


def print_number_of_records(records_number):
    print(f"number of records:\t{thousands_separator(records_number)}")


def missing_values_as_df(df, ascending_sorting=False):
    # count missing values per column
    missing_count = df.select(
        [sum(col(c).isNull().cast("int")).alias(c) for c in df.columns]
    )

    # create a new DataFrame with the missing values count
    missing_count_df = missing_count.selectExpr(
        "stack("
        + str(len(df.columns))
        + ", "
        + ", ".join(["'" + c + "', " + c for c in df.columns])
        + ") as (column_name, missing_count)"
    ).filter("missing_count > 0")
    total_rows = df.count()
    missing_count_df = missing_count_df.withColumn(
        "missing_percentage", (missing_count_df.missing_count / total_rows) * 100
    )

    return missing_count_df.toPandas().sort_values(
        by=["missing_percentage"], ascending=ascending_sorting
    )


def compare_rows(before, after):
    print("Total records before cleaning:")
    print(thousands_separator(before))
    print("Total records after cleaning:")
    print(thousands_separator(after))
    print("The difference is")
    print(thousands_separator((before - after)))
