import pandas as pd
from pyspark.sql.types import DateType, IntegerType, DataType, LongType
from pyspark.sql.functions import (
    udf,
    col,
    lit,
    year,
    month,
    upper,
    to_date,
    monotonically_increasing_id,
)

from json import load

with open("schema.json") as json_file:
    SCHEMA = load(json_file)


def SAS_to_date(date):
    """Convert SAS Timestamp datatype to pandas Timestamp datatype
    Arguments:
            date: spark dataframe column.
    Returns:
            pandas Timestamp
    """
    if date is not None:
        return pd.to_timedelta(date, unit="D") + pd.Timestamp("1960-1-1")


SAS_to_date_udf = udf(SAS_to_date, DateType())


def to_integer(column):
    return column.cast(IntegerType())


def rename_columns(table, new_columns):
    """Renaming table columns with new columns
    Arguments:
            table: spark dataframe.
            new_columns: new columns names.
    Returns:
            spark dataframe
    """
    for old, new in zip(table.columns, new_columns):
        table = table.withColumnRenamed(old, new)
    return table


def apply_schema(table, schema):
    for column in schema:
        data_type = schema[column]
        if data_type == "TIMESTAMP":
            table = table.withColumn(column, SAS_to_date_udf(table[column]))
        else:
            table = table.withColumn(column, table[column].cast(data_type))
    return table



def drop_immigration_empty_columns(df):
    """This function drops columns that have missing values with over 90% in immigration dataset.
    Arguments:
            table: spark dataframe.
    Returns:
            spark dataframe
    """
    # from EDA (in the jupyter notebook)
    # ['occup', 'entdepu' and 'insnum'] columns have missing values with over 90%;
    #  hence we should drop them
    dropped_columns = ["occup", "entdepu", "insnum"]
    df = df.drop(*dropped_columns)
    # drop rows where all values are missed.
    df = df.dropna(how="all")
    return df


def drop_temperature_nulls_and_duplicates_rows(df):
    """This function drops rows that have no average temperature value and duplicate rows in temperature dataset.
    Arguments:
            table: spark dataframe.
    Returns:
            spark dataframe
    """
    # drop rows that have no average temperature value
    df = df.dropna(subset=["AverageTemperature"])
    # drop duplicate rows
    df = df.drop_duplicates(subset=["Country", "City", "dt"])
    return df


def drop_demographics_nulls_and_duplicates_rows(df):
    """This function drops rows with missing values and duplicate rows in demographics dataset.
    Arguments:
            table: spark dataframe.
    Returns:
            spark dataframe
    """
    # drop rows with missing values
    subset_columns = [
        "Male Population",
        "Female Population",
        "Foreign-born",
        "Average Household Size",
        "Number of Veterans",
    ]
    df = df.dropna(subset=subset_columns)
    # drop duplicate columns
    df = df.drop_duplicates(subset=["City", "State", "State Code", "Race"])

    return df


def fact_immigration(table):
    """Clean fact_immigration table by renaming columns and add more columns
    Arguments:
            table: spark dataframe.
    Returns:
            spark dataframe
    """
    new_columns = [
        "cic_id",
        "year",
        "month",
        "city_code",
        "state_code",
        "arrive_date",
        "departure_date",
        "mode",
        "visa",
    ]
    table = rename_columns(table, new_columns)
    # new columns
    table = table.withColumn("country", lit("United States"))
    table = table.withColumn("immigration_id", monotonically_increasing_id())

    schema = SCHEMA["fact_immigration"]
    table = apply_schema(table, schema)
    return table


def dim_immigration_personal(table):
    """Clean dim_immigration_personal table by renaming columns.
    Arguments:
            table: spark dataframe.
    Returns:
            spark dataframe
    """
    new_columns = [
        "cic_id",
        "citizen_country",
        "residence_country",
        "birth_year",
        "gender",
    ]
    table = rename_columns(table, new_columns)
    table = table.withColumn("immi_personal_id", monotonically_increasing_id())

    schema = SCHEMA["dim_immigration_personal"]
    table = apply_schema(table, schema)
    return table


def dim_immigration_airline(table):
    """Clean dim_immigration_airline table by renaming columns.
    Arguments:
            table: spark dataframe.
    Returns:
            spark dataframe
    """
    new_columns = ["cic_id", "airline", "admin_num", "flight_number", "visa_type"]
    table = rename_columns(table, new_columns)
    table = table.withColumn("immi_airline_id", monotonically_increasing_id())

    schema = SCHEMA["dim_immigration_airline"]
    table = apply_schema(table, schema)
    return table


# def dim_temperature(table):
#     """Clean dim_temperature table by renaming columns and add more columns
#     Arguments:
#             table: spark dataframe.
#     Returns:
#             spark dataframe
#     """
#     new_columns = ["dt", "avg_temp", "avg_temp_uncertainty", "city", "country"]
#     table = rename_columns(table, new_columns)

#     table = table.withColumn("dt", to_date(col("dt")))

#     table = table.withColumn("avg_temp", col("avg_temp").cast("FLOAT"))
#     table = table.withColumn(
#         "avg_temp_uncertainty", col("avg_temp_uncertainty").cast("FLOAT")
#     )

#     table = table.withColumn("city", upper(col("city")))
#     table = table.withColumn("country", upper(col("country")))

#     # added columns
#     table = table.withColumn("year", year(table["dt"]))
#     table = table.withColumn("month", month(table["dt"]))

#     #     schema = SCHEMA["dim_temperature"]
#     #     table = apply_schema(table, schema)
#     return table

def dim_temperature(table):
    """Clean dim_temperature table by renaming columns and add more columns
    Arguments:
            table: spark dataframe.
    Returns:
            spark dataframe
    """
    new_columns = ["dt", "avg_temp", "avg_temp_uncertainty", "city", "country"]
    table = rename_columns(table, new_columns)

    table = table.withColumn("city", upper(col("city")))
    table = table.withColumn("country", upper(col("country")))

    # added columns
    table = table.withColumn("year", year(table["dt"]))
    table = table.withColumn("month", month(table["dt"]))

    schema = SCHEMA["dim_temperature"]
    table = apply_schema(table, schema)
    return table


def dim_demog_population(table):
    """Clean dim_demog_population table by renaming columns
    Arguments:
            table: spark dataframe.
    Returns:
            spark dataframe
    """
    new_columns = [
        "city",
        "state",
        "male_population",
        "female_population",
        "num_veterans",
        "foreign_born",
        "race",
    ]
    table = rename_columns(table, new_columns)
    table = table.withColumn("demog_pop_id", monotonically_increasing_id())

    schema = SCHEMA["dim_demog_population"]
    table = apply_schema(table, schema)
    return table


def dim_demog_statistics(table):
    """Clean dim_demog_statistics table by renaming columns and add more columns
    Arguments:
            table: spark dataframe.
    Returns:
            spark dataframe
    """
    new_columns = ["city", "state", "median_age", "avg_household_size"]
    table = rename_columns(table, new_columns)

    table = table.withColumn("city", upper(col("city")))
    table = table.withColumn("state", upper(col("state")))
    table = table.withColumn("demog_stat_id", monotonically_increasing_id())

    schema = SCHEMA["dim_demog_statistics"]
    table = apply_schema(table, schema)
    return table


def state_code(contents):
    codes = {}
    for states in contents[982:1036]:
        pair = states.split("=")
        code, state = pair[0].strip("\t").strip("'"), pair[1].strip().strip("'")
        codes[code] = state
    return codes


def city_code(contents):
    codes = {}
    for cities in contents[303:962]:
        pair = cities.split("=")
        code, city = pair[0].strip("\t").strip().strip("'"), pair[1].strip(
            "\t"
        ).strip().strip("''")
        codes[code] = city
    for code in codes.keys():
        codes[code] = codes[code].split(",")[0]
    return codes


def country_code(contents):
    codes = {}
    for countries in contents[10:298]:
        pair = countries.split("=")
        code, country = pair[0].strip(), pair[1].strip().strip("'")
        codes[code] = country
    return codes
