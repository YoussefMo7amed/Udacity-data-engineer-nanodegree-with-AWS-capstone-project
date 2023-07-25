from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    LongType,
    DateType,
    FloatType,
)

city_code = StructType(
    [StructField("code", StringType(), True), StructField("city", StringType(), True)]
)
dim_immigration_airline = StructType(
    [
        StructField("cic_id", LongType(), True),
        StructField("airline", StringType(), True),
        StructField("admin_num", LongType(), True),
        StructField("flight_number", StringType(), True),
        StructField("visa_type", StringType(), True),
        StructField("immi_airline_id", IntegerType(), True),
    ]
)
dim_demog_statistics = StructType(
    [
        StructField("city", StringType(), True),
        StructField("state", StringType(), True),
        StructField("median_age", IntegerType(), True),
        StructField("avg_household_size", FloatType(), True),
        StructField("demog_stat_id", IntegerType(), True),
    ]
)
dim_demog_population = StructType(
    [
        StructField("city", StringType(), True),
        StructField("state", StringType(), True),
        StructField("male_population", IntegerType(), True),
        StructField("female_population", IntegerType(), True),
        StructField("num_veterans", IntegerType(), True),
        StructField("foreign_born", IntegerType(), True),
        StructField("race", StringType(), True),
        StructField("demog_pop_id", IntegerType(), True),
    ]
)
country_code = StructType(
    [
        StructField("code", StringType(), True),
        StructField("country", StringType(), True),
    ]
)
state_code = StructType(
    [StructField("code", StringType(), True), StructField("state", StringType(), True)]
)
dim_immigration_personal = StructType(
    [
        StructField("cic_id", LongType(), True),
        StructField("citizen_country", IntegerType(), True),
        StructField("residence_country", IntegerType(), True),
        StructField("birth_year", IntegerType(), True),
        StructField("gender", StringType(), True),
        StructField("immi_personal_id", IntegerType(), True),
    ]
)
dim_temperature = StructType(
    [
        StructField("dt", DateType(), True),
        StructField("avg_temp", FloatType(), True),
        StructField("avg_temp_uncertainty", FloatType(), True),
        StructField("city", StringType(), True),
        StructField("country", StringType(), True),
        StructField("year", IntegerType(), True),
        StructField("month", IntegerType(), True),
    ]
)

fact_immigration = StructType(
    [
        StructField("cic_id", LongType(), True),
        StructField("year", IntegerType(), True),
        StructField("month", IntegerType(), True),
        StructField("city_code", StringType(), True),
        StructField("arrive_date", DateType(), True),
        StructField("departure_date", DateType(), True),
        StructField("mode", IntegerType(), True),
        StructField("visa", IntegerType(), True),
        StructField("immigration_id", IntegerType(), True),
        StructField("country", StringType(), True),
        StructField("state_code", StringType(), True),
    ]
)
