import os
import logging as log
import configparser
from pyspark.sql import SparkSession
from pyspark.sql.functions import monotonically_increasing_id

# my models
import wrangle

# setup log
logger = log.getLogger()
logger.setLevel(log.INFO)

# AWS configuration
config = configparser.ConfigParser()
config.read("configuration.cfg", encoding="utf-8-sig")

os.environ["AWS_ACCESS_KEY_ID"] = config["AWS"]["AWS_ACCESS_KEY_ID"]
os.environ["AWS_SECRET_ACCESS_KEY"] = config["AWS"]["AWS_SECRET_ACCESS_KEY"]

SOURCE = config["S3"]["SOURCE_S3_BUCKET"]
DESTINATION = config["S3"]["DEST_S3_BUCKET"]


# data processing functions
def create_spark_session():
    #     spark = (
    #         SparkSession.builder.config(
    #          "spark.jars.packages", "saurfang:spark-sas7bdat:2.0.0-s_2.11"
    #         )
    #         .enableHiveSupport()
    #         .getOrCreate()
    #     )
    spark = (
        SparkSession.builder.config(
            "spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0"
        )
        .enableHiveSupport()
        .getOrCreate()
    )
    return spark


def process_immigration_data(spark, input_data, output_data):
    """Process immigration data to create fact_immigration,  dim_immigration_airline tables and dim_immigration_personal

    Arguments:
        spark {object}: The SparkSession object.
        input_data {object}: The Source of S3 endpoint.
        output_data {object}: The Target of S3 endpoint.

    Returns:
        None
    """

    log.info("Start processing immigration dataset")

    # load immigration data file
    immigration_data_path = os.path.join(
        input_data + "/18-83510-I94-Data-2016/*.sas7bdat"
    )
    try:
        df = spark.read.format("com.github.saurfang.sas.spark").load(
            immigration_data_path
        )
    except:
        df = spark.read.parquet("sas_data")
    df = wrangle.drop_immigration_empty_columns(df)

    log.info("Start processing fact_immigration table")

    # extract columns to create fact_immigration table
    fact_immigration = (
        df.select(
            "cicid",
            "i94yr",
            "i94mon",
            "i94addr",
            "i94port",
            "depdate",
            "arrdate",
            "i94visa",
            "i94mode",
        )
        .distinct()
        .withColumn("immigration_id", monotonically_increasing_id())
    )

    # wrangling data to match data model
    fact_immigration = wrangle.fact_immigration(fact_immigration)

    # writing fact_immigration table to parquet files partitioned by state and city
    fact_immigration.write.mode("overwrite").partitionBy("state_code").parquet(
        path=output_data + "fact_immigration"
    )

    log.info("Start processing dim_immigration_personal")
    # extract columns to create dim_immigration_personal table
    dim_immigration_personal = (
        df.select("cicid", "i94cit", "i94res", "biryear", "gender")
        .distinct()
        .withColumn("immi_personal_id", monotonically_increasing_id())
    )

    # data wrangling to match data model
    dim_immigration_personal = wrangle.dim_immigration_personal(
        dim_immigration_personal
    )

    # writing dim_immigration_personal table to parquet files
    dim_immigration_personal.write.mode("overwrite").parquet(
        path=output_data + "dim_immigration_personal"
    )

    log.info("Start processing dim_immigration_airline")
    # extract columns to create dim_immigration_airline table
    dim_immigration_airline = (
        df.select("cicid", "airline", "admnum", "fltno", "visatype")
        .distinct()
        .withColumn("immi_airline_id", monotonically_increasing_id())
    )

    # data wrangling to match data model
    dim_immigration_airline = wrangle.dim_immigration_airline(dim_immigration_airline)

    # writing dim_immigration_airline table to parquet files
    dim_immigration_airline.write.mode("overwrite").parquet(
        path=output_data + "dim_immigration_airline"
    )


def process_label_descriptions(spark, input_data, output_data):
    """Parsing label description file to get country, city and state codes.

    Arguments:
            spark {object}: The SparkSession object.
            input_data {object}: The Source of S3 endpoint.
            output_data {object}: The Target of S3 endpoint.

    Returns:
        None
    """

    log.info("Start processing label descriptions")
    label_file_path = os.path.join(input_data + "/I94_SAS_Labels_Descriptions.SAS")

    with open(label_file_path) as file:
        contents = file.readlines()

    country_code = wrangle.country_code(contents)
    spark.createDataFrame(country_code.items(), ["code", "country"]).write.mode(
        "overwrite"
    ).parquet(path=output_data + "country_code")
    
    city_code = wrangle.city_code(contents)
    spark.createDataFrame(city_code.items(), ["code", "city"]).write.mode(
        "overwrite"
    ).parquet(path=output_data + "city_code")
    
    state_code = wrangle.state_code(contents)
    spark.createDataFrame(state_code.items(), ["code", "state"]).write.mode(
        "overwrite"
    ).parquet(path=output_data + "state_code")




def process_temperature_data(spark, input_data, output_data):
    """Process temperature data to create dim_temperature table.

    Arguments:
            spark {object}: The SparkSession object.
            input_data {object}: The Source of S3 endpoint.
            output_data {object}: The Target of S3 endpoint.

    Returns:
        None
    """

    log.info("Start processing dim_temperature table")
    # read temperature data file
    tempe_data_path = os.path.join(input_data + "/GlobalLandTemperaturesByCity.csv")
    df = spark.read.csv(tempe_data_path, header=True)
    df = df.where(df["Country"] == "United States")
    df = wrangle.drop_temperature_nulls_and_duplicates_rows(df)

    dim_temperature = df.select(
        ["dt", "AverageTemperature", "AverageTemperatureUncertainty", "City", "Country"]
    ).distinct()

    dim_temperature = wrangle.dim_temperature(dim_temperature)

    # writing dim_temperature table to parquet files.
    dim_temperature.write.mode("overwrite").parquet(
        path=output_data + "dim_temperature"
    )


def process_demography_data(spark, input_data, output_data):
    """Process demograpy data to create dim_demog_population and dim_demog_statistics tables.

    Arguments:
        spark {object}: The SparkSession object
        input_data {object}: The Source of S3 endpoint
        output_data {object}: The Target of S3 endpoint

    Returns:
        None
    """

    log.info("Start processing dim_demog_populaiton table.")

    # read demography data file
    demog_data_path = os.path.join(input_data + "/us-cities-demographics.csv")
    df = (
        spark.read.format("csv")
        .options(header=True, delimiter=";")
        .load(demog_data_path)
    )

    df = wrangle.drop_demographics_nulls_and_duplicates_rows(df)

    dim_demog_population = (
        df.select(
            [
                "City",
                "State",
                "Male Population",
                "Female Population",
                "Number of Veterans",
                "Foreign-born",
                "Race",
            ]
        )
        .distinct()
        .withColumn("demog_pop_id", monotonically_increasing_id())
    )

    dim_demog_population = wrangle.dim_demog_population(dim_demog_population)

    # writing dim_demog_population table to parquet files.
    dim_demog_population.write.mode("overwrite").parquet(
        path=output_data + "dim_demog_population"
    )

    log.info("Start processing dim_demog_statistics")
    dim_demog_statistics = (
        df.select(["City", "State", "Median Age", "Average Household Size"])
        .distinct()
        .withColumn("demog_stat_id", monotonically_increasing_id())
    )

    dim_demog_statistics = wrangle.dim_demog_statistics(dim_demog_statistics)

    # writing dim_demog_statistics table to parquet files.
    dim_demog_statistics.write.mode("overwrite").parquet(
        path=output_data + "dim_demog_statistics"
    )


def process_datasets(spark, input_data, output_data):
    process_immigration_data(spark, input_data, output_data)
    process_label_descriptions(spark, input_data, output_data)
    process_temperature_data(spark, input_data, output_data)
    process_demography_data(spark, input_data, output_data)


def process_datasets_local(spark):
    log.info("Processing datasets from local worksapce...")
    output_data = "output_data/"
    input_data = "../../data"
    process_immigration_data(spark, input_data, output_data)

    input_data = "../../data"
    process_label_descriptions(spark, input_data, output_data)

    input_data = "../../data2"
    process_temperature_data(spark, input_data, output_data)

    input_data = "sample_data"
    process_demography_data(spark, input_data, output_data)


def main():
    spark = create_spark_session()
    input_data = SOURCE
    output_data = DESTINATION

    # process_datasets(spark, input_data, output_data)
    process_datasets_local(spark)
    log.info("Data processing completed!")


if __name__ == "__main__":
    main()
