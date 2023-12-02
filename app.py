from pyspark.sql import SparkSession
from pyspark.sql.functions import col, dayofmonth, year, count, month
from sqlalchemy import create_engine


#Created a function to create a spark session.
def setup_spark_session(app_name="EarthquakeAnalysis"):
    return SparkSession.builder.appName(app_name).config("spark.jars", "C:/Program Files/JDBC/mysql-connector-j-8.2.0.jar") \
    .config("spark.driver.extraClassPath", "C:/Program Files/JDBC/mysql-connector-j-8.2.0.jar") \
    .config("spark.executor.extraClassPath", "C:/Program Files/JDBC/mysql-connector-j-8.2.0.jar").getOrCreate()

#this function reads the csv availble on the provided location.
def read_csv(spark, csv_path, header=True, infer_schema=True):
    return spark.read.csv(csv_path, header=header, inferSchema=infer_schema)

#with this function we get the answers of 1st Question:
#How does the Day of a Week affect the number of earthquakes?
def analyze_day_of_week(earthquake_df):
    return (
        earthquake_df
        .withColumn("day_of_week", col("Date").cast("string").substr(1, 3))
        .groupBy("day_of_week")
        .agg(count("*").alias("count"))
        .orderBy("day_of_week")
        .fillna(0)
    )
#with this function we get the answers of 2st Question:
# ○ What is the relation between Day of the month and Number of earthquakes that
def analyze_day_of_month(earthquake_df):
    return (
        earthquake_df
        .withColumn("day_of_month", dayofmonth("Date"))
        .withColumn("month_year", year("Date"))
        .groupBy("day_of_month", "month_year")
        .agg(count("*").alias("count"))
        .orderBy("month_year", "day_of_month")
        .fillna(0)
    )

#with this function we get the answers of 3rd Question:
# ○ What is the relation between Day of the month and Number of earthquakes that happened in a year?
def analyze_average_monthly_frequency(earthquake_df):
    return (
    earthquake_df.groupBy("year", month("Date").alias("month"))
    .agg(count("*").alias("count"))
    .groupBy("year")
    .agg({"count": "avg"})
    .orderBy("year")
    .fillna(0)
    )

#with this function we get the answers of 4th Question:
# ○ What does the average frequency of earthquakes in a month from the year 1965 to 2016 tell us?
def analyze_yearly_counts(earthquake_df):
    return (
        earthquake_df.groupBy("year").count().orderBy("year").fillna(0)
    )

#with this function we get the answers of 5th Question:
# ○ How has the earthquake magnitude on average been varied over the years?
def analyze_average_magnitude_over_years(earthquake_df):
    return (
        earthquake_df
        .groupBy("year")
        .agg({"magnitude": "avg"})
        .orderBy("year")
        .fillna(0)
    )

#with this function we get the answers of 6th Question:
# ○ How does year impact the standard deviation of the earthquakes?
def analyze_std_deviation_by_year(earthquake_df):
    return (
        earthquake_df
        .groupBy("year")
        .agg({"magnitude": "stddev"})
        .orderBy("year")
        .fillna(0)
    )

#with this function we get the answers of 7th Question:
# ○ Does geographic location have anything to do with earthquakes?
def analyze_location_counts(earthquake_df):
    return (
        earthquake_df
        .groupBy("Latitude", "Longitude")
        .agg(count("*").alias("count"))
        .orderBy(col("count").desc())
        .fillna(0)
    )

#with this function we get the answers of 8th Question:
# ○ Where do earthquakes occur very frequently?
def analyze_high_frequency_locations(location_counts):
    return location_counts.filter(col("count") >= 100)



#with this function we get the answers of 9th Question:
# ○ What is the relation between Magnitude, Magnitude Type , Status and Root Mean Square of the earthquakes?
def analyze_magnitude_relation(earthquake_df):
    return (
        earthquake_df
        .groupBy("magnitude", "magnitude type", "status", "root mean square")
        .agg(count("*").alias("count"))
        .orderBy(col("count").desc())
        .fillna(0)
    )

#write_t_mysql function helps to insert the data into the mysql table
def write_to_mysql(earthquake_df, table_name, mysql_url, mysql_username, mysql_password, mode="overwrite"):
    earthquake_df.write.format("jdbc").option("url", mysql_url).option(
        "dbtable", table_name
    ).option("user", mysql_username).option("password", mysql_password).mode(mode).save()

#closing down the session.
def shutdown_spark_session(spark):
    spark.stop()

#main function to call all transformations and executing all functions.
def main():
    spark = setup_spark_session()

    csv_path = "path/to/csv"
    earthquake_df = read_csv(spark, csv_path)
    earthquake_df = earthquake_df.withColumn("year", year("Date"))
    earthquake_df = earthquake_df.withColumn("day_of_week", col("Date").cast("string").substr(1, 3))
    earthquake_df = earthquake_df.withColumn("day_of_month", dayofmonth("Date"))



    day_of_week_counts = analyze_day_of_week(earthquake_df)
    day_of_month_counts = analyze_day_of_month(earthquake_df)
    average_monthly_frequency = analyze_average_monthly_frequency(earthquake_df)
    yearly_counts = analyze_yearly_counts(earthquake_df)
    average_magnitude_over_years = analyze_average_magnitude_over_years(earthquake_df)
    std_deviation_by_year = analyze_std_deviation_by_year(earthquake_df)
    location_counts = analyze_location_counts(earthquake_df)
    high_frequency_locations = analyze_high_frequency_locations(location_counts)
    magnitude_relation = analyze_magnitude_relation(earthquake_df)

    # Writing to MySQL
    write_to_mysql(earthquake_df, "neic_earthquakes", "jdbc:mysql://mysql_host:mysql_port/mysql_database", "mysql_username", "mysql_password")

    # Showing results
    day_of_week_counts.show()
    day_of_month_counts.show()
    average_monthly_frequency.show()
    yearly_counts.show()
    average_magnitude_over_years.show()
    location_counts.show()
    high_frequency_locations.show()
    magnitude_relation.show()

    # Shutting down Spark session
    shutdown_spark_session(spark)

if __name__ == "__main__":
    main()
