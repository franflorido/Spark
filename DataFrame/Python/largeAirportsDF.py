from pyspark.sql import SparkSession
import pyspark.sql.functions as f
from pyspark.sql.types import *

def main() -> None:

    spark_session = SparkSession \
        .builder \
        .master("local[8]") \
        .getOrCreate()

    logger = spark_session._jvm.org.apache.log4j
    logger.LogManager.getLogger("org").setLevel(logger.Level.WARN)

    #open airports data as dataframe
    data_frame = spark_session \
        .read \
        .options(header='true', inferschema='true') \
        .option("delimiter", ",") \
        .csv("data/airports.csv") \
        .persist()

    # open countries data as dataframe
    countries = spark_session \
        .read \
        .options(header='true', inferschema='true') \
        .option("delimiter", ",") \
        .csv("data/countries.csv") \
        .persist()

    data_frame.show()
    countries.show()

    #get the columns hat im interested in both dataframes
    data_frame = data_frame.select('type','iso_country')
    data_frame.show()
    countries = countries.select('name','code')
    countries.show()

    #get the number of large airports for each country
    data_frame = data_frame\
        .where(data_frame['type'] == "large_airport") \
        .groupBy('iso_country') \
        .count()

    #join datasets to get the final results
    final_dataset = data_frame \
        .join(countries, data_frame.iso_country == countries.code) \
        .select("name", "count") \
        .sort('count', ascending=False)


    final_dataset.show(10)


if __name__ == '__main__':
    main()
