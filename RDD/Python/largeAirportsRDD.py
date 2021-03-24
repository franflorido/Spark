from pyspark import SparkConf, SparkContext
from pyspark.sql.session import SparkSession
from pyspark.sql.types import Row
from pyspark.sql.functions import udf, concat, col, lit
import re


if __name__ == "__main__":

    spark_conf = SparkConf()\
        .setAppName("largeAirportsRDD")\
        .setMaster("local[8]")
    spark_context = SparkContext(conf=spark_conf)

    logger = spark_context._jvm.org.apache.log4j
    logger.LogManager.getLogger("org").setLevel(logger.Level.WARN)

    #import airport data
    splited_data = spark_context \
        .textFile("data/airports.csv") \
        .map(lambda line: line.split(','))

    #import countries data
    countries = spark_context \
        .textFile("data/countries.csv") \
        .map(lambda line: line.split(','))

    #get the num of large airports in each country
    output1 = splited_data \
        .map(lambda word: [word[i] for i in [2,8]]) \
        .filter(lambda word: word[0] == "\"large_airport\"") \
        .map(lambda word: (word[1], 1)) \
        .reduceByKey(lambda a, b: a + b) \
        .sortBy(lambda pair: pair[1], ascending=False)

    #get the fields i need in the countries data
    countries = countries \
        .map(lambda word: word[1:3]) \

    # convert the countries and airports data into dataframes to perform the join operation
    def f(x):
        d = {}
        for i in range(len(x)):
            d[str(i)] = x[i]
        return d

    spark = SparkSession(spark_context)
    hasattr(output1, "toDF")
    hasattr(countries,"toDF")

    # Now populate that
    output1_df = output1.map(lambda x: Row(**f(x))).toDF()
    countries = countries.map(lambda x: Row(**f(x))).toDF()
    output1_df.show()
    countries.show()

    #rename columns to deal with ambiguous names

    data_join = output1_df.withColumnRenamed("0", "iso_code")
    countries = countries\
        .withColumnRenamed("0","code") \
        .withColumnRenamed("1", "name")

    
    data_join.show()
    countries.show()

    #join datasets and get the final results
    final_dataset = data_join\
        .join(countries, data_join.iso_code == countries.code)\
        .withColumnRenamed("1", "count_airports") \
        .select("name", "count_airports") \
        .sort("count_airports",ascending=False)

    final_dataset.show(10)


    spark_context.stop()
