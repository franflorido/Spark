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

    data_frame = spark_session \
        .read \
        .options(header='false', inferschema='true') \
        .option("delimiter", "\t") \
        .csv("data/tweets.tsv") \
        .persist()

    data_frame.show()

    #Drop empty columns
    dataframe_splited = data_frame.drop('_c4','_c5')
    dataframe_splited.show()

    #Renaming columns in the datset
    data = dataframe_splited\
        .withColumnRenamed("_c1", "name") \
        .withColumnRenamed("_c2", "Tweet") \
        .withColumnRenamed("_c3", "date") \
        .withColumnRenamed("_c6", "Location")

    data.show()

    commun_words = ['the','rt','in','to','at', 'a', '-', 'and', 'for', 'of', 'on','RT','with']

    print("Most repeated words and times they appear \n")
    # Show the 10 most repeated words not in commun words
    output = data.withColumn('word',f.explode(f.split(f.col('Tweet'),' '))) \
        .withColumn('word', f.lower(f.col('word'))) \
        .filter(~f.col('word').isin(commun_words)) \
        .groupBy('word') \
        .count() \
        .sort('count',ascending=False)

    output.show(10)

    print("The person who wrote the most tweets and the number of tweets \n")

    #show the person who wrote the most tweets and the number of tweets
    output2 = data \
        .groupBy('name') \
        .count() \
        .sort('count', ascending=False) \
        .show(1)

    #show the person, date and length of teh shortests tweet

    output3 = data \
        .withColumn("len",f.length(data["Tweet"])) \
        .select('name','date','len') \
        .sort('len',ascending=True) \
        .take(1)

    for (name,date,len) in output3:
        print("The name and date of the shortest tweet is: " + str(name) +" ," +str(date)+ " And the length is: " + str(len))


if __name__ == '__main__':
    main()
