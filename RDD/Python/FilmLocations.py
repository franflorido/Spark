import sys

from pyspark import SparkConf, SparkContext


if __name__ == "__main__":

    spark_conf = SparkConf().setAppName("AirportType").setMaster("local[8]")
    spark_context = SparkContext(conf=spark_conf)

    logger = spark_context._jvm.org.apache.log4j
    logger.LogManager.getLogger("org").setLevel(logger.Level.WARN)
    tuples = spark_context \
        .textFile("data/Film_Locations_in_San_Francisco.csv") \
        .map(lambda line: line.split(',')) \
        .map(lambda word: word[0]) \
        .map(lambda word: (word, 1)) \

    peliculas_ordenadas = tuples \
        .reduceByKey(lambda a, b: a + b) \

    output = peliculas_ordenadas \
        .filter(lambda pair: pair[1] >= 20) \
        .map(lambda pair: (pair[1],pair[0])) \
        .sortBy(lambda pair: pair[0], ascending=False) \

    numero_localizaciones = tuples.count()

    numero_peliculas = peliculas_ordenadas.count()

    media = numero_localizaciones/numero_peliculas

    output1 = output.collect()

    with open("data/output_two.txt", 'w') as output:
        for (word,count) in output1:
            output.write(str(word) + ": " + str(count) + "\n")

        output.write("Total number of films: " + str(numero_peliculas) + "\n")
        output.write("The average of film locations per film: " + str(media))
        output.close()

    spark_context.stop()


