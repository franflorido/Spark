import sys

from pyspark import SparkConf, SparkContext

if __name__ == "__main__":
    """
    if len(sys.argv) != 2:
        print("Usage: spark-submit wordcount <file>", file=sys.stderr)
        exit(-1)
    """
    spark_conf = SparkConf().setAppName("AirportType").setMaster("local[8]")
    spark_context = SparkContext(conf=spark_conf)

    logger = spark_context._jvm.org.apache.log4j
    logger.LogManager.getLogger("org").setLevel(logger.Level.WARN)
    output = spark_context \
        .textFile("data/airports.csv") \
        .map(lambda line: line.split(',')) \
        .filter(lambda word: word[8] == "\"ES\"") \
        .map(lambda word: word[2]) \
        .map(lambda word: (word, 1)) \
        .reduceByKey(lambda a, b: a + b) \
        .sortBy(lambda pair: pair[1], ascending=False)



    output.saveAsTextFile("data/output.txt")
    """
    for (word, count) in output:
        print("%s: %i" % (word, count))
    """
    spark_context.stop()


