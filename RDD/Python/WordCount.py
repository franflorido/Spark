import sys
from pyspark import SparkConf, SparkContext

if __name__ == "__main__":
    """
    if len(sys.argv) != 2:
        print("Usage: spark-submit wordcount <file>", file="data/quijote.txt")
        exit(-1)
    """
    spark_conf = SparkConf().setAppName("WordCount").setMaster("local[2]")
    spark_context = SparkContext(conf=spark_conf)

    logger = spark_context._jvm.org.apache.log4j
    logger.LogManager.getLogger("org").setLevel(logger.Level.WARN)

    lines = spark_context.textFile("data/quijote.txt")

    words = lines.flatMap(lambda line: line.split(" "))

    pairs = words.map(lambda word: (word,1))

    grouped_pairs = pairs.reduceByKey(lambda a,b : a+b)

    ordered_pairs = grouped_pairs.sortBy(lambda pair: pair[1],ascending=False)

    result = ordered_pairs.take(20)

    # Para ordenarlo puedo hacer lo que viene comentado
    """    
    output = spark_context \
        .textFile(sys.argv[1]) \
        .flatMap(lambda line: line.split(' ')) \
        .map(lambda word: (word, 1)) \
        .reduceByKey(lambda a, b: a + b) \
        .sortBy(lambda pair: pair[1], ascending=False) \
        .take(20)
    """
    for (word, count) in result:
        print("%s: %i" % (word, count))

    spark_context.stop()
