from pyspark import SparkConf, SparkContext

if __name__ == "__main__":

    spark_conf = SparkConf()\
        .setAppName("TweetsRDD")\
        .setMaster("local[8]")
    spark_context = SparkContext(conf=spark_conf)

    logger = spark_context._jvm.org.apache.log4j
    logger.LogManager.getLogger("org").setLevel(logger.Level.WARN)

    def clean_words(x):
        commun_words = ['the','RT','in','to','at','a','on','-','and','for','of','rt','with','nd']
        lowercased_str = x.lower()
        for ch in commun_words:
            if len(lowercased_str) <= 4:
                lowercased_str = lowercased_str.replace(ch,'')
        return lowercased_str

    splited_data = spark_context \
        .textFile("data/tweets.tsv") \
        .map(lambda line: line.split('\t')) \

    output1 = splited_data \
        .map(lambda word: word[2]) \
        .flatMap(lambda line: line.split(" ")) \
        .map(lambda word: clean_words(word)) \
        .filter(lambda word: word != "") \
        .map(lambda word: (word, 1)) \
        .reduceByKey(lambda a, b: a + b) \
        .sortBy(lambda pair: pair[1], ascending=False)


    for line in output1:
        print(line)


    output2 = splited_data \
        .map(lambda word: word[1]) \
        .map(lambda word: (word, 1)) \
        .reduceByKey(lambda a, b: a + b) \
        .sortBy(lambda pair: pair[1], ascending=False) \
        .take(1)

    for (user,tweets) in output2:
        print("\n The user that has written the most tweets is: " + str(user) + " with a total of " + str(tweets) + " tweets" )

    output3 = splited_data \
        .map(lambda word: (word[1:4], len(word[2]))) \
        .sortBy(lambda pair: pair[-1], ascending=True) \
        .take(1)

    for info,len in output3:
        print("\n The name, Tweet and date of the shortest tweet is: " + str(info) + " And the length is: " + str(len))


    spark_context.stop()


