package org.modulo9.rdd;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class AddNumbersFromFile {
    public static void main(String[] args) {
        //Step 1. Create a SparkConf objectnumbers.txt
        SparkConf sparkConf = new SparkConf()
                .setAppName("Add number")
                .setMaster("local[8]");

        //Step 2. Create a Java Spark context
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
        //Step 3. Read the files content

        long startTime = System.currentTimeMillis();
        JavaRDD<String> lines = sparkContext.textFile("data/manyNumbers.txt");
        //Step 4. Get a RDD of integers
        JavaRDD<Double> numbers = lines.map(line -> Double.valueOf(line));
        //Step5. Sum the numbers
        double sum =numbers.reduce((number1, number2)-> number1 + number2);

        long endTime = System.currentTimeMillis();
        long time = (endTime - startTime)/1000;
        System.out.println("The time it took to compile is: " + time);

        System.out.println("The sum is: " + sum);
        //Step 7. Stop Spark context
        sparkContext.stop();

    }
}
