package org.modulo9.rdd;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class AddNumbersFromFileCompact {
    public static void main(String[] args) {
        //Step 1. Create a SparkConf objectnumbers.txt
        SparkConf sparkConf = new SparkConf()
                .setAppName("Add number")
                .setMaster("local[4]");

        //Step 2. Create a Java Spark context
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
        //Step5. Sum the numbers
        int sum =sparkContext.textFile("data")
                .map(line -> Integer.valueOf(line))
                .reduce((number1, number2)-> number1 + number2);

        System.out.println("The sum is: " + sum);
        //Step 7. Stop Spark context
        sparkContext.stop();

    }
}
