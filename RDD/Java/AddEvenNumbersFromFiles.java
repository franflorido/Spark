package org.modulo9.rdd;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class AddEvenNumbersFromFiles {
    public static void main(String[] args) {
        //Step 1. Create a SparkConf objectnumbers.txt
        SparkConf sparkConf = new SparkConf()
                .setAppName("Add number")
                .setMaster("local[4]");
        //Step 2. Create a Java Spark context
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
        //Step 3. Read the files content
        JavaRDD<String> lines = sparkContext.textFile("data/manyNumbers.txt");
        //Step 4. Get a RDD of integers
        JavaRDD<Double> numbers = lines.map(line -> Double.valueOf(line));
        //Step5. Sum the numbers
        JavaRDD<Double> evenNumbers = numbers.filter((number) -> number % 2 == 0);
        double sum = evenNumbers.reduce((a,b)->a+b);
        System.out.println("The sum is: " + sum);
        //Step 7. Stop Spark context
        sparkContext.stop();
    }
}
