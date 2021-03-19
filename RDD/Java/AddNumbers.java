package org.modulo9.rdd;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.util.LongAccumulator;

import java.util.Arrays;
import java.util.List;

/**
 * Program that sums numbers using Spark RDD´s API
 */
public class AddNumbers
{
    public static void main( String[] args )
    {
        //Step 1. Create a SparkConf object
        SparkConf sparkConf = new SparkConf()
                .setAppName("Add number")
                .setMaster("local[4]");

        //Step 2. Create a Java Spark context
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);

        //Step 3. Create a list of integers
        Integer[] numbers = new Integer[]{1,2,3,4,5,6,7,8};
        List<Integer> integerList = Arrays.asList(numbers);

        /** Suma tradicional
        int sum= 0;
        for (Integer integer : integerList){
            sum = sum + integerList.get(integer);
        }
        */

        //Step 4. Create a JavaRDD
        JavaRDD<Integer> distributedList = sparkContext.parallelize(integerList);

        LongAccumulator counter = sparkContext.sc().longAccumulator();


        //Steo 5. Sum the numbers
        int sum = distributedList.reduce((number1, number2)-> {
            counter.add(1);

            return number1 + number2; });

        //Step 6. Print the sum

        System.out.println(counter);
        System.out.println("The sum is: " + sum);
        sparkContext.stop();

    }
}
