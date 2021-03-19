package org.modulo9.rdd;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.io.FileNotFoundException;

public class CleanBridge {
    public static void main(String[] args) throws FileNotFoundException {
        //Step 1. Create a SparkConf objectnumbers.txt
        SparkConf sparkConf = new SparkConf()
                .setAppName("Airport Type")
                .setMaster("local[8]");
        //Step 2. Create a Java Spark context
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
        //Step 3. Read the files content
        JavaRDD<String> lines = sparkContext.textFile("data/Film_Locations_in_San_Francisco.csv");
        // Step 4. como es csv hay que hacer un split por ,
        JavaRDD<String[]> linesData = lines.map(line -> line.split(","));

        JavaRDD<String> tipeFiltered = linesData.map(array -> array[0]);

        // Step 5. en este punto ya tengo un array de palabras ahora las tengo que contar

        JavaPairRDD<String,Integer> pares = tipeFiltered.mapToPair(word -> new Tuple2<>(word,1));//primero pasamos las palabras a clave valor

        //Step 6. ahora contamos las palabras distintas

        JavaPairRDD<String,Integer> groupedPairs = pares.reduceByKey((integer1,integer2)-> integer1+integer2);


        JavaPairRDD<Integer, String> reversePairs = groupedPairs.mapToPair(pair -> new Tuple2<>(pair._2(), pair._1()));

        JavaPairRDD<Integer, String> filtered = reversePairs.filter(pair -> pair._1() >= 20);

        filtered.sortByKey(false).saveAsTextFile("data/output_two.txt");

        /*
        for(Tuple2<?, ?> tuple : output){

            System.out.println(tuple._1() + ": " + tuple._2());
        }
        */

        //Step 7. Stop Spark context
        sparkContext.stop();
    }
}
