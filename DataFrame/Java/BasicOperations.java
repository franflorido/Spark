package org.modulo9.dataframe;


import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.sum;

public class BasicOperations {


    public static <javaRDD> void main(String[] args){

        Logger.getLogger("org").setLevel(Level.OFF);
        SparkSession sparkSession =
                SparkSession.builder()
                .appName("Java Spark SQL basic example")
                .master("local[8]")
                .getOrCreate();

        Dataset<Row> dataFrame = sparkSession
                .read()
                .format("csv")
                .option("inferschema","true")
                .option("header","true")
                .load("data/simple.csv")
                .cache();

        //System.out.println(dataFrame);
        dataFrame.printSchema();
        dataFrame.show();
        System.out.println("Data types: " + dataFrame.dtypes());
        dataFrame.describe().show();
        dataFrame.explain();

        // select NAme list from dataset
        dataFrame.select("Name").show();

        //select columns name and age
        dataFrame.select("Name","age").show();

        //select column name and age and sum 2 to de age
        //dataFrame.select(col("Name"),col("age").plus(2)).show();

        //select the rows having a name length > 4
        dataFrame.select(functions.length(col("Name")).gt(4))
             .withColumnRenamed("startswith(Name, L)","L-Name")
             .show();

        //select names that start with L
        dataFrame.select(col("Name").startsWith("L")).show();

        //Add a new column name senior containing people with age bigger than 50
        dataFrame.withColumn("Seniors",col("age").gt(50)).show();

        //Rename HasACar as Owner
        dataFrame.withColumnRenamed("HasACar","Owner").show();

        //Remove a column
        dataFrame.drop("BirthDate").show();

        // Sort by Age
        dataFrame.sort(col("Age").desc()).show();

        //
        dataFrame.orderBy(col("Age").desc(),col("Weight")).show();

        // Get a RDD from a dataFrame
        JavaRDD<Row> rddFromDataframe = dataFrame.javaRDD();

        for (Row row : rddFromDataframe.collect() ){
            System.out.println(row);
        }

        // sum all the weigths (RDD)
        double sumOfWeights = rddFromDataframe
                .map(row->row.getDouble(2))
                .reduce((x,y) -> x+y);
        System.out.println("The sum of the weights is: " + sumOfWeights);

        //sum all weigths  in dtaframe
        dataFrame.select("Weight").groupBy().sum().show();
        dataFrame.select(sum("Weight")).show();
        dataFrame.agg(sum("Weight")).show();

        // Average of age (RDD)
        int sumOfAge = rddFromDataframe
                .map(row->row.getInt(1))
                .reduce((x,y) -> x+y);

        System.out.println("Average of age: " + sumOfAge/ (1.0 * rddFromDataframe.count()));

        // Average of age (DataFrame)
        dataFrame.select(functions.avg("Age"))
                .withColumnRenamed("Avg(Age)","Average")
                .show();

        //Write to CSV file
        dataFrame.write().csv("output.csv");

        //rite to json file
        dataFrame.write().json("output.json");

        sparkSession.stop();
    }


}
