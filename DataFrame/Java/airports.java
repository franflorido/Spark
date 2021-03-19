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

public class airports {
    public static <javaRDD> void main(String[] args){

        Logger.getLogger("org").setLevel(Level.OFF);
        SparkSession sparkSession =
                SparkSession.builder()
                        .appName("Analysis of aiport data")
                        .master("local[4]")
                        .getOrCreate();

        Dataset<Row> dataFrame = sparkSession
                .read()
                .format("csv")
                .option("inferschema","true")
                .option("header","true")
                .load("data/airports.csv")
                .cache();

        // First look at the schema
        dataFrame.printSchema();
        dataFrame.show();

        // SHow hoy many types of spanish airports there is

        dataFrame.groupBy("iso_country").count().sort(col("count").desc()).show(50);

        // Spanish airports
        dataFrame.filter(col("iso_country").equalTo("ES")).show();

        // Cout the type of airports in spain
        dataFrame.where(col("iso_country").equalTo("ES"))
                .groupBy("type")
                .count()
                .sort(col("count").desc())
                .show();

        //



        sparkSession.stop();
    }
}
