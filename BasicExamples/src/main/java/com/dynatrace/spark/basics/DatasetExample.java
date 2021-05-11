package com.dynatrace.spark.basics;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;

import java.util.Arrays;
import java.util.List;

public class DatasetExample {

    public static void main(String[] args) {
        Logger.getLogger("org").setLevel(Level.ERROR);

        SparkConf conf = new SparkConf().setAppName("MyApp").setMaster("local[*]");
        SparkSession sparkSession = SparkSession.builder().config(conf).getOrCreate();
        List<String> items = Arrays.asList("a", "b", "c", "d", "e");
        Dataset<String> dataset = sparkSession.createDataset(items, Encoders.STRING());
        dataset = dataset
                .map((MapFunction<String, String>) String::toUpperCase, Encoders.STRING())
                .filter("value != \"B\"");
        dataset.show();
    }

}
