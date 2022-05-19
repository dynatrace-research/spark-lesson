package com.dynatrace.spark.streaming.text;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;

import java.util.Arrays;
import java.util.concurrent.TimeoutException;

public class StructuredStreamProcessing {

    public static void main(String[] args) throws TimeoutException, StreamingQueryException {
        Logger.getLogger("org").setLevel(Level.ERROR);

        SparkConf conf = new SparkConf()
                .setAppName("WordCount")
                .setMaster("local[*]")
                .set("spark.driver.bindAddress", "127.0.0.1");

        SparkSession sparkSession = SparkSession.builder().config(conf).getOrCreate();

        Dataset<Row> input = sparkSession
                .readStream()
                .format("socket")
                .option("host", "localhost")
                .option("port", "1234")
                .load();

        Dataset<String> words = input.as(Encoders.STRING())
                .flatMap((FlatMapFunction<String, String>) content -> Arrays.asList(content.split("[^a-zA-Z0-9]"))
                        .iterator(), Encoders.STRING())
                .filter(functions.col("value").notEqual(""));

        Dataset<Row> counts = words.groupBy("value").count();

        StreamingQuery streamingQuery = counts.writeStream()
                .outputMode("complete")
                .format("console")
                .start();

        streamingQuery.awaitTermination();
    }

}