package com.dynatrace.spark.streaming;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQuery;

import java.util.Arrays;
import java.util.concurrent.TimeoutException;

/**
 * Alternative solution to stream a number of text files using the Spark Structured Streaming API.
 */
public class WordCountStructuredStreaming {

    public static void main(String[] args) throws TimeoutException {
        Logger.getLogger("org").setLevel(Level.ERROR);

        SparkSession sparkSession = SparkSession
                .builder()
                .appName("WordCountStructuredStreaming")
                .master("local[*]")
                .config("spark.driver.bindAddress", "127.0.0.1")
                .getOrCreate();

        // create data source and process the data
        Dataset<Row> dataset = sparkSession.readStream()
                .option("maxFilesPerTrigger", 2) // maximum number of new files to be considered in every iteration
                .text("./data/testExample")
                .as(Encoders.STRING())
                .flatMap((FlatMapFunction<String, String>) s -> Arrays.asList(s.split(" ")).iterator(),
                        Encoders.STRING())
                .groupBy("value")
                .count();

        // define query and result output
        StreamingQuery query = dataset.writeStream()
                .outputMode(OutputMode.Complete())
                .queryName("count") // To identify the query, if multiple queries run at the same time
                .format("console")
                .start();

        query.processAllAvailable();
        query.stop();
    }
}
