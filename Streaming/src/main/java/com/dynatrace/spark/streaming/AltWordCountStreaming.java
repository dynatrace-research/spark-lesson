package com.dynatrace.spark.streaming;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.sql.*;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQuery;

import java.io.File;
import java.net.URL;
import java.util.Arrays;
import java.util.concurrent.TimeoutException;


/**
 * Alternative solution to stream a number of text files using the DataSource v2 API by Spark.
 */
public class AltWordCountStreaming {

    public static void main(String[] args) throws TimeoutException {
        Logger.getLogger("org").setLevel(Level.ERROR);

        SparkConf conf = new SparkConf().setAppName("WordCount").setMaster("local[*]")
                .set("spark.driver.bindAddress", "127.0.0.1");
        SparkSession sparkSession = SparkSession.builder().config(conf).getOrCreate();
        File baseDir = new File("./data/simpleExample");

        // create data source
        Dataset<String> dataFrame = sparkSession.readStream()
                .option("maxFilesPerTrigger", 1) // maximum number of new files to be considered in every trigger
                .text(baseDir.getPath()) 
                .as(Encoders.STRING());

        // do word count as usual
        Dataset<Row> count = dataFrame
                .flatMap((FlatMapFunction<String, String>) s -> Arrays.asList(s.split(" ")).iterator(), Encoders.STRING())
                .groupBy(functions.col("value"))
                .count()
                .sort(functions.desc("count"));

        // define sink
        StreamingQuery query = count.writeStream()
                .outputMode(OutputMode.Complete()) // the rows in the streaming DataFrame will be written to the sink every time there are some updates
                .format("memory")  // We do not want to persist anything, for this
                .queryName("count") // To identify the query, as multiple queries can run at the same time
                .foreachBatch((VoidFunction2<Dataset<Row>, Long>) (batchDF, batchId) -> batchDF.show()) // Print each batch (i.e., here each file)
                .start();

        query.processAllAvailable();
        query.stop();
    }
}
