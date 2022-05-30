package com.dynatrace.spark.structuredstreaming;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQueryException;

import java.util.concurrent.TimeoutException;

public class WordCountStructuredStreaming {

    public static void main(String[] args) throws TimeoutException, StreamingQueryException {
        Logger.getLogger("org").setLevel(Level.ERROR);

        SparkConf conf = new SparkConf()
                .setAppName("WordCount")
                .setMaster("local[*]")
                .set("spark.driver.bindAddress", "127.0.0.1");

        SparkSession sparkSession = SparkSession.builder().config(conf).getOrCreate();

        // TODO

    }

}
