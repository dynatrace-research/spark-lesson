package com.dynatrace.spark.wordcount;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;

import java.net.URL;
import java.util.concurrent.TimeoutException;

public class StreamingWordCount {

    public static void main(String[] args) throws TimeoutException {
        SparkConf conf = new SparkConf().setAppName("WordCount").setMaster("local[*]");
        SparkSession sparkSession = SparkSession.builder().config(conf).getOrCreate();
        URL resource = WordCount.class.getClassLoader().getResource("stream");

    }
}
