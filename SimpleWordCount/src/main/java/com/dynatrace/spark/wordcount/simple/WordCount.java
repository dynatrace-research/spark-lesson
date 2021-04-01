package com.dynatrace.spark.wordcount.simple;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;

public class WordCount {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("WordCount").setMaster("local[*]");
        SparkSession sparkSession = SparkSession.builder().config(conf).getOrCreate();
    }
}
