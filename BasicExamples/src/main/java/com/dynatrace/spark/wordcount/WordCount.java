package com.dynatrace.spark.wordcount;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.net.URL;
import java.util.Arrays;
import java.util.Map;

public class WordCount {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("WordCount").setMaster("local[*]");
        SparkSession sparkSession = SparkSession.builder().config(conf).getOrCreate();
        SparkContext sparkContext = sparkSession.sparkContext();

        URL resource = WordCount.class.getClassLoader().getResource("loremipsum");
        JavaRDD<String> textFile = sparkContext.textFile(resource.getPath(), 1).toJavaRDD();
    }
}
