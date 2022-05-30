package com.dynatrace.spark.wordcount;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;

import java.io.File;
import java.util.Arrays;

public class DocumentCount {

    public static void main(String[] args) {
        Logger.getLogger("org").setLevel(Level.ERROR);
        SparkConf conf = new SparkConf()
                .setAppName("WordCount")
                .setMaster("local[*]")
                .set("spark.driver.bindAddress", "127.0.0.1");
        SparkSession sparkSession = SparkSession.builder().config(conf).getOrCreate();
        SparkContext sparkContext = sparkSession.sparkContext();

        File file = new File("./data/loremipsum");
        JavaRDD<String> textFile = sparkContext.textFile(file.getPath(), 1).toJavaRDD();
        long count = textFile
                .flatMap(s -> Arrays.asList(s.split(" ")).iterator())
                .count();
        System.out.println(count);
    }

}
