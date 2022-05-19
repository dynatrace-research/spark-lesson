package com.dynatrace.spark.streaming.text;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;

public class BatchProcessing {

    public static void main(String[] args) throws InterruptedException {
        Logger.getLogger("org").setLevel(Level.ERROR);

        SparkConf conf = new SparkConf().setAppName("WordCount").setMaster("local[*]")
                .set("spark.driver.bindAddress", "127.0.0.1");

        JavaReceiverInputDStream<String> receiver;
        try (JavaStreamingContext sparkContext = new JavaStreamingContext(conf, Durations.seconds(10))) {
            receiver = sparkContext.socketTextStream("localhost", 1234);
            JavaDStream<String> a = receiver
                    .flatMap(s -> Arrays.asList(s.split("[^a-zA-Z0-9]")).iterator());

            JavaPairDStream<String, Integer> pairs = a.mapToPair(s -> new Tuple2<>(s, 1));
            JavaPairDStream<String, Integer> wordCounts = pairs.reduceByKey(Integer::sum);

            wordCounts.print();
            sparkContext.start();
            sparkContext.awaitTermination();
        }
    }

}
