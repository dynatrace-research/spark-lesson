package com.dynatrace.spark.streaming;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;

public class WordCountStreaming {

    public static void main(String[] args) throws InterruptedException {
        Logger.getLogger("org").setLevel(Level.ERROR);

        // create the config and SparkContext
        SparkConf conf = new SparkConf().setAppName("StreamingExample").setMaster("local[*]")
                .set("spark.driver.bindAddress", "127.0.0.1");

        // 5s time interval for batches
        try (JavaStreamingContext context = new JavaStreamingContext(conf, new Duration(5_000))) {
            JavaReceiverInputDStream<String> inputStream =
                    context.socketTextStream("localhost", 1234);
            JavaDStream<String> words = inputStream
                    .flatMap(s -> Arrays.asList(s.split("[^a-zA-Z0-9]")).iterator())
                    .filter(word -> word.length() > 0);
            words.mapToPair(s -> new Tuple2<>(s, 1))
                    .reduceByKey(Integer::sum)
                    .print();

            context.start();
            context.awaitTermination();
        }
    }

}
