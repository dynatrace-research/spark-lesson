package com.dynatrace.spark.streaming;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.io.File;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.Objects;
import java.util.Queue;

public class WordCountStreaming {

    public static void main(String[] args) throws InterruptedException {
        Logger.getLogger("org").setLevel(Level.ERROR);

        // create the config and SparkContext
        SparkConf conf = new SparkConf().setAppName("StreamingExample").setMaster("local[*]")
                .set("spark.driver.bindAddress", "127.0.0.1");
        // 5s time interval for batches
        try (JavaStreamingContext context = new JavaStreamingContext(conf, new Duration(5_000))) {

            // get all files names
            File baseDir = new File("./data/testExample");
            String[] fileDir = Arrays.stream(Objects.requireNonNull(baseDir.listFiles()))
                    .map(File::getPath).toArray(String[]::new);

            // define RDDs with the file content (split into individual words)
            Queue<JavaRDD<String>> rdds = new LinkedList<>();
            for (String file : fileDir) {
                JavaRDD<String> javaRdd = context.sparkContext()
                        .textFile(file)
                        .flatMap(s -> Arrays.asList(s.split("[^a-zA-Z0-9]")).iterator());
                rdds.add(javaRdd);
            }
            // add the RDDs as batches to the stream
            JavaDStream<String> inputStream = context.queueStream(rdds);
            inputStream
                    .filter(x -> x.length() > 0)
                    .mapToPair(s -> new Tuple2<>(s, 1))
                    .reduceByKey(Integer::sum)
                    .print(20);

            context.start();
            context.awaitTermination();
        }
    }

}
