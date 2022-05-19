package com.dynatrace.spark.streaming;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.Queue;

public class StreamingExample {

    public static void main(String[] args) throws InterruptedException {
        Logger.getLogger("org").setLevel(Level.ERROR);

        // create the config and SparkContext
        SparkConf conf = new SparkConf().setAppName("StreamingExample").setMaster("local[*]")
                .set("spark.driver.bindAddress", "127.0.0.1");

        try (JavaStreamingContext context = new JavaStreamingContext(conf, Durations.seconds(5))) {
            // documents to be processed
            Queue<JavaRDD<String>> rdds = new LinkedList<>();
            rdds.add(context.sparkContext().parallelize(Arrays.asList("a", "b", "c")));
            rdds.add(context.sparkContext().parallelize(Arrays.asList("d", "e", "f")));
            rdds.add(context.sparkContext().parallelize(Arrays.asList("g", "h", "i")));

            // add the documents to the stream
            JavaDStream<String> inputStream = context.queueStream(rdds);
            // processing step
            inputStream.map(String::toUpperCase).print();

            // start the processing
            context.start();
            // don't stop the processing until the context terminates
            // Since we don't close the stream, the application will run forever.
            context.awaitTermination();
        }
    }

}
