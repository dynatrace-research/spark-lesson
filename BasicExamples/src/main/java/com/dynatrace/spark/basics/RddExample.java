package com.dynatrace.spark.basics;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;

public class RddExample {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("MyApp").setMaster("local[*]")
                .set("spark.driver.bindAddress", "127.0.0.1");
        JavaSparkContext jContext = new JavaSparkContext(conf);
        List<String> items = Arrays.asList("a", "b", "c", "d", "e");
        JavaRDD<String> rdd = jContext.parallelize(items);
        rdd = rdd
                .map(String::toUpperCase)
                .filter(x -> !x.equals("B"));
        System.out.println("rdd definition done.");
        System.out.println(rdd.collect());
    }

}
