package com.dynatrace.spark.basics;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;

public class RddExample {

    public static void main(String[] args) {
        Logger.getLogger("org").setLevel(Level.ERROR);

        SparkConf conf = new SparkConf().setAppName("MyApp").setMaster("local[*]");
        JavaSparkContext jContext = new JavaSparkContext(conf);
        List<String> items = Arrays.asList("a", "b", "c", "d", "e");
        JavaRDD<String> rdd = jContext.parallelize(items);
        rdd = rdd
                .map(String::toUpperCase)
                .filter(x -> !x.equals("B"));
        System.out.println(rdd.collect());
    }

}
