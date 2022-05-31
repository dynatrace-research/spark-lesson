package com.dynatrace.spark.partitions;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;

public class DatasetPartitionExample {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("MyApp").setMaster("local[*]")
                .set("spark.driver.bindAddress", "127.0.0.1");
        SparkSession sparkSession = SparkSession.builder().config(conf).getOrCreate();

        // TODO
    }

}
