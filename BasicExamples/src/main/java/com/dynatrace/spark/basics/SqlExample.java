package com.dynatrace.spark.basics;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Arrays;
import java.util.List;

public class SqlExample {

    public static void main(String[] args) {
        Logger.getLogger("org").setLevel(Level.ERROR);

        SparkConf conf = new SparkConf().setAppName("MyApp").setMaster("local[*]");
        SparkSession sparkSession = SparkSession.builder().config(conf).getOrCreate();
        List<String> items = Arrays.asList("a", "b", "c", "d", "e");
        Dataset<String> dataset = sparkSession.createDataset(items, Encoders.STRING());
        dataset.createOrReplaceTempView("data");
        Dataset<Row> result = sparkSession.sql(
                "SELECT UPPER(value) as value FROM data WHERE UPPER(value) != \"B\"");
        result.show();
    }

}
