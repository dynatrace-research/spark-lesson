package com.dynatrace.spark.partitions;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.spark_partition_id;

public class DatasetPartitionExample {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("MyApp").setMaster("local[*]")
                .set("spark.driver.bindAddress", "127.0.0.1");
        SparkSession sparkSession = SparkSession.builder().config(conf).getOrCreate();

        Dataset<Long> numbers = sparkSession.range(0, 100);
        Dataset<Row> dataset = numbers
                .filter(col("id").mod(3).equalTo(0L))
                .withColumn("partitionId", spark_partition_id());
        dataset.show(100);
    }

}
