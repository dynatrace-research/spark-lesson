package com.dynatrace.spark.dataset;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.Arrays;
import java.util.List;

public class DataFrameExample {

    public static void main(String[] args) {
        Logger.getLogger("org").setLevel(Level.ERROR);

        SparkConf conf = new SparkConf().setAppName("DataFrameExample").setMaster("local[*]")
                .set("spark.driver.bindAddress", "127.0.0.1");
        SparkSession sparkSession = SparkSession.builder().config(conf).getOrCreate();

        Dataset<Row> dataFrame = sparkSession.createDataFrame(rows(), schema());
        Dataset<Row> sumPerCategory = dataFrame.groupBy("a", "b").sum("c");

        sumPerCategory.show();
    }

    private static List<Row> rows() {
        return Arrays.asList(
                RowFactory.create("A1", "B1", 11),
                RowFactory.create("A1", "B1", 12),
                RowFactory.create("A3", "B3", 13)
        );
    }

    private static StructType schema() {
        return DataTypes.createStructType(new StructField[]{
                new StructField("a", DataTypes.StringType, false, Metadata.empty()),
                new StructField("b", DataTypes.StringType, false, Metadata.empty()),
                new StructField("c", DataTypes.IntegerType, true, Metadata.empty()),
        });
    }
}


