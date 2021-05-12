package com.dynatrace.spark.dataset;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.sql.*;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

public class DataSetExampleEncoder {

    public static void main(String[] args) {
        Logger.getLogger("org").setLevel(Level.ERROR);

        SparkConf conf = new SparkConf().setAppName("WordCount").setMaster("local[*]")
                .set("spark.driver.bindAddress", "127.0.0.1");
        SparkSession sparkSession = SparkSession.builder().config(conf).getOrCreate();

        Encoder<Record> encoder = Encoders.javaSerialization(Record.class);
        Dataset<Record> dataset = sparkSession.createDataset(rows(), encoder);

        dataset.filter((FilterFunction<Record>) wrapper -> wrapper.getC() == 11)
                .collect();
    }

    private static List<Record> rows() {
        return Arrays.asList(
                new Record("A1", "B1", 11),
                new Record("A1", "B1", 12),
                new Record("A3", "B3", 13)
        );
    }

    public static class Record implements Serializable {
        private static final long serialVersionUID = 1L;
        private String a;
        private String b;
        private int c;

        public Record(String a, String b, int c) {
            this.a = a;
            this.b = b;
            this.c = c;
        }

        public String getA() {
            return a;
        }

        public void setA(String a) {
            this.a = a;
        }

        public String getB() {
            return b;
        }

        public void setB(String b) {
            this.b = b;
        }

        public int getC() {
            return c;
        }

        public void setC(int c) {
            this.c = c;
        }


    }
}


