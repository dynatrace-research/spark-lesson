package com.dynatrace.spark.wordcount;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.io.File;
import java.io.Serializable;
import java.util.Comparator;

public class WordCount {

    public static void main(String[] args) {
        Logger.getLogger("org").setLevel(Level.ERROR);
        SparkConf conf = new SparkConf()
                .setAppName("WordCount")
                .setMaster("local[*]")
                .set("spark.driver.bindAddress", "127.0.0.1");
        SparkSession sparkSession = SparkSession.builder().config(conf).getOrCreate();
        SparkContext sparkContext = sparkSession.sparkContext();

        File file = new File("./data/loremipsum");

        // TODO

    }

    static class TupleComparator implements Comparator<Tuple2<String, Integer>>, Serializable {
        public int compare(Tuple2<String, Integer> t1,
                           Tuple2<String, Integer> t2) {
            return t2._2.compareTo(t1._2);
        }
    }
}
