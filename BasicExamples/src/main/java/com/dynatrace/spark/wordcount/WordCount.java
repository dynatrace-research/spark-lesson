package com.dynatrace.spark.wordcount;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;
import scala.Int;
import scala.Tuple2;

import java.io.File;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

public class WordCount {

    public static void main(String[] args) {
        Logger.getLogger("org").setLevel(Level.ERROR);
        SparkConf conf = new SparkConf().setAppName("WordCount").setMaster("local[*]")
                .set("spark.driver.bindAddress", "127.0.0.1");
        SparkSession sparkSession = SparkSession.builder().config(conf).getOrCreate();
        SparkContext sparkContext = sparkSession.sparkContext();

        File file = new File("./data/loremipsum");
        JavaRDD<String> textFile = sparkContext.textFile(file.getPath(), 1).toJavaRDD();
        JavaRDD<String> wordsFromFile = textFile.flatMap(content -> Arrays.asList(content.split(" ")).iterator());

        JavaPairRDD<String, Integer> javaPairRDD = wordsFromFile
                .mapToPair(t -> new Tuple2(t, 1))
                .reduceByKey((x, y) -> (int) x + (int) y);
        List<Tuple2<String, Integer>> ordered = javaPairRDD.takeOrdered(100, new TupleComparator());
        System.out.println(ordered.toString());
    }

    static class TupleComparator implements
            Comparator<Tuple2<String, Integer>>, Serializable {
        public int compare(Tuple2<String, Integer> t1,
                           Tuple2<String, Integer> t2) {
            return -t1._2.compareTo(t2._2);
        }
    }
}
