package com.dynatrace.spark.similarity;

import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.io.File;
import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.stream.Collectors;

public class SimilarityMain {

    /**
     * logger to print intermediate results
     */
    private static final Logger LOG = LogManager.getLogger(SimilarityMain.class);

    /**
     * the directory from which the files for this experiment are used
     */
    private static final String PATH = "./data/sparkTexts";

    /**
     * stores for one term the number of documents that this term appears for
     */
    public static class TermDocumentCount implements Serializable {
        private final String term;
        private final long documentCount;

        public TermDocumentCount(String term, long documentCount) {
            this.term = term;
            this.documentCount = documentCount;
        }

        public String getTerm() {
            return term;
        }

        public long getDocumentCount() {
            return documentCount;
        }

        @Override
        public String toString() {
            return "TermDocumentCount{" +
                    "term='" + term + '\'' +
                    ", documentCount=" + documentCount +
                    '}';
        }
    }

    public static void main(String[] args) {
        // reduce Spark log output
        Logger.getLogger("org").setLevel(Level.ERROR);

        // create the SparkSession
        SparkConf conf = new SparkConf().setAppName("CosineSimilarity").setMaster("local[*]");
        SparkSession sparkSession = SparkSession.builder().config(conf).getOrCreate();

        // READ THE FILES USED FOR THE EXPERIMENT

        // get the file names
        String[] fileDir = Arrays.stream(Objects.requireNonNull(new File(PATH).listFiles()))
                .map(File::getAbsolutePath).toArray(String[]::new);
        JavaRDD<String> filePaths = sparkSession.createDataset(Arrays.asList(fileDir), Encoders.STRING()).toJavaRDD();
        // read and split the files
        JavaPairRDD<String, String[]> files = filePaths.mapToPair(
                o -> new Tuple2<>(new File(o).getName(),
                        Files.readString(Path.of(o)).toUpperCase(Locale.ROOT).split("[. \n]")));

        // GET THE TERM COUNTS

        // all terms from all documents
        List<String> terms = files.flatMap(x -> Arrays.asList(x._2).iterator()).distinct().collect();
        LOG.info("Terms: " + terms);
        // all terms and the number of documents that this term appears in
        List<TermDocumentCount> termCounts = terms.stream().map(t -> new TermDocumentCount(t,
                files.filter(e -> Arrays.asList(e._2).contains(t)).count())).collect(Collectors.toList());

        // FEATURE VECTORS

        // the file names and the feature vectors for these files
        JavaPairRDD<String, List<Double>> featureVectors =
                files.mapToPair(x -> new Tuple2<>(x._1, createFeatureVectors(x._2, termCounts, fileDir.length)));
        LOG.info("Feature Vectors: " + featureVectors.collect());
        Map<String, List<Double>> vectors = featureVectors.collect().stream()
                .collect(Collectors.toMap(x -> x._1, x -> x._2));

        // CALCULATE THE SIMILARITY AND PRINT THE RESULTS

        // (sorted) file names
        ArrayList<String> names = new ArrayList<>(files.map(x -> x._1).collect());
        Collections.sort(names);
        JavaRDD<String> nameRdd = sparkSession.createDataset(names, Encoders.STRING()).toJavaRDD();

        // first line of the result output
        System.out.println("\t\t" + String.join("\t", names));

        // calculate and print each line separately
        for (String s : names) {
            Map<String, Double> tempMap = nameRdd.mapToPair(
                    x -> new Tuple2<>(x, CosineSimilarity.calculate(vectors.get(s), vectors.get(x))))
                    .collect().stream().collect(Collectors.toMap(y -> y._1, y -> y._2));

            // %.3f -> print as float with 3 decimal digits
            System.out.println(s + names.stream().map(x -> "\t" + String.format("%.3f", tempMap.get(x)))
                    .reduce((a, b) -> a + b).orElse(""));
        }

    }

    private static List<Double> createFeatureVectors(String[] document, List<TermDocumentCount> termCounts, long documentCount) {
        return termCounts.stream()
                .map(t -> TfIdf.calculate(t.getTerm(), document, documentCount, t.getDocumentCount()))
                .collect(Collectors.toList());
    }

}
