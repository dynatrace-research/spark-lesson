package com.dynatrace.spark.similarity;

import java.util.Arrays;

/**
 * contains methods to calculate the term frequency - inverse document frequency (tfidf) score
 */
public class TfIdf {

    private TfIdf() {
    }

    /**
     * @param term the word for which the score should be calculated
     * @param document the entire text of the document
     * @param numDocAll the number of all documents in the corpus
     * @param numDocT the number of documents containing the term at least once
     * @return the tfidf score of the given term within the given document as part of a corpus defined by the given values
     */
    public static double calculate(String term, String[] document, long numDocAll, long numDocT) {
        return tf(term, document) * idf(numDocAll, numDocT);
    }

    private static double tf(String term, String[] document) {
        long countT = Arrays.stream(document).filter(s -> s.equals(term)).count();
        return 1.0 * countT / document.length;
    }

    private static double idf(long numDocAll, long numDocT) {
        return Math.log(1.0 * numDocAll / numDocT);
    }

}
