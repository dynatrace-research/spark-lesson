package com.dynatrace.spark.similarity;

import java.util.List;

/**
 * allows to calculate the cosine similarity
 */
public class CosineSimilarity {

    private CosineSimilarity() {
    }

    /**
     * @param a first feature vector
     * @param b second feature vector
     * @return the cosine similarity between two feature vectors a and b
     */
    public static double calculate(List<Double> a, List<Double> b) {
        if (a.size() != b.size() || a.isEmpty()) {
            return 0;
        }

        // numerator
        double num = 0;

        // denominator
        double denomA = 0;
        double denomB = 0;

        for (int i = 0; i < a.size(); i++) {
            num += a.get(i) * b.get(i);
            denomA += a.get(i) * a.get(i);
            denomB += b.get(i) * b.get(i);
        }

        return num / (Math.sqrt(denomA) * Math.sqrt(denomB));
    }

}
