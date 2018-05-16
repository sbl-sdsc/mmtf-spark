package edu.sdsc.mmtf.spark.utils;

import java.util.Arrays;

/**
 * Removes outliers from an array of values using Dixon's Q test.
 * 
 * Reference<p>
 * RB Dean, WJ Dixon (1951) Simplified Statistics for Small Numbers of Observations 
 * Anal. Chem. 23 (4), 636-638 <a href="http://dx.doi.org/10.1021/ac60052a025">DOI: 10.1021/ac60052a025</a>
 * 
 * <p>Wikipedia <a href="https://en.wikipedia.org/wiki/Dixon%27s_Q_test">Dixon's Q test</a>.
 * 
 * @author Peter Rose
 * @since 0.2.0
 *
 */
public class OutlierDetector {
    /*
     *  Limit values of the two-tailed Dixon's Q test at the 90, 95, and 99% confidence interval
     *  for 3 - 10 data points (see https://en.wikipedia.org/wiki/Dixon%27s_Q_test).
     */
    private static double[] q90 = {0.941, 0.765, 0.642, 0.560, 0.507, 0.468, 0.437, 0.412};
    private static double[] q95 = {0.970, 0.829, 0.710, 0.625, 0.568, 0.526, 0.493, 0.466};
    private static double[] q99 = {0.994, 0.926, 0.821, 0.740, 0.680, 0.634, 0.598, 0.568};
    
    /**
     * Returns index to largest outlier or -1 if no outlier is found.
     * 
     * @param values array of values
     * @param confidenceInterval CI threshold (90, 95, or 99%)
     * @return index of largest outlier or -1 if no outlier is found
     */
    public static int getLargestOutlier(double[] values, int confidenceInterval) {
        double qLimit = getQLimit(values, confidenceInterval);
        
        double[] sortedValues = values.clone();
        Arrays.sort(sortedValues);

        double range = sortedValues[sortedValues.length-1] - sortedValues[0];
        double q1 = (sortedValues[1] - sortedValues[0]) / range;
        double q2 = (sortedValues[sortedValues.length-1] - sortedValues[sortedValues.length-2]) / range;
        
        // return index to largest outlier or -1 if there is no outlier
        if (q1 > q2 && q1 > qLimit) {
            return getIndex(values, sortedValues[0]);
        } else if (q2 > q1 && q2 > qLimit) {
            return getIndex(values, sortedValues[sortedValues.length - 1]);
        } else {
            return -1;
        }
    }
    
    /**
     * Removes the largest outlier at the given confidence interval.
     * 
     * @param values array of values
     * @param confidenceInterval CI threshold (90, 95, or 99%)
     * @return subarray with outliers removed
     */
    public static double[] removeLargestOutliers(double[] values, int confidenceInterval) {
        double[] array = values.clone();
        int outlier = getLargestOutlier(array, confidenceInterval);
        
        if (outlier != -1) {
            double[] subArray = new double[array.length-1];
            for (int i = 0, n = 0; i < array.length; i++) {
                if (i != outlier) {
                    subArray[n++] = array[i];
                }
            }
            return subArray;
        }
        
        return values;
    }

    /**
     * Iteratively removes outliers until no more outliers are detected
     * at the given confidence interval.
     * @param values array of values
     * @param confidenceInterval CI threshold (90, 95, or 99%)
     * @return subarray with outliers removed
     */
    public static double[] removeAllOutliers(double[] values, int confidenceInterval) {
        double[] array = values.clone();
        int outlier = getLargestOutlier(array, confidenceInterval);
        
        while (outlier != -1) {
            double[] subArray = new double[array.length-1];
            for (int i = 0, n = 0; i < array.length; i++) {
                if (i != outlier) {
                    subArray[n++] = array[i];
                }
            }
            array = subArray;
            outlier = getLargestOutlier(array, confidenceInterval);
        }
        
        return array;
    }
    
    /**
     * Returns index of outlier in original array.
     * 
     * @param values original array
     * @param value for which to find the index of
     * @return index
     */
    private static int getIndex(double[] values, double value) {
        for (int i = 0; i < values.length; i++) {
            if (values[i] == value) {
                return i;
            }
        }
        return -1;
    }
    
    private static double getQLimit(double[] values, int confidenceInterval) {
        if (values.length > 10) {
            throw new IllegalArgumentException("ERROR: array must have less than 11 elements");
        }
        
        double qLimit = 1.0;
        if (confidenceInterval == 90) {
            qLimit = q90[values.length-3];
        } else if (confidenceInterval == 95) {
            qLimit = q95[values.length-3];
        } else if (confidenceInterval == 99) {
            qLimit = q99[values.length-3];
        } else {
            throw new IllegalArgumentException("ERROR: confidenceInterval must be: 90, 95, or 99");
        }
        
        return qLimit;
    }
}
