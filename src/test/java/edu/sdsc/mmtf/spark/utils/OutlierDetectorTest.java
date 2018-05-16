package edu.sdsc.mmtf.spark.utils;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import org.junit.Test;

public class OutlierDetectorTest {

    @Test
    public void test1() {
        double[] values = {0.189, 0.167, 0.187, 0.183, 0.186, 0.182, 0.181, 0.184, 0.181, 0.177};
 
        // test at 90% confidence interval
        int actual = OutlierDetector.getLargestOutlier(values, 90);
        int expected = 1; // 0.167 is the outlier
        assertEquals(expected, actual);
   
        // test at 95% confidence interval
        actual = OutlierDetector.getLargestOutlier(values, 95);
        expected = -1; // no outlier
        assertEquals(expected, actual);
        
        // test at 99% confidence interval
        actual = OutlierDetector.getLargestOutlier(values, 99);
        expected = -1; // no outlier
        assertEquals(expected, actual);
    }
    
    @Test
    public void test2() {
        double delta = 1.0E-8;
        double[] values = {0.189, 0.167, 0.187, 0.183, 0.186, 0.182, 0.181, 0.184, 0.181, 0.177};
 
        // test at 90% confidence interval
        double[] actuals = OutlierDetector.removeAllOutliers(values, 90);
        double[] expecteds = {0.189, 0.187, 0.183, 0.186, 0.182, 0.181, 0.184, 0.181, 0.177};
        assertArrayEquals(expecteds, actuals, delta);
   
        // test at 95% confidence interval
        actuals = OutlierDetector.removeAllOutliers(values, 95);
        // no outlier
        assertArrayEquals(values, actuals, delta);
       
        // test at 99% confidence interval
        actuals = OutlierDetector.removeAllOutliers(values, 99);
        // no outlier
        assertArrayEquals(values, actuals, delta);
    }
}
