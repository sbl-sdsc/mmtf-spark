package edu.sdsc.mmtf.spark.filters.demos;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import edu.sdsc.mmtf.spark.filters.DepositionDate;
import edu.sdsc.mmtf.spark.io.MmtfReader;

/**
 * This example demonstrates how to filter structures with
 * specified depositionDate range
 * 
 * @author Yue Yu
 */
public class FilterByDepositionDate {

	public static void main(String[] args) {

		String path = System.getProperty("MMTF_REDUCED");
	    if (path == null) {
	    	    System.err.println("Environment variable for Hadoop sequence file has not been set");
	        System.exit(-1);
	    }
	    
	    SparkConf conf = new SparkConf().setMaster("local[*]").setAppName(FilterByDepositionDate.class.getSimpleName());
	    JavaSparkContext sc = new JavaSparkContext(conf);
		 
	    long count = MmtfReader
	    		.readSequenceFile(path, sc)
	    		.filter(new DepositionDate("2016-01-28","2017-02-28"))
	    		.count();
	    
	    System.out.println("Structures deposited between 2016-01-28 and 2017-02-28 " + count);
	    
	    sc.close();
	}
}
