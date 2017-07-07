package edu.sdsc.mmtf.spark.filters.demos;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import edu.sdsc.mmtf.spark.filters.ReleaseDate;
import edu.sdsc.mmtf.spark.io.MmtfReader;

/**
 * This example demonstrates how to filter structures with
 * specified releaseDate range
 * 
 * @author Yue Yu
 */
public class FilterByReleaseDate {

	public static void main(String[] args) {

		String path = System.getProperty("MMTF_REDUCED");
	    if (path == null) {
	    	    System.err.println("Environment variable for Hadoop sequence file has not been set");
	        System.exit(-1);
	    }
	    
	    SparkConf conf = new SparkConf().setMaster("local[*]").setAppName(FilterByReleaseDate.class.getSimpleName());
	    JavaSparkContext sc = new JavaSparkContext(conf);
		 
	    long count = MmtfReader
	    		.readSequenceFile(path, sc)
	    		.filter(new ReleaseDate("2000-01-28","2017-02-28"))
	    		.count();
	    
	    System.out.println("Structures released between 2000-01-28 and 2017-02-28 " + count);
	    
	    sc.close();
	}
}
