package edu.sdsc.mmtf.spark.demos;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import edu.sdsc.mmtf.spark.filters.Resolution;
import edu.sdsc.mmtf.spark.io.MmtfReader;

/**
 * Simple example of reading an MMTF Hadoop Sequence file, filtering the entries by resolution,
 * and counting the number of entries. This example shows how methods can be chained for a more
 * concise syntax.
 * 
 * @author Peter Rose
 *
 */
public class Demo1b {

	public static void main(String[] args) {

		String path = System.getProperty("MMTF_REDUCED_NEW");
	    if (path == null) {
	    	System.err.println("Environment variable for Hadoop sequence file has not been set");
	        System.exit(-1);
	    }
	    
	    SparkConf conf = new SparkConf().setMaster("local[*]").setAppName(Demo1b.class.getSimpleName());
	    JavaSparkContext sc = new JavaSparkContext(conf);
		 
	    // same as Demo1, but here the methods are chained together
	    long count = MmtfReader
	    		.readSequenceFile(path, sc)
	    		.filter(new Resolution(0.0, 2.0))
	    		.count();
	    
	    System.out.println("# structures: " + count);
	    
	    sc.close();
	}

}
