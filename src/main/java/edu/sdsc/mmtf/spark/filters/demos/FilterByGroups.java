package edu.sdsc.mmtf.spark.filters.demos;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import edu.sdsc.mmtf.spark.filters.ContainsGroup;
import edu.sdsc.mmtf.spark.io.MmtfReader;

/**
 * This example demonstrates how to filter structures with
 * specified groups (residues).
 * Groups are specified by their one, two, or three-letter codes,
 * e.g. "F", "MG", "ATP", as defined in the 
 * <a href="https://www.wwpdb.org/data/ccd">wwPDB Chemical Component Dictionary</a>.
 * 
 * @author Peter Rose
 *
 */
public class FilterByGroups {

	public static void main(String[] args) {

		String path = System.getProperty("MMTF_REDUCED");
	    if (path == null) {
	    	    System.err.println("Environment variable for Hadoop sequence file has not been set");
	        System.exit(-1);
	    }
	    
	    SparkConf conf = new SparkConf().setMaster("local[*]").setAppName(FilterByGroups.class.getSimpleName());
	    JavaSparkContext sc = new JavaSparkContext(conf);
		 
	    long count = MmtfReader
	    		.readSequenceFile(path, sc)
	    		.filter(new ContainsGroup("ATP","MG"))
	    		.count();
	    
	    System.out.println("Structures with ATP + MG: " + count);
	    
	    sc.close();
	}
}
