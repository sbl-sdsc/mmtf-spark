package edu.sdsc.mmtf.spark.filters.demos;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import edu.sdsc.mmtf.spark.filters.ContainsDnaChain;
import edu.sdsc.mmtf.spark.filters.ContainsLProteinChain;
import edu.sdsc.mmtf.spark.filters.NotFilter;
import edu.sdsc.mmtf.spark.io.MmtfReader;

/**
 * Example how to wrap a filter in a NotFilter to negate
 * a filter.
 * 
 * @author Peter Rose
 *
 */
public class NotFilterExample {

	public static void main(String[] args) {

		String path = System.getProperty("MMTF_REDUCED");
	    if (path == null) {
	    	    System.err.println("Environment variable for Hadoop sequence file has not been set");
	        System.exit(-1);
	    }
	    
	    SparkConf conf = new SparkConf().setMaster("local[*]").setAppName(NotFilterExample.class.getSimpleName());
	    JavaSparkContext sc = new JavaSparkContext(conf);
	    
	    long count = MmtfReader
	    		.readSequenceFile(path, sc) // read MMTF hadoop sequence file
	    		.filter(new ContainsLProteinChain()) // retain pdb entries that exclusively contain L-peptide chains
	    		// a NotFilter can be used to reverse a filter
	    		.filter(new NotFilter(new ContainsDnaChain())) // should not contain any DNA chains
	    		.count();
	    
	    System.out.println("# PDB entries without DNA chains: " + count);
	    sc.close();
	}
}
