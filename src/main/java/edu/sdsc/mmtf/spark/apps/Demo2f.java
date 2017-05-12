package edu.sdsc.mmtf.spark.apps;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import edu.sdsc.mmtf.spark.filters.ContainsGroup;
import edu.sdsc.mmtf.spark.io.MmtfSequenceFileReader;

/**
 * This example demonstrates how to filter the PDB entries by a list of chemical components.
 * 
 * @author Peter Rose
 *
 */
public class Demo2f {

	public static void main(String[] args) {

	    if (args.length != 1) {
	        System.err.println("Usage: " + Demo2f.class.getSimpleName() + " <hadoop sequence file>");
	        System.exit(1);
	    }
	    
	    SparkConf conf = new SparkConf().setMaster("local[*]").setAppName(Demo2f.class.getSimpleName());
	    JavaSparkContext sc = new JavaSparkContext(conf);
	    
		 
	    long count = MmtfSequenceFileReader
	    	.read(args[0], sc) // read MMTF hadoop sequence file
	    	.filter(new ContainsGroup("ATP","MG"))
            .count();
	    
	    System.out.println("Structure with ATP + MG: " + count);
	    
	    sc.close();
	}

}
