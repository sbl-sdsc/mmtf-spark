package edu.sdsc.mmtf.spark.demos;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import edu.sdsc.mmtf.spark.filters.ContainsAlternativeLocations;
import edu.sdsc.mmtf.spark.io.MmtfReader;

/**
 * This example demonstrates how to read a list of PDB entries from a Hadoop Sequence File.
 * 
 * @author Peter Rose
 *
 */
public class Demo1f {

	public static void main(String[] args) {

	    if (args.length != 1) {
	        System.err.println("Usage: " + Demo1f.class.getSimpleName() + " <hadoop sequence file>");
	        System.exit(1);
	    }
	    
	    SparkConf conf = new SparkConf().setMaster("local[*]").setAppName(Demo1f.class.getSimpleName());
	    JavaSparkContext sc = new JavaSparkContext(conf);
	    
	    System.out.println("# structures with alternative locations: " +
	    MmtfReader
	    		.readSequenceFile(args[0], sc) // read a set of structure from an MMTF hadoop sequence file
	    		.filter(new ContainsAlternativeLocations())
	    		.count());

	    
	    sc.close();
	}

}
