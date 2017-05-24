package edu.sdsc.mmtf.spark.apps;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import edu.sdsc.mmtf.spark.filters.ExperimentalMethodsFilter;
import edu.sdsc.mmtf.spark.io.MmtfReader;

/**
 * This example demonstrates how to filter the PDB by polymer chain type. It filters
 * 
 * Simple example of reading an MMTF Hadoop Sequence file, filtering the entries by resolution,
 * and counting the number of entries. This example shows how methods can be chained for a more
 * concise syntax.
 * 
 * @author Peter Rose
 *
 */
public class Demo1c {

	public static void main(String[] args) {

	    if (args.length != 1) {
	        System.err.println("Usage: " + Demo1c.class.getSimpleName() + " <hadoop sequence file>");
	        System.exit(1);
	    }
	    
	    SparkConf conf = new SparkConf().setMaster("local[*]").setAppName(Demo1c.class.getSimpleName());
	    JavaSparkContext sc = new JavaSparkContext(conf);
		 
	    MmtfReader
	    		.readSequenceFile(args[0], sc) // read MMTF hadoop sequence file
	    		 // filter by experimental method using joint X-RAY and NMR refinement
	    		.filter(new ExperimentalMethodsFilter("X-RAY DIFFRACTION", "SOLUTION NMR")) // filter by experimental method
	    		.keys() // extract the keys (PDB IDs)
	    		.foreach(key -> System.out.println(key)); // print the keys (using a lambda expression)
	    
	    sc.close();
	}

}
