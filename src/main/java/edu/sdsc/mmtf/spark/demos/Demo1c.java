package edu.sdsc.mmtf.spark.demos;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import edu.sdsc.mmtf.spark.filters.ExperimentalMethods;
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
	    		 // filter by experimental methods using joint Neutron/X-RAY diffraction
	    		.filter(new ExperimentalMethods(ExperimentalMethods.NEUTRON_DIFFRACTION, ExperimentalMethods.X_RAY_DIFFRACTION)) 
	    		.keys() // extract the keys (PDB IDs)
	    		.foreach(key -> System.out.println(key)); // print the keys (using a lambda expression)
	    
	    sc.close();
	}

}
