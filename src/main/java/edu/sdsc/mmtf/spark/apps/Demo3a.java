package edu.sdsc.mmtf.spark.apps;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import edu.sdsc.mmtf.spark.filters.ContainsGroup;
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
public class Demo3a {

	public static void main(String[] args) {

	    if (args.length != 1) {
	        System.err.println("Usage: " + Demo3a.class.getSimpleName() + " <hadoop sequence file>");
	        System.exit(1);
	    }
	    
	    SparkConf conf = new SparkConf().setMaster("local[*]").setAppName(Demo3a.class.getSimpleName());
	    JavaSparkContext sc = new JavaSparkContext(conf);
		 
	    long count = MmtfReader
	    	.readSequenceFile(args[0], sc) // read MMTF hadoop sequence file
	    	.filter(new ContainsGroup("ATP","MG"))
            .count();
	    
	    System.out.println("Structure with ATP + MG: " + count);
	    
	    sc.close();
	}

}
