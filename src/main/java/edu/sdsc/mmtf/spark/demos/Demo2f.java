package edu.sdsc.mmtf.spark.demos;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import edu.sdsc.mmtf.spark.filters.PolymerComposition;
import edu.sdsc.mmtf.spark.io.MmtfReader;
import edu.sdsc.mmtf.spark.mappers.StructureToPolymerChains;

/**
 * This example demonstrates how to filter the PDB entries by a list of chemical components.
 * 
 * @author Peter Rose
 *
 */
public class Demo2f {

	public static void main(String[] args) {

		String path = System.getProperty("MMTF_REDUCED");
	    if (path == null) {
	    	    System.err.println("Environment variable for Hadoop sequence file has not been set");
	        System.exit(-1);
	    }
	    
	    SparkConf conf = new SparkConf().setMaster("local[*]").setAppName(Demo2f.class.getSimpleName());
	    JavaSparkContext sc = new JavaSparkContext(conf);
	    
	    long count = MmtfReader
	    		.readSequenceFile(path, sc) // read MMTF hadoop sequence file
	    		.flatMapToPair(new StructureToPolymerChains(false, true))
	    		.filter(new PolymerComposition(PolymerComposition.AMINO_ACIDS_20))
	    		.count();
	    
	    System.out.println("Chains with standard amino acids: " + count); //329692
	    
	    sc.close();
	}
}
