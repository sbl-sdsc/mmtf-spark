package edu.sdsc.mmtf.spark.mappers.demos;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaSparkContext;

import edu.sdsc.mmtf.spark.filters.PolymerComposition;
import edu.sdsc.mmtf.spark.io.MmtfReader;
import edu.sdsc.mmtf.spark.mappers.StructureToBioJava;
import edu.sdsc.mmtf.spark.mappers.StructureToPolymerChains;

/**
 * Example demonstrating how to extract protein chains from
 * PDB entries. This example uses a flatMapToPair function
 * to transform a structure to its polymer chains.
 * 
 * @author Peter Rose
 *
 */
public class MapToBioJava {

	public static void main(String[] args) {

		String path = System.getProperty("MMTF_REDUCED");
	    if (path == null) {
	    	    System.err.println("Environment variable for Hadoop sequence file has not been set");
	        System.exit(-1);
	    }
	    
	    SparkConf conf = new SparkConf().setMaster("local[*]").setAppName(MapToBioJava.class.getSimpleName());
	    JavaSparkContext sc = new JavaSparkContext(conf);
	    
	    long count = MmtfReader
	    		.readSequenceFile(path, sc) // read MMTF hadoop sequence file
	    		.flatMapToPair(new StructureToPolymerChains())
	    		.mapValues(new StructureToBioJava())
	    		.count();
	    
	    System.out.println("# structures: " + count);
	    
	    sc.close();
	}
}
