package edu.sdsc.mmtf.spark.mappers.demos;

import java.io.FileNotFoundException;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import edu.sdsc.mmtf.spark.filters.PolymerComposition;
import edu.sdsc.mmtf.spark.io.MmtfReader;
import edu.sdsc.mmtf.spark.mappers.StructureToPolymerChains;

/**
 * Example demonstrating how to extract protein chains from
 * PDB entries. This example uses a flatMapToPair function
 * to transform a structure to its polymer chains.
 * 
 * @author Peter Rose
 * @since 0.1.0
 *
 */
public class FlatMapChains {

	public static void main(String[] args) throws FileNotFoundException {

		String path = MmtfReader.getMmtfReducedPath();
	    
	    SparkConf conf = new SparkConf().setMaster("local[*]").setAppName(FlatMapChains.class.getSimpleName());
	    JavaSparkContext sc = new JavaSparkContext(conf);
	    
	    long count = MmtfReader
	    		.readSequenceFile(path, sc) // read MMTF hadoop sequence file
	    		.flatMapToPair(new StructureToPolymerChains(false, true))
	    		.filter(new PolymerComposition(PolymerComposition.AMINO_ACIDS_20))
	    		.count();
	    
	    System.out.println("Chains with standard amino acids: " + count);
	    
	    sc.close();
	}
}
