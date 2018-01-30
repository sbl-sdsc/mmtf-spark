package edu.sdsc.mmtf.spark.mappers.demos;

import java.io.FileNotFoundException;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaSparkContext;

import edu.sdsc.mmtf.spark.filters.PolymerComposition;
import edu.sdsc.mmtf.spark.io.MmtfReader;
import edu.sdsc.mmtf.spark.mappers.StructureToPolymerChains;

/**
 * Example demonstrating how to extract protein chains from
 * PDB entries. This example uses a flatMapToPair function
 * to transform a structure to its polymer chains and
 * calculate polymer length statistics.
 * 
 * @author Peter Rose
 *
 */
public class PolyPeptideChainStatistics {

	public static void main(String[] args) throws FileNotFoundException {

		String path = MmtfReader.getMmtfReducedPath();
	    
	    SparkConf conf = new SparkConf().setMaster("local[*]").setAppName(PolyPeptideChainStatistics.class.getSimpleName());
	    JavaSparkContext sc = new JavaSparkContext(conf);
	    
	    JavaDoubleRDD chainLengths = MmtfReader
	    		.readSequenceFile(path, sc) // read MMTF hadoop sequence file
	    		.flatMapToPair(new StructureToPolymerChains(false, true))
	    		.filter(new PolymerComposition(PolymerComposition.AMINO_ACIDS_20))
	    		.mapToDouble(t -> t._2.getNumGroups())
	    		.cache();
	    
	    System.out.println("Total # chains: " + chainLengths.count());
	    System.out.println("Total # groups: " + chainLengths.sum());
	    System.out.println(" Min chain length: " + chainLengths.min());
	    System.out.println("Mean chain length: " + chainLengths.mean());
	    System.out.println(" Max chain length: " + chainLengths.max());
	    
	    sc.close();
	}
}
