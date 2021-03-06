package edu.sdsc.mmtf.spark.mappers.demos;

import java.io.FileNotFoundException;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import edu.sdsc.mmtf.spark.io.MmtfReader;
import edu.sdsc.mmtf.spark.mappers.StructureToBioJava;
import edu.sdsc.mmtf.spark.mappers.StructureToPolymerChains;

/**
 * Example demonstrating how to extract protein chains from
 * PDB entries and convert them to BioJava structure objects. 
 * 
 * @author Peter Rose
 *
 */
public class MapToBioJava {

	public static void main(String[] args) throws FileNotFoundException {

	    SparkConf conf = new SparkConf().setMaster("local[*]").setAppName(MapToBioJava.class.getSimpleName());
	    JavaSparkContext sc = new JavaSparkContext(conf);
	    
	    long count = MmtfReader
	    		.readReducedSequenceFile(sc) // read MMTF-Hadoop sequence file
	    		.flatMapToPair(new StructureToPolymerChains())
	    		.mapValues(new StructureToBioJava())
	    		.count();
	    
	    System.out.println("Number of polymer chains: " + count);
	    
	    sc.close();
	}
}
