package edu.sdsc.mmtf.spark.mappers.demos;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.rcsb.mmtf.api.StructureDataInterface;

import edu.sdsc.mmtf.spark.datasets.demos.CustomReportDemo;
import edu.sdsc.mmtf.spark.filters.PolymerComposition;
import edu.sdsc.mmtf.spark.io.MmtfReader;
import edu.sdsc.mmtf.spark.mappers.StructureToBioJava;
import edu.sdsc.mmtf.spark.mappers.StructureToBioassembly;
import edu.sdsc.mmtf.spark.mappers.StructureToPolymerChains;

/**
 * Example demonstrating how to extract protein chains from
 * PDB entries. This example uses a flatMapToPair function
 * to transform a structure to its polymer chains.
 * 
 * @author Peter Rose
 *
 */
public class MapToBioAssembly {

	public static void main(String[] args) {

	    SparkConf conf = new SparkConf().setMaster("local[*]").setAppName(CustomReportDemo.class.getSimpleName());
	    JavaSparkContext sc = new JavaSparkContext(conf);
	    List<String> pdbIds = Arrays.asList("1HV4"); // single protein chain
	    JavaPairRDD<String, StructureDataInterface> pdb = MmtfReader.downloadMmtfFiles(pdbIds, sc).cache(); 
	    long count = pdb // read MMTF hadoop sequence file
	    		.flatMapToPair(new StructureToBioassembly())
	    		.count();
	    
	    System.out.println("# structures: " + count);
	    
	    sc.close();
	}
}
