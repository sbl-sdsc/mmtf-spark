package edu.sdsc.mmtf.spark.mappers.demos;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.rcsb.mmtf.api.StructureDataInterface;

import edu.sdsc.mmtf.spark.datasets.demos.CustomReportDemo;
import edu.sdsc.mmtf.spark.io.MmtfReader;
import edu.sdsc.mmtf.spark.io.MmtfWriter;
import edu.sdsc.mmtf.spark.mappers.StructureToBioassembly;

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
		if (args.length != 1) {
			System.err.println("Usage: " + MapToBioAssembly.class.getSimpleName() + " <outputFilePath>");
			System.exit(1);
		}
	    SparkConf conf = new SparkConf().setMaster("local[*]").setAppName(CustomReportDemo.class.getSimpleName());
	    JavaSparkContext sc = new JavaSparkContext(conf);
	    List<String> pdbIds = Arrays.asList("1HV4"); // single protein chain
	    JavaPairRDD<String, StructureDataInterface> pdb = MmtfReader.downloadMmtfFiles(pdbIds, sc).cache(); 
	   
	    pdb = pdb // read MMTF hadoop sequence file
	    		.flatMapToPair(new StructureToBioassembly());
	    long count = pdb // read MMTF hadoop sequence file
	    		.count();
	    pdb = pdb.coalesce(1);
	    System.out.println("# structures: " + count);
	    MmtfWriter.writeMmtfFiles(args[0], sc, pdb);
	    
	    sc.close();
	}
}
