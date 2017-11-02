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
import edu.sdsc.mmtf.spark.mappers.StructureToProteinDimers;

/**
 * Example demonstrating how to extract protein chains from
 * PDB entries. This example uses a flatMapToPair function
 * to transform a structure to its polymer chains.
 * 
 * @author Peter Rose
 *
 */
public class MapToProteinDimers {

	public static void main(String[] args) {

		if (args.length != 1) {
			System.err.println("Usage: " + MapToProteinDimers.class.getSimpleName() + " <outputFilePath>");
			System.exit(1);
		}
		
	    SparkConf conf = new SparkConf().setMaster("local[*]").setAppName(CustomReportDemo.class.getSimpleName());
	    JavaSparkContext sc = new JavaSparkContext(conf);
	    List<String> pdbIds = Arrays.asList("1BZ5"); // single protein chain
	    JavaPairRDD<String, StructureDataInterface> pdb = MmtfReader.downloadMmtfFiles(pdbIds, sc).cache(); 
	   
	    pdb = pdb.flatMapToPair(new StructureToBioassembly()).flatMapToPair(new StructureToProteinDimers(9, 10, false, true));
	    long count = pdb.count();
	    pdb = pdb.coalesce(1);
	    System.out.println("# structures: " + count);
	    MmtfWriter.writeMmtfFiles("D://output/", sc, pdb);
	    
	    sc.close();
	}
}
