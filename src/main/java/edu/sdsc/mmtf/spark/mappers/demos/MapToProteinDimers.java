package edu.sdsc.mmtf.spark.mappers.demos;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.rcsb.mmtf.api.StructureDataInterface;

import edu.sdsc.mmtf.spark.io.MmtfReader;
import edu.sdsc.mmtf.spark.mappers.StructureToBioassembly;
import edu.sdsc.mmtf.spark.mappers.StructureToProteinDimers;

/**
 * Example demonstrating how to extract all possible protein dimers
 * from a biological assembly.
 * 
 * @author Peter Rose
 * @since 0.1.0
 *
 */
public class MapToProteinDimers {

	public static void main(String[] args) {

	    SparkConf conf = new SparkConf().setMaster("local[*]").setAppName(MapToProteinDimers.class.getSimpleName());
	    JavaSparkContext sc = new JavaSparkContext(conf);

	    List<String> pdbIds = Arrays.asList("5IBZ"); // single protein chain 5IBZ 
	    JavaPairRDD<String, StructureDataInterface> pdb = MmtfReader.downloadFullMmtfFiles(pdbIds, sc);
	   
	    double cutoffDistance = 8; // if distance between C-beta atoms is less than the cutoff distance, two chains are considered in contact
	    int minContacts = 20; // minimum number of contacts to qualify as an interaction
	    pdb = pdb.flatMapToPair(new StructureToBioassembly()) // convert to bioassembly (homotetramer with D2 symmetry)
	             .flatMapToPair(new StructureToProteinDimers(cutoffDistance, minContacts)); // find all dimers with in bioassembly
	   
	    System.out.println("Number of dimers in 5IBZ bioassembly: " + pdb.count());
	    
	    sc.close();
	}
}
