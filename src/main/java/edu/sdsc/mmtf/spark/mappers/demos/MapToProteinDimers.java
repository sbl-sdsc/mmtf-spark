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

	    List<String> pdbIds = Arrays.asList("5IBZ"); // single protein chain 5IBZ -> D2
	    JavaPairRDD<String, StructureDataInterface> pdb = MmtfReader.downloadMmtfFiles(pdbIds, sc);
	   
	    pdb = pdb.flatMapToPair(new StructureToBioassembly())
	            .flatMapToPair(new StructureToProteinDimers(8, 20, true, true));
	   
	    System.out.println("# dimers in 5IBZ bioassembly: " + pdb.count());
	    
	    sc.close();
	}
}
