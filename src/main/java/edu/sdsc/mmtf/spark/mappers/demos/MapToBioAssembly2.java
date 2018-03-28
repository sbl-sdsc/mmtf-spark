package edu.sdsc.mmtf.spark.mappers.demos;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.rcsb.mmtf.api.StructureDataInterface;

import edu.sdsc.mmtf.spark.datasets.demos.CustomReportDemo;
import edu.sdsc.mmtf.spark.io.MmtfReader;
import edu.sdsc.mmtf.spark.io.demos.TraverseStructureHierarchy;
import edu.sdsc.mmtf.spark.mappers.StructureToBioassembly;
import edu.sdsc.mmtf.spark.mappers.StructureToBioassembly2;

/**
 * Example demonstrating how to generate Biological assemblies for a PDB entry.
 * 
 * @author Peter Rose
 * @since 0.1.0
 *
 */
public class MapToBioAssembly2 {

	public static void main(String[] args) {

		SparkConf conf = new SparkConf().setMaster("local[*]").setAppName(CustomReportDemo.class.getSimpleName());
		JavaSparkContext sc = new JavaSparkContext(conf);

		List<String> pdbIds = Arrays.asList("1HV4");
//		List<String> pdbIds = Arrays.asList("1STP");
		JavaPairRDD<String, StructureDataInterface> pdb = MmtfReader
				.downloadFullMmtfFiles(pdbIds, sc);
		
		System.out.println("**** AU ****");
       pdb.foreach(t -> TraverseStructureHierarchy.demo(t._2));
		
        JavaPairRDD<String, StructureDataInterface> bioassemblies = pdb.flatMapToPair(new StructureToBioassembly2());

//		System.out.println("Number of bioassemblies for 1HV4: " + bioassemblies.count());
		
	    System.out.println("**** BA ****");
		bioassemblies.foreach(t -> TraverseStructureHierarchy.demo(t._2));

		sc.close();
	}
}
