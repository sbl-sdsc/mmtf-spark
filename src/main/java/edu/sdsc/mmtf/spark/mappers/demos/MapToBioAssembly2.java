package edu.sdsc.mmtf.spark.mappers.demos;

import java.io.FileNotFoundException;
import java.io.IOException;
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
import edu.sdsc.mmtf.spark.webfilters.Pisces;

/**
 * Example demonstrating how to generate Biological assemblies for a PDB entry.
 * 
 * @author Peter Rose
 * @since 0.1.0
 *
 */
public class MapToBioAssembly2 {

	public static void main(String[] args) throws FileNotFoundException, IOException {

		SparkConf conf = new SparkConf().setMaster("local[*]").setAppName(CustomReportDemo.class.getSimpleName());
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		long start = System.nanoTime();

//		List<String> pdbIds = Arrays.asList("1HV4");
//		List<String> pdbIds = Arrays.asList("2HHB");
//		JavaPairRDD<String, StructureDataInterface> pdb = MmtfReader
//				.downloadFullMmtfFiles(pdbIds, sc);
		
		JavaPairRDD<String, StructureDataInterface> pdb = MmtfReader
				.readFullSequenceFile(sc)
				.filter(new Pisces(20, 3.0));

		
//		System.out.println("**** AU ****");
//        pdb.foreach(t -> TraverseStructureHierarchy.printStructureData(t._2));
		
        JavaPairRDD<String, StructureDataInterface> bioassemblies = pdb.flatMapToPair(new StructureToBioassembly2());

		System.out.println("Number of bioassemblies: " + bioassemblies.count());
		
		long end = System.nanoTime();
		
		System.out.println("time: " + (end-start)/1E9 + " sec.");
		
//	    System.out.println("**** BA ****");
//        bioassemblies.foreach(t -> TraverseStructureHierarchy.printStructureData(t._2));
//		bioassemblies.foreach(t -> TraverseStructureHierarchy.printChainEntityGroupAtomInfo(t._2));

		sc.close();
	}
}
