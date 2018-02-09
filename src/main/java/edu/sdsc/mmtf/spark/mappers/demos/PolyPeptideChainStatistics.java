package edu.sdsc.mmtf.spark.mappers.demos;

import java.io.FileNotFoundException;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaSparkContext;

import edu.sdsc.mmtf.spark.filters.PolymerComposition;
import edu.sdsc.mmtf.spark.io.MmtfReader;
import edu.sdsc.mmtf.spark.mappers.StructureToPolymerChains;

/**
 * Example demonstrating how to extract protein chains from PDB entries. This
 * example uses a flatMapToPair function to transform a structure to its polymer
 * chains and calculate polymer length statistics.
 * 
 * @author Peter Rose
 *
 */
public class PolyPeptideChainStatistics {

	public static void main(String[] args) throws FileNotFoundException {

		SparkConf conf = new SparkConf().setMaster("local[*]").setAppName(PolyPeptideChainStatistics.class.getSimpleName());
		JavaSparkContext sc = new JavaSparkContext(conf);

		JavaDoubleRDD chainLengths = MmtfReader
				.readReducedSequenceFile(sc) // read PDB from MMTF-Hadoop sequence file															
				.flatMapToPair(new StructureToPolymerChains(false, true)) // split (flatmap) into unique polymer chains
				.filter(new PolymerComposition(PolymerComposition.AMINO_ACIDS_20)) // only consider chains that contain the 20 standard aminoacids
				.mapToDouble(t -> t._2.getNumGroups()); // get the number of groups (residues) in each chain using a lambda expression

		System.out.println("Protein chains length statistics for proteins in the PDB with the 20 standard amino acids:");
		System.out.println(chainLengths.stats());

		sc.close();
	}
}
