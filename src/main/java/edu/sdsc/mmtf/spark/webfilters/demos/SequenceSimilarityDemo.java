/**
 * 
 */
package edu.sdsc.mmtf.spark.webfilters.demos;

import java.io.IOException;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import edu.sdsc.mmtf.spark.io.MmtfReader;
import edu.sdsc.mmtf.spark.mappers.StructureToPolymerChains;
import edu.sdsc.mmtf.spark.webfilters.SequenceSimilarity;

/**
 * This demo filters PDB chains by sequence similarity using
 * RCSB PDB webservices.
 * 
 * @author Peter Rose
 * @since 0.2.0
 *
 */
public class SequenceSimilarityDemo {

	/**
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException {
	    
	    SparkConf conf = new SparkConf().setMaster("local[*]").setAppName(SequenceSimilarityDemo.class.getSimpleName());
	    JavaSparkContext sc = new JavaSparkContext(conf);
		
	    String sequence = "NLVQFGVMIEKMTGKSALQYNDYGCYCGIGGSHWPVDQ";
	    double eValueCutoff = 0.001;
	    int sequenceIdentityCutoff = 40;
	    boolean maskLowComplexity = true;
	    
		// read PDB in MMTF format, split into polymer chains,
	    // search by sequence similarity, and print sequences found
	    MmtfReader
	    		.readReducedSequenceFile(sc)
	    		.flatMapToPair(new StructureToPolymerChains(false, true))
	    		.filter(new SequenceSimilarity(sequence, SequenceSimilarity.BLAST, eValueCutoff, sequenceIdentityCutoff, maskLowComplexity))
	    		.foreach(t -> System.out.println(t._1 + ": " + t._2.getEntitySequence(0)));
	    		
	    sc.close();
	}
}
