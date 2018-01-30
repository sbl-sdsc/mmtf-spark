package edu.sdsc.mmtf.spark.filters.demos;

import java.io.FileNotFoundException;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import edu.sdsc.mmtf.spark.filters.ContainsDnaChain;
import edu.sdsc.mmtf.spark.filters.ContainsLProteinChain;
import edu.sdsc.mmtf.spark.filters.ContainsRnaChain;
import edu.sdsc.mmtf.spark.filters.NotFilter;
import edu.sdsc.mmtf.spark.io.MmtfReader;

/**
 * Example how to filter PDB entries that contain L-peptide/DNA complexes.
 * 
 * @author Peter Rose
 * @since 0.1.0
 *
 */
public class FilterProteinDnaComplexes {

	public static void main(String[] args) throws FileNotFoundException {

		String path = MmtfReader.getMmtfReducedPath();
	    
	    SparkConf conf = new SparkConf().setMaster("local[*]").setAppName(FilterProteinDnaComplexes.class.getSimpleName());
	    JavaSparkContext sc = new JavaSparkContext(conf);
	    
		 
	    long count = MmtfReader
	    		.readSequenceFile(path, sc) // read MMTF hadoop sequence file
	    		.filter(new ContainsLProteinChain()) // retain pdb entries that contain L-peptide chains
	    	    .filter(new ContainsDnaChain()) // retain pdb entries that contain L-Dna chains
	    		.filter(new NotFilter(new ContainsRnaChain())) // filter out an RNA containing entries
	    	    .count();
	    
	    System.out.println("# L-peptide/DNA complexes: " + count);
	    sc.close();
	}
}
