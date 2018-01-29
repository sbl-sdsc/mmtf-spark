package edu.sdsc.mmtf.spark.filters.demos;

import java.io.FileNotFoundException;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import edu.sdsc.mmtf.spark.filters.ContainsLProteinChain;
import edu.sdsc.mmtf.spark.io.MmtfReader;

/**
 * Example how to filter PDB entries that exclusively contain L-protein chains.
 * 
 * @author Peter Rose
 * @since 0.1.0
 *
 */
public class FilterExclusivelyByLProteins {

	public static void main(String[] args) throws FileNotFoundException {

		String path = MmtfReader.getMmtfReducedPath();
	    
	    SparkConf conf = new SparkConf().setMaster("local[*]").setAppName(FilterExclusivelyByLProteins.class.getSimpleName());
	    JavaSparkContext sc = new JavaSparkContext(conf);
	    		 
	    boolean exclusive = true;
	    
	    long count = MmtfReader
	    		.readSequenceFile(path, sc) // read MMTF hadoop sequence file
	    		// retain pdb entries that exclusively (flag set to true) contain L-protein chains
	    		.filter(new ContainsLProteinChain(exclusive)) 
	    		.count();
	    
	    System.out.println("# L-proteins: " + count);
	    sc.close();
	}

}
