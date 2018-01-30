/**
 * 
 */
package edu.sdsc.mmtf.spark.filters.demos;

import java.io.FileNotFoundException;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.rcsb.mmtf.api.StructureDataInterface;

import edu.sdsc.mmtf.spark.filters.ContainsSequenceRegex;
import edu.sdsc.mmtf.spark.io.MmtfReader;

/**
 * Example how to filter PDB entries by a sequence pattern.
 * 
 * @author Peter Rose
 * @since 0.1.0
 *
 */
public class FilterBySequenceRegex {

	/**
	 * @param args
	 * @throws FileNotFoundException 
	 */
	public static void main(String[] args) throws FileNotFoundException {

		String path = MmtfReader.getMmtfReducedPath();
	    
	    long start = System.nanoTime();
	    
	    SparkConf conf = new SparkConf().setMaster("local[*]").setAppName(FilterBySequenceRegex.class.getSimpleName());
	    JavaSparkContext sc = new JavaSparkContext(conf);
		 
	    // read PDB in MMTF format
	    JavaPairRDD<String, StructureDataInterface> pdb = MmtfReader.readSequenceFile(path,  sc);

	    // find structures that containing a Zinc finger motif
	    pdb = pdb.filter(new ContainsSequenceRegex("C.{2,4}C.{12}H.{3,5}H"));
	    
	    System.out.println("Number of PDB entries containing a Zinc finger motif: " + pdb.count());
	  
	    long end = System.nanoTime();
	    
	    System.out.println("Time: " + (end-start)/1E9 + " sec.");
	    
	    sc.close();
	}
}
