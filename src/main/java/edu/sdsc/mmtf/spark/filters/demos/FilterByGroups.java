package edu.sdsc.mmtf.spark.filters.demos;

import java.io.FileNotFoundException;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import edu.sdsc.mmtf.spark.filters.ContainsGroup;
import edu.sdsc.mmtf.spark.io.MmtfReader;

/**
 * Example how to filter PDB entries by the presence of specified groups (residues).
 * Groups are specified by their upper case one, two, or three-letter codes,
 * e.g. "F", "MG", "ATP", as defined in the 
 * <a href="https://www.wwpdb.org/data/ccd">wwPDB Chemical Component Dictionary</a>.
 * 
 * @author Peter Rose
 * @since 0.1.0
 *
 */
public class FilterByGroups {

	public static void main(String[] args) throws FileNotFoundException {

		String path = MmtfReader.getMmtfReducedPath();
	    
	    SparkConf conf = new SparkConf().setMaster("local[*]").setAppName(FilterByGroups.class.getSimpleName());
	    JavaSparkContext sc = new JavaSparkContext(conf);
		 
	    // find all structure that contain ATP and MG
	    long count = MmtfReader
	    		.readSequenceFile(path, sc)
	    		.filter(new ContainsGroup("ATP"))
	    		.filter(new ContainsGroup("MG"))
	    		.count();
	    
	    System.out.println("Structures with ATP + MG: " + count);
	    
	    sc.close();
	}
}
