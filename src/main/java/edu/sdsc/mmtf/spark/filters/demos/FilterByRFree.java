package edu.sdsc.mmtf.spark.filters.demos;

import java.io.FileNotFoundException;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import edu.sdsc.mmtf.spark.filters.Resolution;
import edu.sdsc.mmtf.spark.io.MmtfReader;

/**
 * Example of reading an MMTF Hadoop Sequence file, 
 * filtering the entries by resolution, and counting the 
 * number of entries. This example shows how methods can 
 * be chained together.
 * 
 * @see <a href="http://pdb101.rcsb.org/learn/guide-to-understanding-pdb-data/r-value-and-r-free">rfree</a>
 * 
 * @author Peter Rose
 *
 */
public class FilterByRFree {

	public static void main(String[] args) throws FileNotFoundException {

		String path = MmtfReader.getMmtfReducedPath();
	    
	    SparkConf conf = new SparkConf().setMaster("local[*]").setAppName(FilterByRFree.class.getSimpleName());
	    JavaSparkContext sc = new JavaSparkContext(conf);
		 
	    // here the methods are chained together
	    long count = MmtfReader
	    		.readSequenceFile(path, sc)
	    		.filter(new Resolution(0.0, 2.0))
	    		.count();
	    
	    System.out.println("# structures: " + count);
	    
	    sc.close();
	}
}
