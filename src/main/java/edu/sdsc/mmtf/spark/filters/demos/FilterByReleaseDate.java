package edu.sdsc.mmtf.spark.filters.demos;

import java.io.FileNotFoundException;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import edu.sdsc.mmtf.spark.filters.ReleaseDate;
import edu.sdsc.mmtf.spark.io.MmtfReader;

/**
 * Example how to filter PDB entries by release date range.
 * 
 * @author Yue Yu
 * @since 0.1.0
 */
public class FilterByReleaseDate {

	public static void main(String[] args) throws FileNotFoundException {

		String path = MmtfReader.getMmtfReducedPath();
	   
	    SparkConf conf = new SparkConf().setMaster("local[*]").setAppName(FilterByReleaseDate.class.getSimpleName());
	    JavaSparkContext sc = new JavaSparkContext(conf);
		 
	    long count = MmtfReader
	    		.readSequenceFile(path, sc)
	    		.filter(new ReleaseDate("2000-01-28","2017-02-28"))
	    		.count();
	    
	    System.out.println("Structures released between 2000-01-28 and 2017-02-28 " + count);
	    
	    sc.close();
	}
}
