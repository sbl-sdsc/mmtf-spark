/**
 * 
 */
package edu.sdsc.mmtf.spark.webfilters.demos;

import java.io.IOException;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import edu.sdsc.mmtf.spark.io.MmtfReader;
import edu.sdsc.mmtf.spark.webfilters.WildType;

/**
 * @author peter
 *
 */
public class WildTypeQuery {

	/**
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException {

		String path = MmtfReader.getMmtfReducedPath();
	    
	    long start = System.nanoTime();
	    
	    SparkConf conf = new SparkConf().setMaster("local[*]").setAppName(WildTypeQuery.class.getSimpleName());
	    JavaSparkContext sc = new JavaSparkContext(conf);
		
	    boolean includeExpressionTags = true;
	    int sequenceCoverage = 95;
	    
	    long count = MmtfReader
	    		.readSequenceFile(path, sc)
	    		.filter(new WildType(includeExpressionTags, sequenceCoverage))
	    		.count();
	    		
	    System.out.println(count);
	    long end = System.nanoTime();
	    
	    System.out.println("Time: " + (end-start)/1E9 + "sec.");
	    
	    sc.close();
	}
}
