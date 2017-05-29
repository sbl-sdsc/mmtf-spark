/**
 * 
 */
package edu.sdsc.mmtf.spark.apps;

import java.io.IOException;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import edu.sdsc.mmtf.spark.io.MmtfReader;
import edu.sdsc.mmtf.spark.rcsbfilters.WildType;

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

	    if (args.length != 1) {
	        System.err.println("Usage: " + WildTypeQuery.class.getSimpleName() + " <hadoop sequence file>");
	        System.exit(1);
	    }
	    
	    long start = System.nanoTime();
	    
	    SparkConf conf = new SparkConf().setMaster("local[*]").setAppName(WildTypeQuery.class.getSimpleName());
	    JavaSparkContext sc = new JavaSparkContext(conf);
		 
	    long count = MmtfReader
	    .readSequenceFile(args[0], sc)
	    .filter(new WildType(true, WildType.SEQUENCE_COVERAGE_95))
	    .count();
	    		
	    System.out.println(count);
	    long end = System.nanoTime();
	    
	    System.out.println("Time: " + (end-start)/1E9 + "sec.");
	    
	    sc.close();
	}

}
