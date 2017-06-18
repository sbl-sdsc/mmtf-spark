/**
 * 
 */
package edu.sdsc.mmtf.spark.incubator;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * @author peter
 *
 */
public class ReadRawMmtf {

	/**
	 * @param args
	 */
	public static void main(String[] args) {

		String path = System.getProperty("MMTF_FULL");
	    if (path == null) {
	    	    System.err.println("Environment variable for Hadoop sequence file has not been set");
	        System.exit(-1);
	    }
	    
	    long start = System.nanoTime();
	    
	    SparkConf conf = new SparkConf().setMaster("local[*]").setAppName(ReadRawMmtf.class.getSimpleName());
	    JavaSparkContext sc = new JavaSparkContext(conf);
	    
	    long c = sc.sequenceFile(path, Text.class, BytesWritable.class).count();
	    System.out.println(c);
		 
	    long end = System.nanoTime();
	    System.out.println(((end-start)/1e9) + "seconds");
	    
	    sc.close();
	}

}
