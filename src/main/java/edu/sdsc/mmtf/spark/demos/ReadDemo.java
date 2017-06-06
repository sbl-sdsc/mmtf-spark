/**
 * 
 */
package edu.sdsc.mmtf.spark.demos;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * @author peter
 *
 */
public class ReadDemo {

	/**
	 * @param args
	 */
	public static void main(String[] args) {

	    if (args.length != 1) {
	        System.err.println("Usage: " + ReadDemo.class.getSimpleName() + " <hadoop sequence file>");
	        System.exit(1);
	    }
	    
	    long start = System.nanoTime();
	    
	    SparkConf conf = new SparkConf().setMaster("local[*]").setAppName(ReadDemo.class.getSimpleName());
	    JavaSparkContext sc = new JavaSparkContext(conf);
	    
	    long c = sc.sequenceFile(args[0], Text.class, BytesWritable.class).count();
	    System.out.println(c);
		 
	    long end = System.nanoTime();
	    System.out.println(((end-start)/1e9) + "seconds");
	    
	    sc.close();
	}

}
