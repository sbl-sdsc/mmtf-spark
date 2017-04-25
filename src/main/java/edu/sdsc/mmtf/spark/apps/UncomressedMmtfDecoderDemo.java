/**
 * 
 */
package edu.sdsc.mmtf.spark.apps;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.rcsb.mmtf.api.StructureDataInterface;

import edu.sdsc.mmtf.spark.io.UncompressedMmtfSequenceFileReader;

/**
 * @author peter
 *
 */
public class UncomressedMmtfDecoderDemo {

	/**
	 * @param args
	 */
	public static void main(String[] args) {

	    if (args.length != 1) {
	        System.err.println("Usage: MmtfDecoderDemo <hadoop sequence file>");
	        System.exit(1);
	    }
	    
	    long start = System.nanoTime();
	    
	    SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("MmtfDecoderDemo.class.getSimpleName()");
	    JavaSparkContext sc = new JavaSparkContext(conf);
		   
	    JavaPairRDD<String, StructureDataInterface> mmtf = UncompressedMmtfSequenceFileReader.read(args[0],  sc);

	    System.out.println(mmtf.count());
	    
	    long end = System.nanoTime();
	    
	    System.out.println("Time: " + (end-start)/1E9 + "sec.");
	    
	    sc.close();
	}

}
