/**
 * 
 */
package edu.sdsc.mmtf.spark.apps;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import edu.sdsc.mmtf.spark.filters.ContainsGroup;
import edu.sdsc.mmtf.spark.filters.PolymerComposition;
import edu.sdsc.mmtf.spark.io.MmtfReader;

/**
 * @author peter
 *
 */
public class Demo10 {

	/**
	 * @param args
	 */
	public static void main(String[] args) {

	    if (args.length != 1) {
	        System.err.println("Usage: " + Demo10.class.getSimpleName() + " <hadoop sequence file>");
	        System.exit(1);
	    }
	    
	    long start = System.nanoTime();
	    
	    SparkConf conf = new SparkConf().setMaster("local[*]").setAppName(Demo10.class.getSimpleName());
	    JavaSparkContext sc = new JavaSparkContext(conf);
		 
	    MmtfReader
	    .readSequenceFile(args[0], sc)
	    .filter(new PolymerComposition(PolymerComposition.AMINO_ACIDS_22))
	    .filter(new ContainsGroup("SEC"))
	    .keys()
	    .foreach(k ->System.out.println(k));
	    		
	    long end = System.nanoTime();
	    
	    System.out.println("Time: " + (end-start)/1E9 + "sec.");
	    
	    sc.close();
	}

}
