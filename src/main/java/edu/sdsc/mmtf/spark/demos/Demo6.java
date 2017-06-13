/**
 * 
 */
package edu.sdsc.mmtf.spark.demos;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.rcsb.mmtf.api.StructureDataInterface;

import edu.sdsc.mmtf.spark.filters.ContainsSequenceRegex;
import edu.sdsc.mmtf.spark.io.MmtfReader;

/**
 * @author peter
 *
 */
public class Demo6 {

	/**
	 * @param args
	 */
	public static void main(String[] args) {

		String path = System.getProperty("MMTF_REDUCED");
	    if (path == null) {
	    	    System.err.println("Environment variable for Hadoop sequence file has not been set");
	        System.exit(-1);
	    }
	    
	    long start = System.nanoTime();
	    
	    SparkConf conf = new SparkConf().setMaster("local[*]").setAppName(Demo6.class.getSimpleName());
	    JavaSparkContext sc = new JavaSparkContext(conf);
		 
	    // read PDB in MMTF format
	    JavaPairRDD<String, StructureDataInterface> pdb = MmtfReader.readSequenceFile(args[0],  sc);

	    // find structures that containing a Zinc finger motif
	    pdb = pdb.filter(new ContainsSequenceRegex("C.{2,4}C.{12}H.{3,5}H"));
	    
	    System.out.println("Number of PDB entries containing a Zinc finger motif: " + pdb.count());
	  
	    long end = System.nanoTime();
	    
	    System.out.println("Time:     " + (end-start)/1E9 + "sec.");
	    
	    sc.close();
	}

}
