/**
 * 
 */
package edu.sdsc.mmtf.spark.io.demos;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.rcsb.mmtf.api.StructureDataInterface;

import edu.sdsc.mmtf.spark.io.MmtfReader;
import edu.sdsc.mmtf.spark.io.MmtfWriter;

/**
 * @author Peter Rose
 *
 */
public class WriteMmtfSubset {

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
	    
	    SparkConf conf = new SparkConf().setMaster("local[*]").setAppName(WriteMmtfSubset.class.getSimpleName());
	    JavaSparkContext sc = new JavaSparkContext(conf);
		 
	    // read a 10% random sample of the PDB
	    double fraction = 0.1;
	    long seed = 123;
	    JavaPairRDD<String, StructureDataInterface> pdb = MmtfReader.readSequenceFile(path, fraction, seed, sc);

	    // save this subset in a Hadoop Sequence file
	    MmtfWriter.writeSequenceFile(path +"_10", sc, pdb);
	    
	    System.out.println("# structures: " + pdb.count());
	  
	    long end = System.nanoTime();
	    
	    System.out.println("Time: " + (end-start)/1E9 + "sec.");
	    
	    sc.close();
	}
}
