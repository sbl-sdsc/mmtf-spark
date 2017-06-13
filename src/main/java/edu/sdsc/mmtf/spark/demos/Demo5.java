/**
 * 
 */
package edu.sdsc.mmtf.spark.demos;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.rcsb.mmtf.api.StructureDataInterface;

import edu.sdsc.mmtf.spark.filters.ExperimentalMethods;
import edu.sdsc.mmtf.spark.filters.Resolution;
import edu.sdsc.mmtf.spark.filters.Rfree;
import edu.sdsc.mmtf.spark.io.MmtfReader;
import edu.sdsc.mmtf.spark.io.MmtfWriter;

/**
 * @author peter
 *
 */
public class Demo5 {

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
	    
	    SparkConf conf = new SparkConf().setMaster("local[*]").setAppName(Demo5.class.getSimpleName());
	    JavaSparkContext sc = new JavaSparkContext(conf);
		 
	    double fraction = 0.2;
	    long seed = 123;
	    // read PDB in MMTF format
	    JavaPairRDD<String, StructureDataInterface> pdb = MmtfReader.readSequenceFile(path, fraction, seed, sc);

	    // retain high resolution X-ray structures
	    pdb = pdb
	    		.filter(new ExperimentalMethods(ExperimentalMethods.X_RAY_DIFFRACTION))
	    		.filter(new Resolution(0, 2.5))
	    		.filter(new Rfree(0, 0.25));
    
	    MmtfWriter.writeSequenceFile(path +"_xray", sc, pdb);
	    
	    System.out.println("# structures: " + pdb.count());
	  
	    long end = System.nanoTime();
	    
	    System.out.println("Time:     " + (end-start)/1E9 + "sec.");
	    
	    sc.close();
	}
}
