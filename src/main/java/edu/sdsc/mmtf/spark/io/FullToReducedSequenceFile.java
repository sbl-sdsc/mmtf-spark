/**
 * 
 */
package edu.sdsc.mmtf.spark.io;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.rcsb.mmtf.api.StructureDataInterface;

import edu.sdsc.mmtf.spark.incubator.ReducedEncoderNew;
import scala.Tuple2;

/**
 * @author Peter Rose
 *
 */
public class FullToReducedSequenceFile {

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
	    
	    SparkConf conf = new SparkConf().setMaster("local[*]").setAppName(FullToReducedSequenceFile.class.getSimpleName());
	    JavaSparkContext sc = new JavaSparkContext(conf);
	    
	    // read PDB in MMTF format
	    JavaPairRDD<String, StructureDataInterface> pdb = MmtfReader
	    		.readSequenceFile(path, sc)
	    		.mapToPair(t -> new Tuple2<String,StructureDataInterface>(t._1, ReducedEncoderNew.getReduced(t._2)));
    
	    path = System.getProperty("MMTF_REDUCED");
	    if (path == null) {
	    	System.err.println("Environment variable for Hadoop sequence file has not been set");
	        System.exit(-1);
	    }
	    MmtfWriter.writeSequenceFile(path, sc, pdb);
	    
	    System.out.println("# structures: " + pdb.count());
	  
	    long end = System.nanoTime();
	    
	    System.out.println("Time:     " + (end-start)/1E9 + "sec.");
	    
	    sc.close();
	}

}
