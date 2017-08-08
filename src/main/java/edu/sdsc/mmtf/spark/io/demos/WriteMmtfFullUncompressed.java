package edu.sdsc.mmtf.spark.io.demos;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.rcsb.mmtf.api.StructureDataInterface;

import edu.sdsc.mmtf.spark.io.MmtfReader;
import edu.sdsc.mmtf.spark.io.MmtfWriter;

/**
 * Example reading all PDB entries from a local 
 * full MMTF Hadoop sequence file and saving it as 
 * an uncompressed MMTF Hadoop sequence file. 
 * The uncompressed files are preferable for local files
 * on Solid State Drives, since they can be read faster.
 * 
 * @author Peter Rose
 *
 */
public class WriteMmtfFullUncompressed {

	public static void main(String[] args) {  
		
		String path = System.getProperty("MMTF_FULL");
	    if (path == null) {
	    	    System.err.println("Environment variable for Hadoop sequence file has not been set");
	        System.exit(-1);
	    }
	    long start = System.nanoTime();
	    
	    // instantiate Spark. Each Spark application needs these two lines of code.
	    SparkConf conf = new SparkConf().setMaster("local[*]").setAppName(WriteMmtfFullUncompressed.class.getSimpleName());
	    JavaSparkContext sc = new JavaSparkContext(conf);
		 
	    // read all PDB entries from a local Hadoop sequence file
	    JavaPairRDD<String, StructureDataInterface> pdb = MmtfReader.readSequenceFile(path, sc);
	    
	    System.out.println("# structures: " + pdb.count());
	    
	    // write an uncompressed Hadoop sequence file
	    boolean compressed = false;
        MmtfWriter.writeSequenceFile(path + "_uncompressed", sc, pdb, compressed);
		
        // close Spark
	    sc.close();
	    
	    long end = System.nanoTime();
	    System.out.println((end-start)/1E9 + " sec."); 
	}
}
