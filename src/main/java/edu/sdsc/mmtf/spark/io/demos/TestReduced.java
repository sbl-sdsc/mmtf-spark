package edu.sdsc.mmtf.spark.io.demos;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.rcsb.mmtf.api.StructureDataInterface;

import edu.sdsc.mmtf.spark.io.MmtfReader;

/**
 * Example reading a list of PDB IDs from a local 
 * reduced MMTF Hadoop sequence file into a JavaPairRDD.
 * 
 * @author Peter Rose
 *
 */
public class TestReduced {

	public static void main(String[] args) {  
		
		String path = System.getProperty("MMTF_REDUCED");
		System.out.println(path);
	    if (path == null) {
	    	    System.err.println("Environment variable for Hadoop sequence file has not been set");
	        System.exit(-1);
	    }
	    
	    long start = System.nanoTime();
	    // instantiate Spark. Each Spark application needs these two lines of code.
	    SparkConf conf = new SparkConf().setMaster("local[*]").setAppName(TestReduced.class.getSimpleName());
	    JavaSparkContext sc = new JavaSparkContext(conf);
		 
	    // read list of PDB entries from a local Hadoop sequence file
//	    List<String> pdbIds = Arrays.asList("1AQ1","1B38","1B39","1BUH"); 
//	    JavaPairRDD<String, StructureDataInterface> pdb = MmtfReader.readSequenceFile(path, pdbIds, sc);
	    JavaPairRDD<String, StructureDataInterface> pdb = MmtfReader.readSequenceFile(path, sc);
	    
//	    System.out.println("# structures: " + pdb.count());
	    
	    long end = System.nanoTime();
//	    List<String> collect = pdb.keys().collect();
//	    System.out.println("Distinct entries: " + pdb.keys().distinct().count());
	    
	    pdb.foreach(t -> System.out.println(t._1));
	    
	    System.out.println((end-start)/1E9 + " sec.");
	    // close Spark
	    sc.close();
	}
}
