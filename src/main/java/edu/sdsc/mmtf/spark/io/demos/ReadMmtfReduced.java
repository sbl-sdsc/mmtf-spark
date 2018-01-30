package edu.sdsc.mmtf.spark.io.demos;

import java.io.FileNotFoundException;
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
 * @since 0.1.0
 */
public class ReadMmtfReduced {

	public static void main(String[] args) throws FileNotFoundException {  
		
		String path = MmtfReader.getMmtfReducedPath();
	    
	    // instantiate Spark. Each Spark application needs these two lines of code.
	    SparkConf conf = new SparkConf().setMaster("local[*]").setAppName(ReadMmtfReduced.class.getSimpleName());
	    JavaSparkContext sc = new JavaSparkContext(conf);
		 
	    // read list of PDB entries from a local Hadoop sequence file
	    List<String> pdbIds = Arrays.asList("1AQ1","1B38","1B39","1BUH"); 
	    JavaPairRDD<String, StructureDataInterface> pdb = MmtfReader.readSequenceFile(path, pdbIds, sc);
	    
	    System.out.println("# structures: " + pdb.count());
	    
	    // close Spark
	    sc.close();
	}
}
