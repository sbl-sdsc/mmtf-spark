package edu.sdsc.mmtf.spark.demos;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.rcsb.mmtf.api.StructureDataInterface;

import edu.sdsc.mmtf.spark.io.MmtfReader;

/**
 * Example reading a list of PDB IDs from a local MMTF Hadoop sequence file into
 * a JavaPairRDD.
 * 
 * @author Peter Rose
 *
 */
public class Demo0 {

	public static void main(String[] args) {  

	    // instantiate Spark. Each Spark application needs these two lines of code.
	    SparkConf conf = new SparkConf().setMaster("local[*]").setAppName(Demo0.class.getSimpleName());
	    JavaSparkContext sc = new JavaSparkContext(conf);
		 
	    // download a list of PDB entries using web services
	    List<String> pdbIds = Arrays.asList("1AQ1","1B38","1B39","1BUH"); 
	    
	    JavaPairRDD<String, StructureDataInterface> pdb = MmtfReader.downloadMmtfFiles(pdbIds, sc);
	    
	    System.out.println("# structures: " + pdb.count());
	    
	    // close Spark
	    sc.close();
	}

}
