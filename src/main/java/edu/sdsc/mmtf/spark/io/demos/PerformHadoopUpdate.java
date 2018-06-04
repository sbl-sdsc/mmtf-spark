package edu.sdsc.mmtf.spark.io.demos;

import java.io.FileNotFoundException;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import edu.sdsc.mmtf.spark.io.HadoopUpdate;
/**
 * Example reading a list of PDB IDs from a local 
 * reduced MMTF Hadoop sequence file into a JavaPairRDD.
 * 
 * @author Peter Rose
 * @since 0.1.0
 */
public class PerformHadoopUpdate {

	public static void main(String[] args) throws FileNotFoundException {  
		

	    
	    // instantiate Spark. Each Spark application needs these two lines of code.
	    SparkConf conf = new SparkConf().setMaster("local[*]").setAppName(ReadMmtfReduced.class.getSimpleName());
	    JavaSparkContext sc = new JavaSparkContext(conf);
		
	    HadoopUpdate.performUpdate(sc);
	    
	    sc.close();
	}
}
