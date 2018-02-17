package edu.sdsc.mmtf.spark.io.demos;

import java.io.FileNotFoundException;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.rcsb.mmtf.api.StructureDataInterface;

import edu.sdsc.mmtf.spark.io.MmtfReader;

/**
 * Example reading all PDB entries from a local 
 * full MMTF Hadoop sequence file into a JavaPairRDD.
 * 
 * @author Peter Rose
 * @since 0.1.0
 *
 */
public class ReadMmtfFull {

	public static void main(String[] args) throws FileNotFoundException {  
	    long start = System.nanoTime();
	    
	    // instantiate Spark. Each Spark application needs these two lines of code.
	    SparkConf conf = new SparkConf().setMaster("local[*]").setAppName(ReadMmtfFull.class.getSimpleName());
	    JavaSparkContext sc = new JavaSparkContext(conf);
		 
	    // read all PDB entries from a local Hadoop sequence file
	    JavaPairRDD<String, StructureDataInterface> pdb = MmtfReader.readFullSequenceFile(sc);

	    System.out.println("# structures: " + pdb.count());
	    
	    // close Spark
	    sc.close();
	    
	    long end = System.nanoTime();
	    System.out.println((end-start)/1E9 + " sec."); 
	}
}
