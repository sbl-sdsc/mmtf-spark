package edu.sdsc.mmtf.spark.io.demos;

import java.io.FileNotFoundException;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.rcsb.mmtf.api.StructureDataInterface;

import edu.sdsc.mmtf.spark.io.MmtfReader;

/**
 * Method for benchmarking read performance for
 * MMTF-Hadoop Sequence files.
 * 
 * @author Peter Rose
 * @since 0.2.0
 *
 */
public class MmtfBenchmark {

	public static void main(String[] args) throws FileNotFoundException {  
	    long start = System.nanoTime();
	    
	    if (args.length != 1) {
	        System.out.println("Usage: MmtfBenchmark <mmtf-hadoop-sequence-file>");
	    }
	    
	    // instantiate Spark. Each Spark application needs these two lines of code.
	    SparkConf conf = new SparkConf().setAppName(MmtfBenchmark.class.getSimpleName());
	    JavaSparkContext sc = new JavaSparkContext(conf);
		 
	    // read all PDB entries from a local Hadoop sequence file
	    String path = args[0];
	    JavaPairRDD<String, StructureDataInterface> pdb = MmtfReader.readSequenceFile(path, sc);

	    System.out.println("# structures: " + pdb.count());
	    
	    // close Spark
	    sc.close();
	    
	    long end = System.nanoTime();
	    System.out.println((end-start)/1E9 + " sec."); 
	}
}
