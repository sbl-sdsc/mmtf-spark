package edu.sdsc.mmtf.spark.io.demos;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.rcsb.mmtf.api.StructureDataInterface;

import edu.sdsc.mmtf.spark.analysis.TraverseStructureHierarchy;
import edu.sdsc.mmtf.spark.io.MmtfReader;

/**
 * Example reading a list of PDB IDs from a local 
 * MMTF file into a JavaPairRDD.
 * 
 * @author Peter Rose
 * @since 0.1.0
 *
 */
public class ReadLocalMmtf {

	public static void main(String[] args) {  
		
		if (args.length != 1) {
			System.err.println("Usage: " + ReadLocalMmtf.class.getSimpleName() + " <outputFilePath>");
			System.exit(1);
		}
	    
	    // instantiate Spark. Each Spark application needs these two lines of code.
	    SparkConf conf = new SparkConf().setMaster("local[*]").setAppName(ReadLocalMmtf.class.getSimpleName());
	    JavaSparkContext sc = new JavaSparkContext(conf);
		 
	    // read a local MMTF file

	    JavaPairRDD<String, StructureDataInterface> pdb = MmtfReader.readMmtfFiles(args[0], sc);
	    
	    pdb.foreach(t -> TraverseStructureHierarchy.demo(t._2));
	    System.out.println("# structures: " + pdb.count());
	    
	    // close Spark
	    sc.close();
	}
}
