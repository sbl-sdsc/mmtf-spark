package edu.sdsc.mmtf.spark.io.demos;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.rcsb.mmtf.api.StructureDataInterface;

import edu.sdsc.mmtf.spark.io.MmtfReader;

/**
 * Example reading an MMTF Hadoop Sequence file into a JavaPairRDD.
 * 
 * @author Peter Rose
 * @since 0.2.0
 *
 */
public class ReadLocalMmtfHadoopFile {

	public static void main(String[] args) {  
		
		if (args.length != 1) {
			System.err.println("Usage: " + ReadLocalMmtfHadoopFile.class.getSimpleName() + " <inputFilePath>");
			System.exit(1);
		}
	    
	    // instantiate Spark. Each Spark application needs these two lines of code.
	    SparkConf conf = new SparkConf().setMaster("local[*]").setAppName(ReadLocalMmtfHadoopFile.class.getSimpleName());
	    JavaSparkContext sc = new JavaSparkContext(conf);
		 
	    // read a local MMTF file
	    JavaPairRDD<String, StructureDataInterface> pdb = MmtfReader.readSequenceFile(args[0], sc);
	    
	    System.out.println("# structures: " + pdb.count());
	    
	    // print structural details
        pdb = pdb.sample(false, 0.01);
	    pdb.foreach(t -> TraverseStructureHierarchy.printStructureData(t._2));
	    
	    // close Spark
	    sc.close();
	}
}
