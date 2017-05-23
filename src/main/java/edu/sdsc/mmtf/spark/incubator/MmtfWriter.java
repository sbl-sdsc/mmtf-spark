package edu.sdsc.mmtf.spark.incubator;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.rcsb.mmtf.api.StructureDataInterface;

import edu.sdsc.mmtf.spark.filters.Resolution;
import edu.sdsc.mmtf.spark.io.MmtfSequenceFileReader;
import edu.sdsc.mmtf.spark.io.MmtfSequenceFileWriter;

/**
 * Simple example of reading an MMTF Hadoop Sequence file, filtering the entries by resolution,
 * and counting the number of entries.
 * 
 * @author Peter Rose
 *
 */
public class MmtfWriter {

	public static void main(String[] args) {

	    if (args.length != 2) {
	        System.err.println("Usage: " + MmtfWriter.class.getSimpleName() + " <input sequence file> <output sequence file>");
	        System.exit(1);
	    }
	    
	    long start = System.nanoTime();
	    // instantiate Spark. Each Spark application needs these two lines of code.
	    SparkConf conf = new SparkConf().setMaster("local[*]").setAppName(MmtfWriter.class.getSimpleName());
	    JavaSparkContext sc = new JavaSparkContext(conf);
		 
	    // read PDB in MMTF format
	    JavaPairRDD<String, StructureDataInterface> pdb = MmtfSequenceFileReader.read(args[0],  sc);

	    MmtfSequenceFileWriter.write(args[1], sc, pdb);
	    
	    long end = System.nanoTime();
	    System.out.println((end-start)/1E9 + " sec.");
	    
	    // close Spark
	    sc.close();
	}

}
