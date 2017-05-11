package edu.sdsc.mmtf.spark.apps;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.rcsb.mmtf.api.StructureDataInterface;

import edu.sdsc.mmtf.spark.filters.Resolution;
import edu.sdsc.mmtf.spark.io.MmtfSequenceFileReader;

/**
 * Simple example of reading an MMTF Hadoop Sequence file, filtering the entries by resolution,
 * and counting the number of entries.
 * 
 * @author Peter Rose
 *
 */
public class Demo1a {

	public static void main(String[] args) {

	    if (args.length != 1) {
	        System.err.println("Usage: " + Demo1a.class.getSimpleName() + " <hadoop sequence file>");
	        System.exit(1);
	    }
	    
	    // instantiate Spark. Each Spark application needs these two lines of code.
	    SparkConf conf = new SparkConf().setMaster("local[*]").setAppName(Demo1a.class.getSimpleName());
	    JavaSparkContext sc = new JavaSparkContext(conf);
		 
	    // read PDB in MMTF format
	    JavaPairRDD<String, StructureDataInterface> pdb = MmtfSequenceFileReader.read(args[0],  sc);

	    // filter PDB entries by X-ray resolution. Entries without resolution values, 
	    // e.g., NMR structure will also be filtered out.
	    pdb = pdb.filter(new Resolution(0.0, 2.0));
	    
	    System.out.println("# structures: " + pdb.count());
	    
	    // close Spark
	    sc.close();
	}

}
