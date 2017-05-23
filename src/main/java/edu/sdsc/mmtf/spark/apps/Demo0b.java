package edu.sdsc.mmtf.spark.apps;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.rcsb.mmtf.api.StructureDataInterface;

import edu.sdsc.mmtf.spark.io.MmtfSequenceFileReader;

/**
 * Example reading a list of PDB IDs from a local MMTF Hadoop sequence file into
 * a JavaPairRDD.
 * 
 * @author Peter Rose
 *
 */
public class Demo0b {

	public static void main(String[] args) {  
		
		if (args.length != 1) {
	        System.err.println("Usage: " + Demo0b.class.getSimpleName() + " <hadoop sequence file>");
	        System.exit(1);
	    }
		
	    long start = System.nanoTime();
	    
	    // instantiate Spark. Each Spark application needs these two lines of code.
	    SparkConf conf = new SparkConf().setMaster("local[*]").setAppName(Demo0b.class.getSimpleName());
	    JavaSparkContext sc = new JavaSparkContext(conf);
		 
	    String ids = "1AQ1,1B38,1B39,1BUH,1C25,1CKP,1DI8,1DM2,1E1V,1E1X,1E9H,1F5Q,1FIN,1FPZ,1FQ1,1FQV,1FS1";
	    List<String> pdbIds = Arrays.asList(ids.split(","));
	    
	    // read PDB in MMTF format
	    JavaPairRDD<String, StructureDataInterface> pdb = MmtfSequenceFileReader.read(args[0], pdbIds, sc);
	    
	    System.out.println("# structures: " + pdb.count());
	    
	    long end = System.nanoTime();
	    System.out.println((end-start)/1E9 + " sec.");
	    
	    // close Spark
	    sc.close();
	}

}
