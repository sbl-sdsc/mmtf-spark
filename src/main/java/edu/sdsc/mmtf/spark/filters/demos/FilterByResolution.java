package edu.sdsc.mmtf.spark.filters.demos;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.rcsb.mmtf.api.StructureDataInterface;

import edu.sdsc.mmtf.spark.filters.Resolution;
import edu.sdsc.mmtf.spark.io.MmtfReader;

/**
 * Example of reading an MMTF Hadoop Sequence file, 
 * filtering the entries by resolution,
 * and counting the number of entries.
 * 
 * @see <a href="http://pdb101.rcsb.org/learn/guide-to-understanding-pdb-data/resolution">resolution</a>
 * 
 * @author Peter Rose
 *
 */
public class FilterByResolution {

	public static void main(String[] args) {

		String path = System.getProperty("MMTF_REDUCED");
	    if (path == null) {
	    	    System.err.println("Environment variable for Hadoop sequence file has not been set");
	        System.exit(-1);
	    }
	    
	    long start = System.nanoTime();
	    
	    // instantiate Spark. Each Spark application needs these two lines of code.
	    SparkConf conf = new SparkConf().setMaster("local[*]").setAppName(FilterByResolution.class.getSimpleName());
	    JavaSparkContext sc = new JavaSparkContext(conf);

	    // read entire PDB in MMTF format
	    JavaPairRDD<String, StructureDataInterface> pdb = MmtfReader.readSequenceFile(path,  sc);

	    // filter PDB entries resolution. Entries without resolution values, 
	    // e.g., NMR structures, will be filtered out.
	    pdb = pdb.filter(new Resolution(0.0, 2.0));
	    
	    System.out.println("# structures: " + pdb.count());
	   
	    // close Spark
	    sc.close();
	    
	    long end = System.nanoTime();
	    System.out.println((end-start)/1E9 + " sec.");    
	}
}
