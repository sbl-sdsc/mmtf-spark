package edu.sdsc.mmtf.spark.filters.demos;

import java.io.FileNotFoundException;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.rcsb.mmtf.api.StructureDataInterface;

import edu.sdsc.mmtf.spark.filters.Resolution;
import edu.sdsc.mmtf.spark.io.MmtfReader;

/**
 * Example how to filter PDB entries by resolution range.
 * 
 * @see <a href="http://pdb101.rcsb.org/learn/guide-to-understanding-pdb-data/resolution">resolution</a>
 * 
 * @author Peter Rose
 * @since 0.1.0
 *
 */
public class FilterByResolution {

	public static void main(String[] args) throws FileNotFoundException {

		String path = MmtfReader.getMmtfReducedPath();
	    
	    long start = System.nanoTime();
	    
	    // instantiate Spark. Each Spark application needs these two lines of code.
	    SparkConf conf = new SparkConf().setMaster("local[*]").setAppName(FilterByResolution.class.getSimpleName());
	    JavaSparkContext sc = new JavaSparkContext(conf);

	    // read entire PDB in MMTF format
	    JavaPairRDD<String, StructureDataInterface> pdb = MmtfReader.readSequenceFile(path,  sc);

	    // filter PDB entries resolution. Entries without resolution values, 
	    // e.g., NMR structures, will be filtered out as well.
	    pdb = pdb.filter(new Resolution(0.0, 2.0));
	    
	    System.out.println("# structures: " + pdb.count());
	   
	    // close Spark
	    sc.close();
	    
	    long end = System.nanoTime();
	    System.out.println((end-start)/1E9 + " sec.");    
	}
}
