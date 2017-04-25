/**
 * 
 */
package edu.sdsc.mmtf.spark.apps;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.storage.StorageLevel;
import org.rcsb.mmtf.api.StructureDataInterface;

import edu.sdsc.mmtf.spark.filters.IsDSaccharide;
import edu.sdsc.mmtf.spark.filters.IsLProteinChain;
import edu.sdsc.mmtf.spark.filters.SequenceRegexFilter;
import edu.sdsc.mmtf.spark.io.MmtfSequenceFileReader;
import edu.sdsc.mmtf.spark.mappers.StructureToChains;
import edu.sdsc.mmtf.spark.mappers.StructureToPolymerChains;
import edu.sdsc.mmtf.spark.mappers.StructureToPolymerType;
import scala.Tuple2;

/**
 * @author peter
 *
 */
public class CountPolymerTypes {

	/**
	 * @param args
	 */
	public static void main(String[] args) {

	    if (args.length != 1) {
	        System.err.println("Usage: " + CountPolymerTypes.class.getSimpleName() + " <hadoop sequence file>");
	        System.exit(1);
	    }
	    
	    long start = System.nanoTime();
	    
	    SparkConf conf = new SparkConf().setMaster("local[*]").setAppName(CountPolymerTypes.class.getSimpleName());
	    JavaSparkContext sc = new JavaSparkContext(conf);
		 
	    JavaPairRDD<String, StructureDataInterface> full = MmtfSequenceFileReader.read(args[0],  sc);

	    JavaPairRDD<String, String> polymerTypes = full.flatMapToPair(new StructureToPolymerType());
	    
	    System.out.println("polymers: " + polymerTypes.filter(t -> t._2.equals("polymer")).count());    
	    
	    long end = System.nanoTime();
	    
	    System.out.println("Time: " + (end-start)/1E9 + "sec.");
	    
	    sc.close();
	}

}
