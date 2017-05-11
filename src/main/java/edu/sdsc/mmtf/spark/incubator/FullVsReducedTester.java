/**
 * 
 */
package edu.sdsc.mmtf.spark.incubator;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.storage.StorageLevel;
import org.rcsb.mmtf.api.StructureDataInterface;

import edu.sdsc.mmtf.spark.filters.ContainsSequenceRegex;
import edu.sdsc.mmtf.spark.io.MmtfSequenceFileReader;
import edu.sdsc.mmtf.spark.mappers.ReducedEncoder;
import edu.sdsc.mmtf.spark.mappers.StructureToChainInfo;
import edu.sdsc.mmtf.spark.mappers.StructureToChainInfo2;
import edu.sdsc.mmtf.spark.mappers.StructureToChains;
import edu.sdsc.mmtf.spark.mappers.StructureToPolymerChains;
import edu.sdsc.mmtf.spark.mappers.StructureToPolymerType;
import scala.Tuple2;

/**
 * @author peter
 *
 */
public class FullVsReducedTester {

	/**
	 * @param args
	 */
	public static void main(String[] args) {

	    if (args.length != 2) {
	        System.err.println("Usage: " + FullVsReducedTester.class.getSimpleName() + " <hadoop sequence files>");
	        System.exit(1);
	    }
	    
	    long start = System.nanoTime();
	    
	    SparkConf conf = new SparkConf().setMaster("local[*]").setAppName(FullVsReducedTester.class.getSimpleName());
	    JavaSparkContext sc = new JavaSparkContext(conf);
		 
	    // first Hadoop sequence file
	    JavaPairRDD<String, StructureDataInterface> first = MmtfSequenceFileReader.read(args[0],  sc);
	    JavaPairRDD<String, Integer> fChains = first.flatMapToPair(new StructureToChainInfo2()); 
	    System.out.println(args[0] + " #polymers: " + fChains.count());   
	    System.out.println("groups: " + fChains.map(t -> t._2).reduce((a,b) -> a+b));
	    
//	    JavaPairRDD<String, StructureDataInterface> second = first.mapToPair(t -> new Tuple2<String,StructureDataInterface>(t._1, ReducedEncoder.getReduced(t._2)));
	    
//	    JavaPairRDD<String, StructureDataInterface> second = MmtfSequenceFileReader.read(args[1],  sc);
//	    JavaPairRDD<String, Integer> sChains = second.flatMapToPair(new StructureToChainInfo2());
//	    System.out.println(args[1] + " #polymers: " + sChains.count()); 
//	    System.out.println("groups: " + sChains.map(t -> t._2).reduce((a,b) -> a+b));
	  	   

	    long end = System.nanoTime();
	    
	    System.out.println("Time: " + (end-start)/1E9 + "sec.");
	    
	    sc.close();
	}

}
