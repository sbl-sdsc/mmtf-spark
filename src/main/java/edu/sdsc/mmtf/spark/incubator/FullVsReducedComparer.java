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

import edu.sdsc.mmtf.spark.filters.SequenceRegexFilter;
import edu.sdsc.mmtf.spark.io.MmtfSequenceFileReader;
import edu.sdsc.mmtf.spark.mappers.ReducedEncoder;
import edu.sdsc.mmtf.spark.mappers.ReducedEncoderNew;
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
public class FullVsReducedComparer {

	/**
	 * @param args
	 */
	public static void main(String[] args) {

//	    if (args.length != 2) {
//	        System.err.println("Usage: " + FullVsReducedComparer.class.getSimpleName() + " <hadoop sequence files>");
//	        System.exit(1);
//	    }
	    
	    long start = System.nanoTime();
	    
	    SparkConf conf = new SparkConf().setMaster("local[*]").setAppName(FullVsReducedComparer.class.getSimpleName());
	    JavaSparkContext sc = new JavaSparkContext(conf);
		 
	    // first Hadoop sequence file
	    JavaPairRDD<String, StructureDataInterface> first = MmtfSequenceFileReader.read(args[0],  sc);
	    JavaPairRDD<String, StructureDataInterface> fChains = first.flatMapToPair(new StructureToPolymerChains()); 
	    System.out.println(args[0] + " #polymers: " + fChains.count());
	    
	    JavaPairRDD<String, StructureDataInterface> second = first.mapToPair(t -> new Tuple2<String,StructureDataInterface>(t._1, ReducedEncoderNew.getReduced(t._2)));
	    
//	    JavaPairRDD<String, StructureDataInterface> second = MmtfSequenceFileReader.read(args[1],  sc);
	    JavaPairRDD<String, StructureDataInterface> sChains = second.flatMapToPair(new StructureToPolymerChains());
	    System.out.println(" #polymers: " + sChains.count()); 
//	    System.out.println(args[1] + " #polymers: " + sChains.count()); 
//	    System.out.println("groups: " + sChains.map(t -> t._2).reduce((a,b) -> a+b));
	  	   

	    long end = System.nanoTime();
	    
	    System.out.println("Time: " + (end-start)/1E9 + "sec.");
	    
	    sc.close();
	}

}
