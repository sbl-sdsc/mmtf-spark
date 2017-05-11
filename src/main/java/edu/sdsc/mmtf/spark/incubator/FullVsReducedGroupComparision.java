/**
 * 
 */
package edu.sdsc.mmtf.spark.incubator;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.rcsb.mmtf.api.StructureDataInterface;

import edu.sdsc.mmtf.spark.filters.ContainsGroup;
import edu.sdsc.mmtf.spark.filters.ExperimentalMethodsFilter;
import edu.sdsc.mmtf.spark.filters.Resolution;
import edu.sdsc.mmtf.spark.filters.Rfree;
import edu.sdsc.mmtf.spark.io.MmtfSequenceFileReader;
import edu.sdsc.mmtf.spark.mappers.ReducedEncoder;
import edu.sdsc.mmtf.spark.mappers.ReducedEncoderNew;
import edu.sdsc.mmtf.spark.mappers.StructureToPolymerChains;
import scala.Tuple2;

/**
 * @author peter
 *
 */
public class FullVsReducedGroupComparision {

	/**
	 * @param args
	 */
	public static void main(String[] args) {

	    if (args.length != 1) {
	        System.err.println("Usage: " + FullVsReducedGroupComparision.class.getSimpleName() + " <hadoop sequence file>");
	        System.exit(1);
	    }
	    
	    long start = System.nanoTime();
	    
	    SparkConf conf = new SparkConf().setMaster("local[*]").setAppName(FullVsReducedGroupComparision.class.getSimpleName());
	    JavaSparkContext sc = new JavaSparkContext(conf);
		 
	    // read PDB in MMTF format
	    JavaPairRDD<String, StructureDataInterface> pdb = MmtfSequenceFileReader.read(args[0],  sc);

	    String group = "HOH";
//	    pdb = pdb.filter(t -> Arrays.asList("1N48", "4AB2", "4AAR", "4AAU").contains(t._1));
	    // these 4 structure have in common, that ATP has higher old PDB residue numbers than the waters
	    pdb = pdb.filter(t -> Arrays.asList("1ZY8").contains(t._1));
	    pdb = pdb.filter(new ContainsGroup(group));
	    System.out.println("full " + group + ": " + pdb.count());
//	    JavaRDD<String> full = pdb.map(t -> t._1.toString());
	    
	    // with reduced mapping -> 555 results, without -> 572 results ????
	    pdb = pdb.mapToPair(t -> new Tuple2<String,StructureDataInterface>(t._1, ReducedEncoderNew.getReduced(t._2)));
	    
	    // find structures that containing ATP and MG
	    pdb = pdb.filter(new ContainsGroup(group));
	    System.out.println("redu " + group + ": " + pdb.count());
	    System.out.println(pdb.map(t -> t._1.toString()).collect());
//	    JavaRDD<String> reduced = pdb.map(t -> t._1.toString());
//	    System.out.println("difference: " + full.subtract(reduced).collect());
	  
	    long end = System.nanoTime();
	    
	    System.out.println("Time:     " + (end-start)/1E9 + "sec.");
	    
	    sc.close();
	}

}
