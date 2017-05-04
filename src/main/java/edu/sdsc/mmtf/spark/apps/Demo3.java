/**
 * 
 */
package edu.sdsc.mmtf.spark.apps;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.rcsb.mmtf.api.StructureDataInterface;

import edu.sdsc.mmtf.spark.filters.ExperimentalMethodsFilter;
import edu.sdsc.mmtf.spark.filters.ResolutionFilter;
import edu.sdsc.mmtf.spark.filters.RfreeFilter;
import edu.sdsc.mmtf.spark.incubator.IsDProteinChain;
import edu.sdsc.mmtf.spark.incubator.IsDSaccharide;
import edu.sdsc.mmtf.spark.incubator.IsLDnaChain;
import edu.sdsc.mmtf.spark.incubator.IsLProteinChain;
import edu.sdsc.mmtf.spark.incubator.IsLProteinChainNonStrict;
import edu.sdsc.mmtf.spark.incubator.IsLRnaChain;
import edu.sdsc.mmtf.spark.io.MmtfSequenceFileReader;
import edu.sdsc.mmtf.spark.mappers.ReducedEncoder;
import edu.sdsc.mmtf.spark.mappers.ReducedEncoderNew;
import edu.sdsc.mmtf.spark.mappers.StructureToPolymerChains;
import scala.Tuple2;

/**
 * @author peter
 *
 */
public class Demo3 {

	/**
	 * @param args
	 */
	public static void main(String[] args) {

	    if (args.length != 1) {
	        System.err.println("Usage: " + Demo3.class.getSimpleName() + " <hadoop sequence file>");
	        System.exit(1);
	    }
	    
	    long start = System.nanoTime();
	    
	    SparkConf conf = new SparkConf().setMaster("local[*]").setAppName(Demo3.class.getSimpleName());
	    JavaSparkContext sc = new JavaSparkContext(conf);
		 
	    // read PDB in MMTF format
	    JavaPairRDD<String, StructureDataInterface> pdb = MmtfSequenceFileReader.read(args[0],  sc);

	    // count number of atoms
	    long numAtoms = pdb.map(t -> t._2.getNumAtoms()).reduce((a,b) -> a+ b);
	    
	    System.out.println("Total number of atoms: " + numAtoms);
	  
	    long end = System.nanoTime();
	    
	    System.out.println("Time:     " + (end-start)/1E9 + "sec.");
	    
	    sc.close();
	}

}
