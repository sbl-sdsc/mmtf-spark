/**
 * 
 */
package edu.sdsc.mmtf.spark.apps;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.rcsb.mmtf.api.StructureDataInterface;

import edu.sdsc.mmtf.spark.filters.ContainsLProteinChain;
import edu.sdsc.mmtf.spark.io.MmtfSequenceFileReader;

/**
 * @author peter
 *
 */
public class Demo {

	/**
	 * @param args
	 */
	public static void main(String[] args) {

	    if (args.length != 1) {
	        System.err.println("Usage: " + Demo.class.getSimpleName() + " <hadoop sequence file>");
	        System.exit(1);
	    }
	    
	    long start = System.nanoTime();
	    
	    SparkConf conf = new SparkConf().setMaster("local[*]").setAppName(Demo.class.getSimpleName());
	    JavaSparkContext sc = new JavaSparkContext(conf);
		 
	    // read PDB in MMTF format
	    JavaPairRDD<String, StructureDataInterface> pdb = MmtfSequenceFileReader.read(args[0],  sc);

	    pdb = pdb.filter(t -> Arrays.asList("1STP","5X6H","5L2G").contains(t._1));
//	    pdb = pdb.filter(new ExperimentalMethodsFilter("X-RAY DIFFRACTION"));
//	    pdb = pdb.filter(new ResolutionFilter(0.0, 2.0));
//	    pdb = pdb.filter(new RfreeFilter(0.0, 0.25));
	    pdb = pdb.filter(new ContainsLProteinChain(true));
//	    pdb = pdb.filter(new ContainsLRnaChain());
//	    pdb = pdb.subtract(pdb.filter(new ContainsLRnaChain()));
	    System.out.println(pdb.keys().collect());
//	    pdb = pdb.flatMapToPair(new StructureToPolymerChains());
//	    pdb = pdb.filter(new IsLProteinChain());
//	    
//	    JavaDoubleRDD len = pdb.mapToDouble(t -> t._2.getEntitySequence(0).length()).cache();
//	    
//	    System.out.println("Count:    " + len.count());
//	    System.out.println("Min:      " + len.min());
//	    System.out.println("Max:      " + len.max());
//	    System.out.println("Mean:     " + len.mean());
//	    System.out.println("Stdev:    " + len.stdev());
	    
//	    System.out.println("# structures: " + pdb.count());
	    
	    long end = System.nanoTime();
	    
	    System.out.println("Time:     " + (end-start)/1E9 + "sec.");
	    
	    sc.close();
	}

}
