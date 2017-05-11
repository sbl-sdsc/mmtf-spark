/**
 * 
 */
package edu.sdsc.mmtf.spark.incubator;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.rcsb.mmtf.api.StructureDataInterface;

import edu.sdsc.mmtf.spark.filters.ContainsDSaccharide;
import edu.sdsc.mmtf.spark.filters.ContainsPolymerType;
import edu.sdsc.mmtf.spark.io.MmtfSequenceFileReader;
import edu.sdsc.mmtf.spark.mappers.StructureToPolymerChains;

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
//	    List<String> pdbIds = Arrays.asList("1STP");
//	    List<String> pdbIds = Arrays.asList("1JLP");
//	    List<String> pdbIds = Arrays.asList("2B2E");
//	    JavaPairRDD<String, StructureDataInterface> pdb = MmtfSequenceFileReader.read(args[0], pdbIds, sc);
	    
	    JavaPairRDD<String, StructureDataInterface> pdb = MmtfSequenceFileReader.read(args[0], sc);
	  

	    JavaPairRDD<String, StructureDataInterface> pdbf = pdb.flatMapToPair(new StructureToPolymerChains());
	    pdbf = pdbf.filter(new ContainsPolymerType("L-PEPTIDE LINKING", "PEPTIDE LINKING","L-PEPTIDE COOH CARBOXY TERMINUS"));
	   
//	    JavaRDD<String> keysf = pdbf.keys().cache();
//	    keysf.foreach(t -> System.out.println(t));
//	    System.out.println("keysf: " + keysf.count());
	    
//	    pdb = pdb.filter(new ContainsDSaccharide());
//	    System.out.println("structure filter: " + pdb.count());
//	    pdb = pdb.flatMapToPair(new StructureToPolymerChains());
//	    pdb = pdb.filter(new ContainsDSaccharide());
//	    JavaRDD<String> keys = pdb.keys().cache();
//	    keysf.foreach(t -> System.out.println(t));
//	    System.out.println("keys: " + keys.count());
//	    
//	    System.out.println("Difference: ");
//	    keysf.subtract(keys).collect().forEach(t -> System.out.println(t));

	    
	    
//	    
//	    JavaDoubleRDD len = pdb.mapToDouble(t -> t._2.getEntitySequence(0).length()).cache();
//	    
//	    System.out.println("Count:    " + len.count());
//	    System.out.println("Min:      " + len.min());
//	    System.out.println("Max:      " + len.max());
//	    System.out.println("Mean:     " + len.mean());
//	    System.out.println("Stdev:    " + len.stdev());
	    
	    long end = System.nanoTime();
	    
	    System.out.println("Time:     " + (end-start)/1E9 + "sec.");
	    
	    sc.close();
	}

}
