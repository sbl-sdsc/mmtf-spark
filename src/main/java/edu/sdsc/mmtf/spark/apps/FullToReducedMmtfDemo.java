/**
 * 
 */
package edu.sdsc.mmtf.spark.apps;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.rcsb.mmtf.api.StructureDataInterface;

import edu.sdsc.mmtf.spark.io.MmtfSequenceFileReader;
import edu.sdsc.mmtf.spark.mappers.StructureToPolymerChains;

/**
 * @author peter
 *
 */
public class FullToReducedMmtfDemo {

	/**
	 * @param args
	 */
	public static void main(String[] args) {

	    if (args.length != 1) {
	        System.err.println("Usage: " + FullToReducedMmtfDemo.class.getSimpleName() + " <hadoop sequence file>");
	        System.exit(1);
	    }
	    
	    long start = System.nanoTime();
	    
	    SparkConf conf = new SparkConf().setMaster("local[*]").setAppName(FullToReducedMmtfDemo.class.getSimpleName());
	    JavaSparkContext sc = new JavaSparkContext(conf);
		 
	    JavaPairRDD<String, StructureDataInterface> full = MmtfSequenceFileReader.read(args[0],  sc);
//	    JavaPairRDD<String, StructureDataInterface> full = MmtfSequenceFileReader.read(args[0],  sc).filter(t -> t._1.equals("4HHB"));
//	    JavaPairRDD<String, StructureDataInterface> full = MmtfSequenceFileReader.read(args[0],  sc).filter(t -> t._1.equals("3DNA"));
	    
//	    full = full.filter(new ExperimentalMethodsFilter("SOLUTION NMR"));
//	    full = full.flatMapToPair(new StructureToChains());
	    full = full.flatMapToPair(new StructureToPolymerChains());
	    
//	    long results = full.filter(new IsLProteinChain()).filter(new SequenceRegexFilter(".PP.P")).count();
	    long results = full.count();
//	    long results = full.filter(new IsLProteinChain()).filter(new SecondaryStructureFilter().coil(0.9, 1.0)).count();
//	    long lProtein = full.filter(new IsLProteinChain()).count();
//	    long lProteinNS = full.filter(new IsLProteinChainNonStrict()).count();
//	    long dProtein = full.filter(new IsDProteinChain()).count();
//	    long lDna = full.filter(new IsLDnaChain()).count();
//	    long lRna =  full.filter(new IsLRnaChain()).count();
//	    long dSaccharide = full.filter(new IsDSaccharide()).count();
//	    long monomer = full.filter(new IsMonomerChain()).count();
//	    long polymer = full.filter(new IsPolymerChain()).count();
//	    long sum = lProtein + dProtein + lDna + lRna;
 

	    
//	    System.out.println("L-Peptide: " + lProtein);
//	    System.out.println("L-PeptideNS: " + lProteinNS);
//	    System.out.println("D-Peptide: " + dProtein);
//	    System.out.println("L-RNA: " + lDna);
//	    System.out.println("L-DNA: " + lRna);
//	    System.out.println("dSaccaride: " + dSaccharide);
//	    System.out.println("Non-Polymer: " + monomer);
//	    System.out.println("Polymer: " + polymer);
//	    System.out.println("Sum: " + sum);
	    System.out.println("Results: " + results);

//	    full = full.filter(new IsRnaChain());
	    
//	    full.mapToPair(t -> new Tuple2<String, Integer>(t._1, t._2.getNumAtoms())).foreach(t -> System.out.println("full atoms: " + t));
//	    full.mapToPair(t -> new Tuple2<String, Integer>(t._1, t._2.getNumBonds())).foreach(t -> System.out.println("full bonds: " + t));
//	    full.mapToPair(t -> new Tuple2<String, Integer>(t._1, t._2.getNumGroups())).foreach(t -> System.out.println("full groups: " + t));
//	    full.mapToPair(t -> new Tuple2<String, String>(t._1, t._2.getEntitySequence(0))).foreach(t -> System.out.println("sequence: " + t));

	    
	    long end = System.nanoTime();
	    
	    System.out.println("Time: " + (end-start)/1E9 + "sec.");
	    
	    sc.close();
	}

}
