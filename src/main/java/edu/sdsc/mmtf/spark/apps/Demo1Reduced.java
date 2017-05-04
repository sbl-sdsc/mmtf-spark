/**
 * 
 */
package edu.sdsc.mmtf.spark.apps;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.rcsb.mmtf.api.StructureDataInterface;

import edu.sdsc.mmtf.spark.incubator.IsDProteinChain;
import edu.sdsc.mmtf.spark.incubator.IsDSaccharide;
import edu.sdsc.mmtf.spark.incubator.IsLDnaChain;
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
public class Demo1Reduced {

	/**
	 * @param args
	 */
	public static void main(String[] args) {

	    if (args.length != 1) {
	        System.err.println("Usage: " + Demo1Reduced.class.getSimpleName() + " <hadoop sequence file>");
	        System.exit(1);
	    }
	    
	    long start = System.nanoTime();
	    
	    SparkConf conf = new SparkConf().setMaster("local[*]").setAppName(Demo1Reduced.class.getSimpleName());
	    JavaSparkContext sc = new JavaSparkContext(conf);
		 
	    JavaPairRDD<String, StructureDataInterface> full = MmtfSequenceFileReader.read(args[0],  sc);
//	    JavaPairRDD<String, StructureDataInterface> full = MmtfSequenceFileReader.read(args[0],  sc).filter(t -> t._1.equals("4HHB"));
//	    JavaPairRDD<String, StructureDataInterface> full = MmtfSequenceFileReader.read(args[0],  sc).filter(t -> t._1.equals("3DNA"));
	    
//       full = full.filter(t -> t._1.equals("1GIZ"));
//	    full = full.filter(new ExperimentalMethodsFilter("SOLUTION NMR"));
//	    full = full.flatMapToPair(new StructureToChains());
//	    full = full.sample(false, 0.1);
	    
	    // create reduced version
//	    full = full.mapToPair(t -> new Tuple2<String,StructureDataInterface>(t._1, ReducedEncoder.getReduced(t._2)));
        full = full.mapToPair(t -> new Tuple2<String,StructureDataInterface>(t._1, ReducedEncoderNew.getReduced(t._2)));
	    
	    full = full.flatMapToPair(new StructureToPolymerChains());
	    
	   
	    
//	    long results = full.filter(new IsLProteinChain()).filter(new SequenceRegexFilter(".PP.P")).count();
//	    long polymer = full.count();
//	    long results = full.filter(new IsLProteinChain()).filter(new SecondaryStructureFilter().coil(0.9, 1.0)).count();
//	    long lProtein = full.filter(new IsLProteinChain()).count();
	    
	    long lProteinNS = full.filter(new IsLProteinChainNonStrict()).count();
//	    long dProtein = full.filter(new IsDProteinChain()).count();
//	    long lDna = full.filter(new IsLDnaChain()).count();
//	    long lRna =  full.filter(new IsLRnaChain()).count();
//	    long dSaccharide = full.filter(new IsDSaccharide()).count();
//	    long sum = lProteinNS + dProtein + lDna + lRna + dSaccharide;
//	    long bonds = full.map(t -> t._2.getNumBonds()).reduce((a,b) -> a+b);
//	    long interBonds = full.map(t -> t._2.getInterGroupBondOrders().length).reduce((a,b) -> a+b);
 
	    System.out.println("L-PeptideNS: " + lProteinNS);
//	    System.out.println("D-Peptide: " + dProtein);
//	    System.out.println("L-RNA: " + lDna);
//	    System.out.println("L-DNA: " + lRna);
//	    System.out.println("dSaccaride: " + dSaccharide);
//	    System.out.println("Polymer: " + polymer);
//	    System.out.println("Sum: " + sum);
//	    System.out.println("Bonds: " + bonds);
//	    System.out.println("Interbonds: " + interBonds);

	    
//	    full.mapToPair(t -> new Tuple2<String, Integer>(t._1, t._2.getNumAtoms())).foreach(t -> System.out.println("full atoms: " + t));
//	    full.mapToPair(t -> new Tuple2<String, Integer>(t._1, t._2.getNumBonds())).foreach(t -> System.out.println("full bonds: " + t));
//	    full.mapToPair(t -> new Tuple2<String, Integer>(t._1, t._2.getNumGroups())).foreach(t -> System.out.println("full groups: " + t));
//	    full.mapToPair(t -> new Tuple2<String, String>(t._1, t._2.getEntitySequence(0))).foreach(t -> System.out.println("sequence: " + t));

	    
	    long end = System.nanoTime();
	    
	    System.out.println("Time: " + (end-start)/1E9 + "sec.");
	    
	    sc.close();
	}

}
