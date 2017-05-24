/**
 * 
 */
package edu.sdsc.mmtf.spark.incubator;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.rcsb.mmtf.api.StructureDataInterface;
import org.rcsb.mmtf.dataholders.MmtfStructure;
import org.rcsb.mmtf.decoder.GenericDecoder;

import scala.Tuple2;

/**
 * @author peter
 *
 */
public class MmtfStructureDemo {

	/**
	 * @param args
	 */
	public static void main(String[] args) {

	    if (args.length != 1) {
	        System.err.println("Usage: " + MmtfStructureDemo.class.getSimpleName() + " <hadoop sequence file>");
	        System.exit(1);
	    }
	    
	    long start = System.nanoTime();
	    
	    SparkConf conf = new SparkConf().setMaster("local[*]").setAppName(MmtfStructureDemo.class.getSimpleName());
	    JavaSparkContext sc = new JavaSparkContext(conf);
		 
	    // read PDB in MMTF format
	    JavaPairRDD<String, MmtfStructure> pdb = MmtfStructureReader.read(args[0],  sc);


//	    pdb = pdb.filter(new ExperimentalMethodsFilter("X-RAY DIFFRACTION"));
//	    pdb = pdb.filter(new ResolutionFilter(0.0, 2.0));
	    pdb = pdb.filter(new MmtfRfreeFilter(0.0, 0.25));
	    JavaPairRDD<String, StructureDataInterface> sdi = pdb.mapToPair(t -> new Tuple2<String, StructureDataInterface>(t._1, new GenericDecoder(t._2)));
//	    pdb = pdb.flatMapToPair(new StructureToPolymerChains());
//	    pdb = pdb.filter(new IsLProteinChain());
//	    
//	    JavaDoubleRDD len = pdb.mapToDouble(t -> t._2.getEntitySequence(0).length()).cache();
	    
//	    System.out.println("Count:    " + len.count());
//	    System.out.println("Min:      " + len.min());
//	    System.out.println("Max:      " + len.max());
//	    System.out.println("Mean:     " + len.mean());
//	    System.out.println("Stdev:    " + len.stdev());
	    
	    System.out.println("# structures: " + sdi.count());
	    
	    long end = System.nanoTime();
	    
	    System.out.println("Time:     " + (end-start)/1E9 + "sec.");
	    
	    sc.close();
	}

}
