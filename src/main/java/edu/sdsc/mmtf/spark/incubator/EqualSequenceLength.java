/**
 * 
 */
package edu.sdsc.mmtf.spark.incubator;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.forester.development.neTest;
import org.rcsb.mmtf.api.StructureDataInterface;

import edu.sdsc.mmtf.spark.filters.ExperimentalMethods;
import edu.sdsc.mmtf.spark.filters.PolymerComposition;
import edu.sdsc.mmtf.spark.filters.Resolution;
import edu.sdsc.mmtf.spark.filters.Rfree;
import edu.sdsc.mmtf.spark.io.MmtfReader;
import edu.sdsc.mmtf.spark.io.MmtfWriter;
import edu.sdsc.mmtf.spark.mappers.StructureToPolymerChains;
import edu.sdsc.mmtf.spark.mappers.StructureToPolymerSequences;
import scala.Tuple2;

/**
 * @author peter
 *
 */
public class EqualSequenceLength {

	/**
	 * @param args
	 */
	public static void main(String[] args) {

	    if (args.length != 1) {
	        System.err.println("Usage: " + EqualSequenceLength.class.getSimpleName() + " <hadoop sequence file>");
	        System.exit(1);
	    }
	    
	    long start = System.nanoTime();
	    
	    SparkConf conf = new SparkConf().setMaster("local[*]").setAppName(EqualSequenceLength.class.getSimpleName());
	    JavaSparkContext sc = new JavaSparkContext(conf);
		 
	    // read PDB in MMTF format
	    JavaPairRDD<String, StructureDataInterface> pdb = MmtfReader.readSequenceFile(args[0],  sc);

	    // count number of atoms
	    pdb = pdb
	    		.filter(new ExperimentalMethods(ExperimentalMethods.X_RAY_DIFFRACTION))
	    		.filter(new Resolution(0, 2))
	    		.filter(new Rfree(0, 0.25))
	    		.flatMapToPair(new StructureToPolymerChains())
	    		.filter(new PolymerComposition(PolymerComposition.AMINO_ACIDS_20));
	    
	    JavaPairRDD<String, char[]> seq = pdb
	    .flatMapToPair(new StructureToPolymerSequences())
	    .mapToPair(t -> new Tuple2<String,char[]>(t._1, t._2.toCharArray()));
	    
//	    System.out.println("#sequences: " + count);
	  
	    long end = System.nanoTime();
	    
	    System.out.println("Time:     " + (end-start)/1E9 + "sec.");
	    
	    sc.close();
	}

}
