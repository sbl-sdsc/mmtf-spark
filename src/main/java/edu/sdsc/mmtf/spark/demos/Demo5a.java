/**
 * 
 */
package edu.sdsc.mmtf.spark.demos;

import java.io.IOException;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.rcsb.mmtf.api.StructureDataInterface;

import edu.sdsc.mmtf.spark.io.MmtfReader;
import edu.sdsc.mmtf.spark.io.MmtfWriter;
import edu.sdsc.mmtf.spark.rcsbfilters.BlastClusters;

/**
 * @author peter
 *
 */
public class Demo5a {

	/**
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException {

	    if (args.length != 1) {
	        System.err.println("Usage: " + Demo5a.class.getSimpleName() + " <hadoop sequence file>");
	        System.exit(1);
	    }
	    
	    long start = System.nanoTime();
	    
	    SparkConf conf = new SparkConf().setMaster("local[*]").setAppName(Demo5a.class.getSimpleName());
	    JavaSparkContext sc = new JavaSparkContext(conf);
		 
	    double fraction = 0.5;
	    long seed = 123;
	    // read PDB in MMTF format
	    JavaPairRDD<String, StructureDataInterface> pdb = MmtfReader.readSequenceFile(args[0], fraction, seed, sc);

	    // count number of atoms
	    pdb = pdb
	    		.filter(new BlastClusters(40));
//	    		.filter(new ExperimentalMethods(ExperimentalMethods.X_RAY_DIFFRACTION));
//	    		.filter(new Resolution(0, 2.5))
//	    		.filter(new Rfree(0, 0.25));
    
	    MmtfWriter.writeSequenceFile(args[0]+"_xray", sc, pdb);
	    
	    System.out.println("# structures: " + pdb.count());
	  
	    long end = System.nanoTime();
	    
	    System.out.println("Time:     " + (end-start)/1E9 + "sec.");
	    
	    sc.close();
	}

}
