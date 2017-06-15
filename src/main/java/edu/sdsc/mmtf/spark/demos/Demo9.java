/**
 * 
 */
package edu.sdsc.mmtf.spark.demos;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.biojava.nbio.structure.Structure;
import org.rcsb.mmtf.api.StructureDataInterface;

import edu.sdsc.mmtf.spark.io.MmtfReader;
import edu.sdsc.mmtf.spark.mappers.StructureToBioJava;

/**
 * @author peter
 *
 */
public class Demo9 {

	/**
	 * @param args
	 */
	public static void main(String[] args) {

		String path = System.getProperty("MMTF_REDUCED");
	    if (path == null) {
	    	    System.err.println("Environment variable for Hadoop sequence file has not been set");
	        System.exit(-1);
	    }
	     
	    long start = System.nanoTime();
	    
	    SparkConf conf = new SparkConf().setMaster("local[*]").setAppName(Demo9.class.getSimpleName());
	    JavaSparkContext sc = new JavaSparkContext(conf);
		 
	    // read PDB in MMTF format
	    JavaPairRDD<String, StructureDataInterface> pdb = MmtfReader.readSequenceFile(path, sc);

	    pdb = pdb.filter(t -> t._1.equals("1STP"));
	    
	    // convert to BioJava structure
	    JavaPairRDD<String, Structure> structures = pdb.mapValues(new StructureToBioJava());
	   
	    JavaPairRDD<String, String> seqRes = structures.mapValues(v -> v.getPolyChains().get(0).getSeqResSequence());
	    seqRes.foreach(t -> System.out.println(t));
	    // seqRes has X for non-observed residues
	    
	    System.out.println("Number of structures: " + structures.count());
	    long end = System.nanoTime();
	    
	    System.out.println("Time: " + (end-start)/1E9 + "sec.");
	    
	    sc.close();
	}
}
