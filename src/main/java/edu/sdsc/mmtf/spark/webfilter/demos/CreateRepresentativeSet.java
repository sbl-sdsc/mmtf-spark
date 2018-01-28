/**
 * 
 */
package edu.sdsc.mmtf.spark.webfilter.demos;

import java.io.IOException;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.rcsb.mmtf.api.StructureDataInterface;

import edu.sdsc.mmtf.spark.io.MmtfReader;
import edu.sdsc.mmtf.spark.io.MmtfWriter;
import edu.sdsc.mmtf.spark.webfilters.Pisces;

/**
 * @author peter
 *
 */
public class CreateRepresentativeSet {

	/**
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException {

		String path = System.getProperty("MMTF_FULL");
	    if (path == null) {
	    	    System.err.println("Environment variable for Hadoop sequence file has not been set");
	        System.exit(-1);
	    }
	    
	    long start = System.nanoTime();
	    
	    SparkConf conf = new SparkConf().setMaster("local[*]").setAppName(CreateRepresentativeSet.class.getSimpleName());
	    JavaSparkContext sc = new JavaSparkContext(conf);
	    
	    // read PDB in MMTF format
	    JavaPairRDD<String, StructureDataInterface> pdb = MmtfReader.readSequenceFile(path, sc);

	    // filter by representative protein chains at 40% sequence identify
	    int sequenceIdentity = 40;
	    
	    pdb = pdb

//	    		.flatMapToPair(new StructureToPolymerChains())
//	    		.filter(new BlastClusters(sequenceIdentity))
	    		.filter(new Pisces(sequenceIdentity, 2.5));
//	    		.filter(new PolymerComposition(PolymerComposition.AMINO_ACIDS_20));
    
	    pdb = pdb.coalesce(12);
	    // save representative set
	    MmtfWriter.writeSequenceFile(path +"_representatives" + sequenceIdentity, sc, pdb);
	    
//	    System.out.println("# representative chains: " + pdb.count());
		    
	    sc.close();
	    
        long end = System.nanoTime();
	    System.out.println("Time:     " + (end-start)/1E9 + "sec.");
	}

}
