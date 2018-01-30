/**
 * 
 */
package edu.sdsc.mmtf.spark.webfilters.demos;

import java.io.IOException;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.rcsb.mmtf.api.StructureDataInterface;

import edu.sdsc.mmtf.spark.io.MmtfReader;
import edu.sdsc.mmtf.spark.io.MmtfWriter;
import edu.sdsc.mmtf.spark.mappers.StructureToPolymerChains;
import edu.sdsc.mmtf.spark.webfilters.Pisces;

/**
 * Creates an MMTF-Hadoop Sequence file for a representative set of
 * protein chains.
 * 
 * @author Peter Rose
 * @since 0.1.0
 *
 */
public class CreateRepresentativeSet {

	/**
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException {

		String path = MmtfReader.getMmtfFullPath();
	    
	    long start = System.nanoTime();
	    
	    SparkConf conf = new SparkConf().setMaster("local[*]").setAppName(CreateRepresentativeSet.class.getSimpleName());
	    JavaSparkContext sc = new JavaSparkContext(conf);
	    
	    // read PDB in MMTF format
	    JavaPairRDD<String, StructureDataInterface> pdb = MmtfReader.readSequenceFile(path, sc);

	    // filter by representative protein chains at 40% sequence identify and
	    // 2.5 A resolution using the Pisces filter
	    int sequenceIdentity = 40;
	    double resolution = 2.5;
	    
	    pdb = pdb
	    		.flatMapToPair(new StructureToPolymerChains())
	    		.filter(new Pisces(sequenceIdentity, resolution));
    
	    System.out.println("# representative chains: " + pdb.count());
	    
	    // coalesce partitions to avoid saving many small files
	    pdb = pdb.coalesce(12);
	    
	    // save representative set
	    MmtfWriter.writeSequenceFile(path +"_representatives_i40_r2.5", sc, pdb);
		    
	    sc.close();
	    
        long end = System.nanoTime();
	    System.out.println("Time:     " + (end-start)/1E9 + "sec.");
	}
}
