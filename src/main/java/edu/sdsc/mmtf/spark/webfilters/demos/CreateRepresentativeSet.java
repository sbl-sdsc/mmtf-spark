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
 * Creates an MMTF-Hadoop Sequence file for a Picses representative set of
 * protein chains.
 * 
 * <p> See <a href="http://dunbrack.fccc.edu/PISCES.php">PISCES</a>.
 * Please cite the following in any work that uses lists provided by PISCES
 * G. Wang and R. L. Dunbrack, Jr. PISCES: a protein sequence culling server. 
 * Bioinformatics, 19:1589-1591, 2003. 
 * 
 * @author Peter Rose
 * @since 0.1.0
 *
 */
public class CreateRepresentativeSet {

	/**
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException {
	    
	    SparkConf conf = new SparkConf().setMaster("local[*]").setAppName(CreateRepresentativeSet.class.getSimpleName());
	    JavaSparkContext sc = new JavaSparkContext(conf);

	    // filter by representative protein chains at 40% sequence identify 
	    // and  2.5 A resolution using the Pisces filter. Any pair of protein
	    // chains in the representative set will have <= 40% sequence identity.
	    int sequenceIdentity = 40;
	    double resolution = 2.5;
	    
	    // read PDB, split entries into polymer chains, and filter by Pisces filter
	    JavaPairRDD<String, StructureDataInterface> pdb = MmtfReader
	    		.readReducedSequenceFile(sc)
	    		.flatMapToPair(new StructureToPolymerChains())
	    		.filter(new Pisces(sequenceIdentity, resolution));
    
	    System.out.println("# representative chains: " + pdb.count());
	    
	    // coalesce partitions to avoid saving many small files
	    pdb = pdb.coalesce(12);
	    
	    // save representative set
	    String path = MmtfReader.getMmtfReducedPath();
	    MmtfWriter.writeSequenceFile(path +"_representatives_i40_r2.5", sc, pdb);
		    
	    sc.close();
	}
}
